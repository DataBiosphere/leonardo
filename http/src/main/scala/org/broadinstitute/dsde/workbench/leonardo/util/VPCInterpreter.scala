package org.broadinstitute.dsde.workbench.leonardo.util

import _root_.org.typelevel.log4cats.StructuredLogger
import cats.Parallel
import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import com.google.api.gax.longrunning.OperationFuture
import com.google.cloud.compute.v1._
import org.broadinstitute.dsde.workbench.DoneCheckable
import org.broadinstitute.dsde.workbench.google2.util.RetryPredicates
import org.broadinstitute.dsde.workbench.google2.{
  isSuccess,
  streamUntilDoneOrTimeout,
  tracedRetryF,
  FirewallRuleName,
  GoogleComputeService,
  GoogleResourceService,
  NetworkName,
  RegionName,
  SubnetworkName
}
import org.broadinstitute.dsde.workbench.leonardo.IpRange
import org.broadinstitute.dsde.workbench.leonardo.config.FirewallRuleConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.google._
import org.broadinstitute.dsde.workbench.leonardo.model.LeoException
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

final case class InvalidVPCSetupException(project: GoogleProject)
    extends LeoException(s"Invalid VPC configuration in project ${project.value}", traceId = None)

final case class NetworkNotReadyException(project: GoogleProject, network: NetworkName)
    extends LeoException(s"Network ${network.value} in project ${project.value} not ready within the specified time",
                         traceId = None
    )

final case class SubnetworkNotReadyException(project: GoogleProject, subnetwork: SubnetworkName)
    extends LeoException(
      s"Subnetwork ${subnetwork.value} in project ${project.value} not ready within the specified time",
      traceId = None
    )

final case class FirewallNotReadyException(project: GoogleProject, firewall: FirewallRuleName, traceId: TraceId)
    extends LeoException(s"Firewall ${firewall.value} in project ${project.value} not ready within the specified time",
                         traceId = Some(traceId)
    )

final class VPCInterpreter[F[_]: Parallel](
  config: VPCInterpreterConfig,
  googleResourceService: GoogleResourceService[F],
  googleComputeService: GoogleComputeService[F]
)(implicit F: Async[F], logger: StructuredLogger[F])
    extends VPCAlgebra[F] {

  val defaultNetworkName = NetworkName("default")

  // Retry 409s to support concurrent get-check-create operations
  val retryPolicy = RetryPredicates.retryConfigWithPredicates(
    RetryPredicates.standardGoogleRetryPredicate,
    RetryPredicates.whenStatusCode(409)
  )

  override def setUpProjectNetworkAndFirewalls(
    params: SetUpProjectNetworkParams
  )(implicit ev: Ask[F, TraceId]): F[(NetworkName, SubnetworkName)] =
    for {
      ctx <- ev.ask
      // For high-security projects, the network and subnetwork are pre-created and specified by project label.
      // See https://github.com/broadinstitute/gcp-dm-templates/blob/44b13216e5284d1ce46f58514fe51404cdf8f393/firecloud_project.py#L355-L359
      projectLabels <- googleResourceService.getLabels(params.project)
      networkFromLabel = projectLabels.flatMap(_.get(config.vpcConfig.highSecurityProjectNetworkLabel.value))
      subnetworkFromLabel = projectLabels.flatMap(_.get(config.vpcConfig.highSecurityProjectSubnetworkLabel.value))
      (network, subnetwork) <- (networkFromLabel, subnetworkFromLabel) match {
        // If we found project labels, we're done
        case (Some(network), Some(subnet)) =>
          F.pure((NetworkName(network), SubnetworkName(subnet)))
        // Otherwise, we potentially need to create the network and subnet
        case (None, None) =>
          for {
            regionalIpRange <- F.fromOption(
              config.vpcConfig.subnetworkRegionIpRangeMap.get(params.region),
              new LeoException(s"Unable to create subnetwork due to unsupported region ${params.region.value}",
                               traceId = Some(ctx)
              )
            )
            // create the network
            _ <- createIfAbsent(
              googleComputeService.getNetwork(params.project, config.vpcConfig.networkName),
              googleComputeService.createNetwork(params.project, buildNetwork(params.project)),
              NetworkNotReadyException(params.project, config.vpcConfig.networkName),
              s"get or create network (${params.project} / ${config.vpcConfig.networkName.value})"
            )
            // If we specify autoCreateSubnetworks, a subnet is automatically created in each region with the same name as the network.
            // See https://cloud.google.com/vpc/docs/vpc#subnet-ranges
            subnetworkName <-
              if (config.vpcConfig.autoCreateSubnetworks) {
                logger
                  .info(Map("traceId" -> ctx.asString))(
                    s"autoCreateSubnetworks is false. Not going to create ${config.vpcConfig.subnetworkName}"
                  )
                  .as(SubnetworkName(config.vpcConfig.networkName.value))
              } else {
                // create the subnet
                createIfAbsent(
                  googleComputeService.getSubnetwork(params.project, params.region, config.vpcConfig.subnetworkName),
                  googleComputeService.createSubnetwork(
                    params.project,
                    params.region,
                    buildSubnetwork(params.project, params.region, config.vpcConfig.subnetworkName, regionalIpRange)
                  ),
                  SubnetworkNotReadyException(params.project, config.vpcConfig.subnetworkName),
                  s"get or create subnetwork (${params.project} / ${config.vpcConfig.subnetworkName.value})"
                ).as(config.vpcConfig.subnetworkName)
              }
          } yield (config.vpcConfig.networkName, subnetworkName)
        case _ =>
          F.raiseError[(NetworkName, SubnetworkName)](InvalidVPCSetupException(params.project))
      }
      _ <- setUpProjectFirewalls(
        SetUpProjectFirewallsParams(params.project, network, params.region, projectLabels.getOrElse(Map.empty))
      )
    } yield (network, subnetwork)

  private[util] def setUpProjectFirewalls(
    params: SetUpProjectFirewallsParams
  )(implicit ev: Ask[F, TraceId]): F[Unit] =
    for {
      ctx <- ev.ask
      finalFirewallRulesToAdd = firewallRulesToAdd(params.projectLabels)
      // create firewalls in the Leonardo network
      _ <- finalFirewallRulesToAdd.parTraverse_ { fw =>
        for {
          firewallRegionalIprange <- F.fromOption(
            fw.sourceRanges.get(params.region),
            new LeoException(s"Fail to create firewall due to unsupported Region ${params.region.value}",
                             traceId = Some(ctx)
            )
          )
          firewallName = buildFirewallName(fw.namePrefix, params.region)
          _ <- createIfAbsent(
            googleComputeService.getFirewallRule(params.project, firewallName),
            googleComputeService
              .addFirewallRule(
                params.project,
                buildFirewall(params.project, params.networkName, firewallName, fw, firewallRegionalIprange)
              ),
            FirewallNotReadyException(params.project, firewallName, ctx),
            s"get or create firewall rule (${params.project} / ${firewallName.value})"
          )
        } yield ()
      }
      // if the default network exists, remove configured firewalls
      defaultNetwork <- googleComputeService.getNetwork(params.project, defaultNetworkName)
      _ <-
        if (defaultNetwork.isDefined) {
          config.vpcConfig.firewallsToRemove
            .parTraverse_(fw => googleComputeService.deleteFirewallRule(params.project, fw))
        } else F.unit
    } yield ()

  private[util] def firewallRulesToAdd(projectLabels: Map[String, String]): List[FirewallRuleConfig] = {
    val rbsAllowHttpsFirewallRuleName = projectLabels.get(config.vpcConfig.firewallAllowHttpsLabelKey.value)
    val rbsAllowInternalFirewallruleName =
      projectLabels.get(config.vpcConfig.firewallAllowInternalLabelKey.value)
    val firewallRulesCreatedByRbs = List(rbsAllowHttpsFirewallRuleName, rbsAllowInternalFirewallruleName).flatten
    config.vpcConfig.firewallsToAdd.filterNot(rule => rule.rbsName.exists(n => firewallRulesCreatedByRbs.contains(n)))
  }
  private def createIfAbsent[A](get: F[Option[A]],
                                create: F[OperationFuture[Operation, Operation]],
                                fail: Throwable,
                                msg: String
  )(implicit
    ev: Ask[F, TraceId]
  ): F[Unit] = {
    val getAndCreate = for {
      existing <- get
      _ <-
        if (existing.isEmpty) {
          for {
            opFuture <- create
            res <- F.blocking(opFuture.get())
            _ <-
              if (
                isSuccess(res.getHttpErrorStatusCode) || res.getHttpErrorStatusCode == 409 && res.getHttpErrorMessage
                  .contains("already exists")
              )
                F.unit
              else F.raiseError(fail)
          } yield ()
        } else F.unit
    } yield ()

    val res = getAndCreate.recoverWith {
      case e: java.util.concurrent.ExecutionException =>
        if (
          e.getMessage.contains(
            "Conflict"
          )
        )
          F.unit
        else F.raiseError(e)
      case e: com.google.api.gax.rpc.ApiException =>
        if (
          e.getStatusCode.getCode == com.google.api.gax.rpc.StatusCode.Code.ABORTED && e.getMessage.contains(
            "already exists"
          )
        )
          F.unit
        else F.raiseError(e)
    }

    // Retry the whole get-check-create operation in case of 409
    tracedRetryF(retryPolicy)(res, msg).compile.lastOrError
  }

  private[util] def buildNetwork(project: GoogleProject): Network =
    Network
      .newBuilder()
      .setName(config.vpcConfig.networkName.value)
      .setAutoCreateSubnetworks(config.vpcConfig.autoCreateSubnetworks)
      .build

  private[util] def buildSubnetwork(project: GoogleProject,
                                    region: RegionName,
                                    name: SubnetworkName,
                                    subnetRegionIpRange: IpRange
  ): Subnetwork =
    Subnetwork
      .newBuilder()
      .setName(name.value)
      .setRegion(region.value)
      .setNetwork(buildNetworkUri(project, config.vpcConfig.networkName))
      .setIpCidrRange(
        subnetRegionIpRange.value
      )
      .build

  private[util] def buildFirewall(googleProject: GoogleProject,
                                  networkName: NetworkName,
                                  firewallRuleName: FirewallRuleName,
                                  fwConfig: FirewallRuleConfig,
                                  regionalSources: List[IpRange]
  ): Firewall =
    Firewall
      .newBuilder()
      .setName(s"${firewallRuleName.value}")
      .setNetwork(buildNetworkUri(googleProject, networkName))
      .addAllSourceRanges(
        regionalSources.map(_.value).asJava
      )
      .addTargetTags(config.vpcConfig.networkTag.value)
      .addAllAllowed(
        fwConfig.allowed
          .map(a =>
            Allowed
              .newBuilder()
              .setIPProtocol(a.protocol)
              .addAllPorts(a.port.toList.asJava)
              .build()
          )
          .asJava
      )
      .build

  private def buildFirewallName(prefix: String, regionName: RegionName): FirewallRuleName =
    FirewallRuleName(s"${prefix}-${regionName.value}")
}
