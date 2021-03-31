package org.broadinstitute.dsde.workbench.leonardo.util

import _root_.org.typelevel.log4cats.StructuredLogger
import cats.Parallel
import cats.effect.{Async, ContextShift, Timer}
import cats.syntax.all._
import cats.mtl.Ask
import com.google.cloud.compute.v1._
import org.broadinstitute.dsde.workbench.google2.util.RetryPredicates
import org.broadinstitute.dsde.workbench.google2.{
  tracedRetryGoogleF,
  ComputePollOperation,
  FirewallRuleName,
  GoogleComputeService,
  GoogleResourceService,
  NetworkName,
  RegionName,
  SubnetworkName
}
import org.broadinstitute.dsde.workbench.leonardo.config.FirewallRuleConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.google._
import org.broadinstitute.dsde.workbench.leonardo.model.LeoException
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.jdk.CollectionConverters._

final case class InvalidVPCSetupException(project: GoogleProject)
    extends LeoException(s"Invalid VPC configuration in project ${project.value}", traceId = None)

final case class NetworkNotReadyException(project: GoogleProject, network: NetworkName)
    extends LeoException(s"Network ${network.value} in project ${project.value} not ready within the specified time",
                         traceId = None)

final case class SubnetworkNotReadyException(project: GoogleProject, subnetwork: SubnetworkName)
    extends LeoException(
      s"Subnetwork ${subnetwork.value} in project ${project.value} not ready within the specified time",
      traceId = None
    )

final case class FirewallNotReadyException(project: GoogleProject, firewall: FirewallRuleName)
    extends LeoException(s"Firewall ${firewall.value} in project ${project.value} not ready within the specified time",
                         traceId = None)

final class VPCInterpreter[F[_]: Parallel: ContextShift: StructuredLogger: Timer](
  config: VPCInterpreterConfig,
  googleResourceService: GoogleResourceService[F],
  googleComputeService: GoogleComputeService[F],
  computePollOperation: ComputePollOperation[F]
)(implicit F: Async[F])
    extends VPCAlgebra[F] {

  val defaultNetworkName = NetworkName("default")

  // Retry 409s to support concurrent get-check-create operations
  val retryPolicy = RetryPredicates.retryConfigWithPredicates(
    RetryPredicates.standardRetryPredicate,
    RetryPredicates.whenStatusCode(409)
  )

  override def setUpProjectNetwork(
    params: SetUpProjectNetworkParams
  )(implicit ev: Ask[F, TraceId]): F[(NetworkName, SubnetworkName)] =
    for {
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
            // create the network
            _ <- createIfAbsent(
              params.project,
              googleComputeService.getNetwork(params.project, config.vpcConfig.networkName),
              googleComputeService.createNetwork(params.project, buildNetwork(params.project)),
              NetworkNotReadyException(params.project, config.vpcConfig.networkName),
              s"get or create network (${params.project} / ${config.vpcConfig.networkName.value})"
            )
            // If we specify autoCreateSubnetworks, a subnet is automatically created in each region with the same name as the network.
            // See https://cloud.google.com/vpc/docs/vpc#subnet-ranges
            subnetworkName <- if (config.vpcConfig.autoCreateSubnetworks) {
              F.pure(SubnetworkName(config.vpcConfig.networkName.value))
            } else {
              // create the subnet
              createIfAbsent(
                params.project,
                googleComputeService.getSubnetwork(params.project, params.region, config.vpcConfig.subnetworkName),
                googleComputeService.createSubnetwork(params.project,
                                                      params.region,
                                                      buildSubnetwork(params.project, params.region)),
                SubnetworkNotReadyException(params.project, config.vpcConfig.subnetworkName),
                s"get or create subnetwork (${params.project} / ${config.vpcConfig.subnetworkName.value})"
              ).as(config.vpcConfig.subnetworkName)
            }
          } yield (config.vpcConfig.networkName, subnetworkName)
        case _ =>
          F.raiseError[(NetworkName, SubnetworkName)](InvalidVPCSetupException(params.project))
      }
    } yield (network, subnetwork)

  override def setUpProjectFirewalls(
    params: SetUpProjectFirewallsParams
  )(implicit ev: Ask[F, TraceId]): F[Unit] =
    for {
      // create firewalls in the Leonardo network
      _ <- config.vpcConfig.firewallsToAdd.parTraverse_ { fw =>
        createIfAbsent(
          params.project,
          googleComputeService.getFirewallRule(params.project, fw.name),
          googleComputeService.addFirewallRule(params.project, buildFirewall(params.project, params.networkName, fw)),
          FirewallNotReadyException(params.project, fw.name),
          s"get or create firewall rule (${params.project} / ${fw.name.value})"
        )
      }
      // if the default network exists, remove configured firewalls
      defaultNetwork <- googleComputeService.getNetwork(params.project, defaultNetworkName)
      _ <- if (defaultNetwork.isDefined) {
        config.vpcConfig.firewallsToRemove
          .parTraverse_(fw => googleComputeService.deleteFirewallRule(params.project, fw))
      } else F.unit
    } yield ()

  private def createIfAbsent[A](project: GoogleProject,
                                get: F[Option[A]],
                                create: F[Operation],
                                fail: Throwable,
                                msg: String)(
    implicit ev: Ask[F, TraceId]
  ): F[Unit] = {
    val getAndCreate = for {
      existing <- get
      _ <- if (existing.isEmpty) {
        for {
          initialOp <- create
          _ <- computePollOperation
            .pollOperation(project, initialOp, config.vpcConfig.pollPeriod, config.vpcConfig.maxAttempts, None)(
              F.unit,
              F.raiseError[Unit](fail),
              F.unit
            )
        } yield ()
      } else F.unit
    } yield ()

    // Retry the whole get-check-create operation in case of 409
    tracedRetryGoogleF(retryPolicy)(getAndCreate, msg).compile.lastOrError
  }

  private[util] def buildNetwork(project: GoogleProject): Network =
    Network
      .newBuilder()
      .setName(config.vpcConfig.networkName.value)
      .setAutoCreateSubnetworks(config.vpcConfig.autoCreateSubnetworks)
      .build

  private[util] def buildSubnetwork(project: GoogleProject, region: RegionName): Subnetwork =
    Subnetwork
      .newBuilder()
      .setName(config.vpcConfig.subnetworkName.value)
      .setRegion(region.value)
      .setNetwork(buildNetworkUri(project, config.vpcConfig.networkName))
      .setIpCidrRange(
        config.vpcConfig.subnetworkRegionIpRangeMap
          .getOrElse(region, throw new Exception(s"Unsupported Region ${region.value}"))
          .value
      )
      .build

  private[util] def buildFirewall(googleProject: GoogleProject,
                                  networkName: NetworkName,
                                  fwConfig: FirewallRuleConfig): Firewall =
    Firewall
      .newBuilder()
      .setName(fwConfig.name.value)
      .setNetwork(buildNetworkUri(googleProject, networkName))
      .addAllSourceRanges(fwConfig.sourceRanges.map(_.value).asJava)
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
}
