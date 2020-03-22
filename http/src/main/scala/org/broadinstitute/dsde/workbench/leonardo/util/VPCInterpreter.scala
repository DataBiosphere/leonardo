package org.broadinstitute.dsde.workbench.leonardo.util

import cats.Parallel
import cats.effect.{Async, ContextShift, IO}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.google.cloud.compute.v1.{Allowed, Firewall, Network, Operation, Subnetwork}
import io.chrisdavenport.log4cats.Logger
import org.broadinstitute.dsde.workbench.google.GoogleProjectDAO
import org.broadinstitute.dsde.workbench.google2.{FirewallRuleName, GoogleComputeService, NetworkName, SubnetworkName}
import org.broadinstitute.dsde.workbench.leonardo.config.FirewallRuleConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.google._
import org.broadinstitute.dsde.workbench.leonardo.model.LeoException
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import scala.collection.JavaConverters._

final case class InvalidVPCSetupException(project: GoogleProject)
    extends LeoException(s"Invalid VPC configuration in project ${project.value}")

final case class NetworkNotReadyException(project: GoogleProject, network: NetworkName)
    extends LeoException(s"Network ${network.value} in project ${project.value} not ready within the specified time")

final case class SubnetworkNotReadyException(project: GoogleProject, subnetwork: SubnetworkName)
    extends LeoException(
      s"Subnetwork ${subnetwork.value} in project ${project.value} not ready within the specified time"
    )

final case class FirewallNotReadyException(project: GoogleProject, firewall: FirewallRuleName)
    extends LeoException(s"Firewall ${firewall.value} in project ${project.value} not ready within the specified time")

class VPCInterpreter[F[_]: Async: Parallel: ContextShift: Logger](
  config: VPCInterpreterConfig,
  googleProjectDAO: GoogleProjectDAO,
  googleComputeService: GoogleComputeService[F]
)(implicit cs: ContextShift[IO])
    extends VPCAlgebra[F] {

  val defaultNetwork = NetworkName("default")

  override def setUpProjectNetwork(
    params: SetUpProjectNetworkParams
  )(implicit ev: ApplicativeAsk[F, TraceId]): F[(NetworkName, SubnetworkName)] =
    for {
      // For high-security projects, the network and subnetwork are pre-created and specified by project label.
      // See https://github.com/broadinstitute/gcp-dm-templates/blob/44b13216e5284d1ce46f58514fe51404cdf8f393/firecloud_project.py#L355-L359
      projectLabels <- Async[F].liftIO(IO.fromFuture(IO(googleProjectDAO.getLabels(params.project.value))))
      networkFromLabel = projectLabels.get(config.vpcConfig.highSecurityProjectNetworkLabel)
      subnetworkFromLabel = projectLabels.get(config.vpcConfig.highSecurityProjectSubnetworkLabel)
      (network, subnetwork) <- (networkFromLabel, subnetworkFromLabel) match {
        // If we found project labels, we're done
        case (Some(network), Some(subnet)) =>
          Async[F].pure((NetworkName(network), SubnetworkName(subnet)))
        // Otherwise, we potentially need to create the network and subnet
        case (None, None) =>
          for {
            // create the network
            _ <- createIfAbsent(
              params.project,
              googleComputeService.getNetwork(params.project, config.vpcConfig.networkName),
              googleComputeService.createNetwork(params.project, buildNetwork(params.project)),
              NetworkNotReadyException(params.project, config.vpcConfig.networkName)
            )
            // If we specify autoCreateSubnetworks, a subnet is automatically created in each region with the same name as the network.
            // See https://cloud.google.com/vpc/docs/vpc#subnet-ranges
            subnetworkName <- if (config.vpcConfig.autoCreateSubnetworks) {
              Async[F].pure(SubnetworkName(config.vpcConfig.networkName.value))
            } else {
              // create the subnet
              createIfAbsent(
                params.project,
                googleComputeService.getSubnetwork(params.project,
                                                   config.vpcConfig.subnetworkRegion,
                                                   config.vpcConfig.subnetworkName),
                googleComputeService.createSubnetwork(params.project,
                                                      config.vpcConfig.subnetworkRegion,
                                                      buildSubnetwork(params.project)),
                SubnetworkNotReadyException(params.project, config.vpcConfig.subnetworkName)
              ).as(config.vpcConfig.subnetworkName)
            }
          } yield (config.vpcConfig.networkName, subnetworkName)
        case _ =>
          Async[F].raiseError[(NetworkName, SubnetworkName)](InvalidVPCSetupException(params.project))
      }
    } yield (network, subnetwork)

  override def setUpProjectFirewalls(
    params: SetUpProjectFirewallsParams
  )(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    for {
      // create the https firewall rule
      _ <- createIfAbsent(
        params.project,
        googleComputeService.getFirewallRule(params.project, config.vpcConfig.httpsFirewallRule.name),
        googleComputeService.addFirewallRule(params.project,
                                             buildFirewall(params.project, config.vpcConfig.httpsFirewallRule)),
        FirewallNotReadyException(params.project, config.vpcConfig.httpsFirewallRule.name)
      )
      // if the default network exists, do some maintenance there
      defaultNetwork <- googleComputeService.getNetwork(params.project, defaultNetwork)
      _ <- if (defaultNetwork.isDefined) {
        for {
          // create the internal ssh firewall rule in the default network
          _ <- createIfAbsent(
            params.project,
            googleComputeService.getFirewallRule(params.project, config.vpcConfig.sshFirewallRule.name),
            googleComputeService.addFirewallRule(params.project,
                                                 buildFirewall(params.project, config.vpcConfig.sshFirewallRule)),
            FirewallNotReadyException(params.project, config.vpcConfig.httpsFirewallRule.name)
          )
          // clean up some firewall rules like allow-ssh rule
          _ <- config.vpcConfig.firewallsToRemove
            .parTraverse_(fw => googleComputeService.deleteFirewallRule(params.project, fw))
        } yield ()
      } else Async[F].unit
    } yield ()

  private def createIfAbsent[A](project: GoogleProject, get: F[Option[A]], create: F[Operation], fail: Throwable)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[Unit] =
    for {
      existing <- get
      _ <- if (existing.isEmpty) {
        for {
          initialOp <- create
          lastOp <- googleComputeService
            .pollOperation(project, initialOp, config.vpcConfig.pollPeriod, config.vpcConfig.maxAttempts)
            .compile
            .lastOrError
          _ <- if (lastOp.isDone) Async[F].unit else Async[F].raiseError[Unit](fail)
        } yield ()
      } else Async[F].unit
    } yield ()

  private[util] def buildNetwork(project: GoogleProject): Network =
    Network
      .newBuilder()
      .setName(config.vpcConfig.networkName.value)
      .setAutoCreateSubnetworks(config.vpcConfig.autoCreateSubnetworks)
      .build

  private[util] def buildSubnetwork(project: GoogleProject): Subnetwork =
    Subnetwork
      .newBuilder()
      .setName(config.vpcConfig.subnetworkName.value)
      .setRegion(config.vpcConfig.subnetworkRegion.value)
      .setNetwork(config.vpcConfig.networkName.value)
      .setIpCidrRange(config.vpcConfig.subnetworkIpRange.value)
      .build

  private[util] def buildFirewall(googleProject: GoogleProject, fwConfig: FirewallRuleConfig): Firewall =
    Firewall
      .newBuilder()
      .setName(fwConfig.name.value)
      .setNetwork(buildNetworkUri(googleProject, fwConfig.network))
      .addAllSourceRanges(fwConfig.sourceRanges.map(_.value).asJava)
      .addTargetTags(config.vpcConfig.networkTag.value)
      .addAllowed(
        Allowed
          .newBuilder()
          .setIPProtocol(fwConfig.protocol)
          .addPorts(fwConfig.port.toString)
          .build
      )
      .build
}
