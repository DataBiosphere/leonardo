package org.broadinstitute.dsde.workbench.leonardo.util

import cats.Parallel
import cats.effect.{Async, ContextShift, IO}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.google.cloud.compute.v1.{Allowed, Firewall, Network, Subnetwork}
import io.chrisdavenport.log4cats.Logger
import org.broadinstitute.dsde.workbench.google.GoogleProjectDAO
import org.broadinstitute.dsde.workbench.google2.{GoogleComputeService, NetworkName, SubnetworkName}
import org.broadinstitute.dsde.workbench.leonardo.config.{FirewallRuleConfig, VPCConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.google._
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import scala.collection.JavaConverters._

class VPCInterpreter[F[_]: Async: Parallel: ContextShift: Logger](
  config: VPCInterpreterConfig,
  googleProjectDAO: GoogleProjectDAO,
  googleComputeService: GoogleComputeService[F]
)(implicit cs: ContextShift[IO]) extends VPCAlgebra[F] {

  val defaultNetwork = NetworkName("default")

  override def setUpProjectNetwork(params: SetUpProjectNetworkParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[(NetworkName, SubnetworkName)] = {
    for {
      projectLabels <- Async[F].liftIO(IO.fromFuture(IO(googleProjectDAO.getLabels(params.project.value))))
      networkFromLabel = projectLabels.get(config.vpcConfig.highSecurityProjectNetworkLabel)
      subnetworkFromLabel = projectLabels.get(config.vpcConfig.highSecurityProjectSubnetworkLabel)
      (network, subnetwork) <- (networkFromLabel, subnetworkFromLabel) match {
        case (Some(network), Some(subnet)) =>
          Async[F].pure((NetworkName(network), SubnetworkName(subnet)))
        case (None, None) =>
          for {
            existingNetwork <- googleComputeService.getNetwork(params.project, config.vpcConfig.networkName)
            _ <- existingNetwork.fold(googleComputeService.createNetwork(params.project, buildNetwork(params.project)).void)(_ => Async[F].unit)
            existingSubnetwork <- googleComputeService.getSubnetwork(params.project, config.vpcConfig.subnetworkRegion, config.vpcConfig.subnetworkName)
            _ <- existingSubnetwork.fold(googleComputeService.createSubnetwork(params.project, config.vpcConfig.subnetworkRegion, buildSubnetwork(params.project)).void)(_ => Async[F].unit)
          } yield (config.vpcConfig.networkName, config.vpcConfig.subnetworkName)
        case _ =>
          Async[F].raiseError[(NetworkName, SubnetworkName)](new Exception("invalid vpc setup"))
      }
    } yield (network, subnetwork)
  }

  override def setUpProjectFirewalls(params: SetUpProjectFirewallsParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] = {
    for {
      existingHttpsFirewall <- googleComputeService.getFirewallRule(params.project, config.vpcConfig.httpsFirewallRule.name)
      _ <- existingHttpsFirewall.fold(googleComputeService.addFirewallRule(params.project, buildFirewall(params.project, config.vpcConfig.httpsFirewallRule)).void)(_ => Async[F].unit)
      defaultNetwork <- googleComputeService.getNetwork(params.project, defaultNetwork)
      _ <- if (defaultNetwork.isDefined) {
        for {
          existingSshFirewall <- googleComputeService.getFirewallRule(params.project, config.vpcConfig.sshFirewallRule.name)
          _ <- if (existingSshFirewall.isEmpty) {
            googleComputeService.addFirewallRule(params.project, buildFirewall(params.project, config.vpcConfig.sshFirewallRule)).void >>
              config.vpcConfig.firewallsToRemove.parTraverse_(fw => googleComputeService.deleteFirewallRule(params.project, fw))
          } else Async[F].unit
        } yield ()
      } else Async[F].unit
    } yield ()
  }

  private[util] def buildNetwork(project: GoogleProject): Network = {
    Network.newBuilder().setName(config.vpcConfig.networkName.value).build
  }

  private[util] def buildSubnetwork(project: GoogleProject): Subnetwork = {
    Subnetwork.newBuilder()
      .setName(config.vpcConfig.subnetworkName.value)
      .setRegion(config.vpcConfig.subnetworkRegion.value)
      .setNetwork(config.vpcConfig.networkName.value)
      .setIpCidrRange(config.vpcConfig.subnetworkIpRange.value)
      .build
  }

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