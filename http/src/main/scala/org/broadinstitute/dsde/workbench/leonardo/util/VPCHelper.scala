package org.broadinstitute.dsde.workbench.leonardo.util

import cats.effect.{ContextShift, IO}
import cats.mtl.ApplicativeAsk
import com.google.cloud.compute.v1.{Allowed, Firewall}
import org.broadinstitute.dsde.workbench.google.GoogleProjectDAO
import org.broadinstitute.dsde.workbench.google2.{FirewallRuleName, GoogleComputeService}
import org.broadinstitute.dsde.workbench.leonardo.{NetworkTag, VPCConfig}
import org.broadinstitute.dsde.workbench.leonardo.VPCConfig.{VPCNetwork, VPCSubnet}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.collection.JavaConverters._

class VPCHelper(config: VPCHelperConfig,
                googleProjectDAO: GoogleProjectDAO,
                googleComputeService: GoogleComputeService[IO])(implicit contextShift: ContextShift[IO]) {

  def getOrCreateVPCSettings(googleProject: GoogleProject): IO[VPCConfig] =
    for {
      // In priority order, use:
      // 1. the subnet specified by project label
      // 2. the network specified by project label
      // 3. the default network
      // TODO in the future create a dedicated subnet instead of using the default
      projectLabels <- IO.fromFuture(IO(googleProjectDAO.getLabels(googleProject.value)))
      subnetFromLabel = projectLabels.get(config.projectVPCSubnetLabelName).map(VPCSubnet)
      networkFromLabel = projectLabels.get(config.projectVPCNetworkLabelName).map(VPCNetwork)
      res = subnetFromLabel.orElse(networkFromLabel).getOrElse(VPCConfig.defaultNetwork)
    } yield res

  def getOrCreateFirewallRule(googleProject: GoogleProject)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Unit] =
    for {
      ruleOpt <- googleComputeService.getFirewallRule(googleProject, config.firewallRuleName)
      _ <- if (ruleOpt.isEmpty) {
        // Firewall rules don't work with a subnet, so we have to pass a network. This is
        // either specified by project label or the default network.
        for {
          projectLabels <- IO.fromFuture(IO(googleProjectDAO.getLabels(googleProject.value)))
          networkFromLabel = projectLabels.get(config.projectVPCNetworkLabelName).map(VPCNetwork)
          network = networkFromLabel.getOrElse(VPCConfig.defaultNetwork)
          _ <- googleComputeService.addFirewallRule(googleProject, buildFirewall(googleProject, network))
        } yield ()
      } else IO.unit
    } yield ()

  private[util] def buildFirewall(googleProject: GoogleProject, vpcNetwork: VPCNetwork): Firewall =
    Firewall
      .newBuilder()
      .setName(config.firewallRuleName.value)
      // Note networks are global, not regional. See:
      // https://cloud.google.com/vpc/docs/vpc
      .setNetwork(s"projects/${googleProject.value}/global/networks/${vpcNetwork.value}")
      .addAllTargetTags(config.firewallRuleTargetTags.map(_.value).asJava)
      .addAllowed(
        Allowed
          .newBuilder()
          .setIPProtocol(config.firewallRuleProtocol)
          .addPorts(config.firewallRulePort.toString)
          .build
      )
      .build
}

final case class VPCHelperConfig(projectVPCNetworkLabelName: String,
                                 projectVPCSubnetLabelName: String,
                                 firewallRuleName: FirewallRuleName,
                                 firewallRuleProtocol: String = "tcp",
                                 firewallRulePort: Int = 443,
                                 firewallRuleTargetTags: List[NetworkTag])
