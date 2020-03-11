package org.broadinstitute.dsde.workbench.leonardo.util

import cats.effect.{ContextShift, IO}
import cats.mtl.ApplicativeAsk
import com.google.cloud.compute.v1.{Allowed, Firewall}
import org.broadinstitute.dsde.workbench.google.GoogleProjectDAO
import org.broadinstitute.dsde.workbench.google2.{FirewallRuleName, GoogleComputeService, RegionName}
import org.broadinstitute.dsde.workbench.leonardo.{NetworkTag, VPCConfig}
import org.broadinstitute.dsde.workbench.leonardo.VPCConfig.{VPCNetwork, VPCSubnet}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.collection.JavaConverters._

class VPCHelper(config: VPCHelperConfig,
                googleProjectDAO: GoogleProjectDAO,
                googleComputeService: GoogleComputeService[IO])(implicit contextShift: ContextShift[IO]) {

  private def getVPCSettingsFromProjectLabel(googleProject: GoogleProject): IO[Option[VPCConfig]] =
    IO.fromFuture(IO(googleProjectDAO.getLabels(googleProject.value))).map { labelMap =>
      labelMap.get(config.projectVPCSubnetLabelName).map(VPCSubnet) orElse
        labelMap.get(config.projectVPCNetworkLabelName).map(VPCNetwork)
    }

  // TODO move toward creating our own dedicated subnet instead of using the default
  private def createVPCSubnet(googleProject: GoogleProject): IO[VPCConfig] = IO.pure(VPCConfig.default)

  def getOrCreateVPCSettings(googleProject: GoogleProject): IO[VPCConfig] =
    for {
      fromProjectLabels <- getVPCSettingsFromProjectLabel(googleProject)
      res <- fromProjectLabels.fold(createVPCSubnet(googleProject))(IO.pure)
    } yield res

  def getOrCreateFirewallRule(googleProject: GoogleProject,
                              vpcConfig: VPCConfig)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    for {
      rule <- googleComputeService.getFirewallRule(googleProject, config.firewallRuleName)
      _ <- rule.fold(googleComputeService.addFirewallRule(googleProject, buildFirewall(googleProject, vpcConfig)))(
        _ => IO.unit
      )
    } yield ()

  private def buildFirewall(googleProject: GoogleProject, vpcConfig: VPCConfig): Firewall =
    Firewall
      .newBuilder()
      .setName(config.firewallRuleName.value)
      .setNetwork(buildNetworkUri(googleProject, vpcConfig))
      .addAllTargetTags(config.firewallRuleTargetTags.map(_.value).asJava)
      .addAllowed(
        Allowed
          .newBuilder()
          .setIPProtocol(config.firewallRuleProtocol)
          .addPorts(config.firewallRulePort.toString)
          .build
      )
      .build

  def buildNetworkUri(googleProject: GoogleProject, vpcConfig: VPCConfig): String =
    // Note networks are global, subnets are regional.
    // See: https://cloud.google.com/vpc/docs/vpc
    vpcConfig match {
      case VPCNetwork(value) => s"projects/${googleProject.value}/global/networks/$value"
      case VPCSubnet(value) =>
        s"projects/${googleProject.value}/regions/${config.projectVPCSubnetRegion.value}/subnetworks/$value"
    }

}

final case class VPCHelperConfig(projectVPCNetworkLabelName: String,
                                 projectVPCSubnetLabelName: String,
                                 projectVPCSubnetRegion: RegionName,
                                 firewallRuleName: FirewallRuleName,
                                 firewallRuleProtocol: String = "tcp",
                                 firewallRulePort: Int = 443,
                                 firewallRuleTargetTags: List[NetworkTag])
