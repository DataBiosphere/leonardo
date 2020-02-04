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
                googleComputeService: GoogleComputeService[IO])(implicit contextShift: ContextShift[IO]
                                                                //  log: Logger[IO]
) {

  private def getVPCSettingsFromProjectLabel(googleProject: GoogleProject): IO[Option[VPCConfig]] =
    IO.fromFuture(IO(googleProjectDAO.getLabels(googleProject.value))).map { labelMap =>
      labelMap.get(config.projectVPCNetworkLabelName).map(VPCNetwork) orElse
        labelMap.get(config.projectVPCSubnetLabelName).map(VPCSubnet)
    }

  private def createVPCSubnet(googleProject: GoogleProject): IO[VPCConfig] = ???

  def getOrCreateVPCSettings(googleProject: GoogleProject): IO[VPCConfig] =
    for {
      fromProjectLabels <- getVPCSettingsFromProjectLabel(googleProject)
      res <- fromProjectLabels.fold(createVPCSubnet(googleProject))(IO.pure)
    } yield res

  def getOrCreateFirewallRule(googleProject: GoogleProject,
                              vpcConfig: VPCConfig)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    for {
      rule <- googleComputeService.getFirewallRule(googleProject, config.firewallRuleName)
      _ <- rule.fold(googleComputeService.addFirewallRule(googleProject, buildFirewall(vpcConfig)))(_ => IO.unit)
    } yield ()

  private def buildFirewall(vpcConfig: VPCConfig): Firewall =
    Firewall
      .newBuilder()
      .setName(config.firewallRuleName.value)
      .setNetwork(vpcConfig.value)
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
