package org.broadinstitute.dsde.workbench.leonardo.util

import cats.effect.{Async, Blocker, ContextShift, IO}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.google.cloud.compute.v1.{Allowed, Firewall}
import io.chrisdavenport.log4cats.Logger
import org.broadinstitute.dsde.workbench.google.GoogleProjectDAO
import org.broadinstitute.dsde.workbench.google2.{FirewallRuleName, GoogleComputeService}
import org.broadinstitute.dsde.workbench.leonardo.{NetworkTag, VPCConfig}
import org.broadinstitute.dsde.workbench.leonardo.VPCConfig.{VPCNetwork, VPCSubnet}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.collection.JavaConverters._

class VPCHelper[F[_]: Async: ContextShift: Logger](config: VPCHelperConfig,
                                                   googleProjectDAO: GoogleProjectDAO,
                                                   googleComputeService: GoogleComputeService[F],
                                                   blocker: Blocker)(implicit cs: ContextShift[IO]) {

  private def getVPCSettingsFromProjectLabel(googleProject: GoogleProject): F[Option[VPCConfig]] =
    Async[F].liftIO(IO.fromFuture(IO(googleProjectDAO.getLabels(googleProject.value)))).map { labelMap =>
      labelMap.get(config.projectVPCNetworkLabelName).map(VPCNetwork) orElse
        labelMap.get(config.projectVPCSubnetLabelName).map(VPCSubnet)
    }

  // TODO
  private def createVPCSubnet(googleProject: GoogleProject): F[VPCConfig] = Async[F].pure(VPCNetwork("default"))

  def getOrCreateVPCSettings(googleProject: GoogleProject): F[VPCConfig] =
    for {
      fromProjectLabels <- getVPCSettingsFromProjectLabel(googleProject)
      res <- fromProjectLabels.fold(createVPCSubnet(googleProject))(Async[F].pure)
    } yield res

  def getOrCreateFirewallRule(googleProject: GoogleProject,
                              vpcConfig: VPCConfig)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    for {
      rule <- googleComputeService.getFirewallRule(googleProject, config.firewallRuleName)
      _ <- rule.fold(googleComputeService.addFirewallRule(googleProject, buildFirewall(googleProject, vpcConfig)))(
        _ => Async[F].unit
      )
    } yield ()

  private def buildFirewall(googleProject: GoogleProject, vpcConfig: VPCConfig): Firewall =
    Firewall
      .newBuilder()
      .setName(config.firewallRuleName.value)
      .setNetwork(networkUri(googleProject, vpcConfig))
      .addAllTargetTags(config.firewallRuleTargetTags.map(_.value).asJava)
      .addAllowed(
        Allowed
          .newBuilder()
          .setIPProtocol(config.firewallRuleProtocol)
          .addPorts(config.firewallRulePort.toString)
          .build
      )
      .build

  private def networkUri(googleProject: GoogleProject, vpcConfig: VPCConfig): String =
    s"projects/${googleProject.value}/global/networks/${vpcConfig.value}"

}

final case class VPCHelperConfig(projectVPCNetworkLabelName: String,
                                 projectVPCSubnetLabelName: String,
                                 firewallRuleName: FirewallRuleName,
                                 firewallRuleProtocol: String = "tcp",
                                 firewallRulePort: Int = 443,
                                 firewallRuleTargetTags: List[NetworkTag])
