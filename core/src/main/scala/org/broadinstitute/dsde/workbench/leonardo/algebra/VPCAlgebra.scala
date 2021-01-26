package org.broadinstitute.dsde.workbench.leonardo
package algebra

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.google2.{FirewallRuleName, NetworkName, RegionName, SubnetworkName}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.duration.FiniteDuration

trait VPCAlgebra[F[_]] {

  def setUpProjectNetwork(params: SetUpProjectNetworkParams)(
    implicit ev: Ask[F, TraceId]
  ): F[(NetworkName, SubnetworkName)]

  def setUpProjectFirewalls(params: SetUpProjectFirewallsParams)(implicit ev: Ask[F, TraceId]): F[Unit]

}

final case class VPCInterpreterConfig(vpcConfig: VPCConfig)
final case class SetUpProjectNetworkParams(project: GoogleProject)
final case class SetUpProjectFirewallsParams(project: GoogleProject, networkName: NetworkName)

final case class VPCConfig(highSecurityProjectNetworkLabel: NetworkLabel,
                           highSecurityProjectSubnetworkLabel: SubnetworkLabel,
                           networkName: NetworkName,
                           networkTag: NetworkTag,
                           autoCreateSubnetworks: Boolean,
                           subnetworkName: SubnetworkName,
                           subnetworkRegion: RegionName,
                           subnetworkIpRange: IpRange,
                           firewallsToAdd: List[FirewallRuleConfig],
                           firewallsToRemove: List[FirewallRuleName],
                           pollPeriod: FiniteDuration,
                           maxAttempts: Int)

final case class FirewallRuleConfig(name: FirewallRuleName, sourceRanges: List[IpRange], allowed: List[Allowed])

final case class Allowed(protocol: String, port: Option[String])

final case class NetworkLabel(value: String) extends AnyVal
final case class SubnetworkLabel(value: String) extends AnyVal
