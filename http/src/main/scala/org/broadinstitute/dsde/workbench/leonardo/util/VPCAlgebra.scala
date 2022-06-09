package org.broadinstitute.dsde.workbench.leonardo.util

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.google2.{NetworkName, RegionName, SubnetworkName}
import org.broadinstitute.dsde.workbench.leonardo.config.VPCConfig
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

trait VPCAlgebra[F[_]] {

  def setUpProjectNetworkAndFirewalls(params: SetUpProjectNetworkParams)(implicit
    ev: Ask[F, TraceId]
  ): F[(NetworkName, SubnetworkName)]

}

final case class VPCInterpreterConfig(vpcConfig: VPCConfig)
final case class SetUpProjectNetworkParams(project: GoogleProject, region: RegionName)
final case class SetUpProjectFirewallsParams(project: GoogleProject,
                                             networkName: NetworkName,
                                             region: RegionName,
                                             projectLabels: Map[String, String]
)
