package org.broadinstitute.dsde.workbench.leonardo.util

import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.google2.{NetworkName, SubnetworkName}
import org.broadinstitute.dsde.workbench.leonardo.config.VPCConfig
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

trait VPCAlgebra[F[_]] {

  def setUpProjectNetwork(params: SetUpProjectNetworkParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[(NetworkName, SubnetworkName)]

  def setUpProjectFirewalls(params: SetUpProjectFirewallsParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit]

}

final case class VPCInterpreterConfig(vpcConfig: VPCConfig)
final case class SetUpProjectNetworkParams(project: GoogleProject)
final case class SetUpProjectFirewallsParams(project: GoogleProject, networkName: NetworkName)