package org.broadinstitute.dsde.workbench.leonardo.dao

import org.broadinstitute.dsde.workbench.leonardo.RuntimeContainerServiceType.{
  JupyterService,
  RStudioService,
  WelderService
}
import org.broadinstitute.dsde.workbench.leonardo.{CloudContext, RuntimeContainerServiceType, RuntimeName}

trait ToolDAO[F[_], A] {
  def isProxyAvailable(cloudContext: CloudContext, runtimeName: RuntimeName): F[Boolean]
}

object ToolDAO {
  def clusterToolToToolDao[F[_]](
    jupyterDAO: JupyterDAO[F],
    welderDAO: WelderDAO[F],
    rstudioDAO: RStudioDAO[F]
  ): RuntimeContainerServiceType => ToolDAO[F, RuntimeContainerServiceType] =
    clusterTool =>
      clusterTool match {
        case JupyterService =>
          (cloudContext: CloudContext, runtimeName: RuntimeName) =>
            jupyterDAO.isProxyAvailable(cloudContext, runtimeName, None)
        case WelderService =>
          (cloudContext: CloudContext, runtimeName: RuntimeName) =>
            welderDAO.isProxyAvailable(cloudContext, runtimeName, None)
        case RStudioService =>
          (cloudContext: CloudContext, runtimeName: RuntimeName) =>
            rstudioDAO.isProxyAvailable(cloudContext, runtimeName)
      }
}
