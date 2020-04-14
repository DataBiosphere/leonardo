package org.broadinstitute.dsde.workbench.leonardo.dao

import org.broadinstitute.dsde.workbench.leonardo.RuntimeContainerServiceType.{
  JupyterService,
  RStudioService,
  WelderService
}
import org.broadinstitute.dsde.workbench.leonardo.{RuntimeContainerServiceType, RuntimeName}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

trait ToolDAO[F[_], A] {
  def isProxyAvailable(googleProject: GoogleProject, runtimeName: RuntimeName): F[Boolean]
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
          (googleProject: GoogleProject, runtimeName: RuntimeName) =>
            jupyterDAO.isProxyAvailable(googleProject, runtimeName)
        case WelderService =>
          (googleProject: GoogleProject, runtimeName: RuntimeName) =>
            welderDAO.isProxyAvailable(googleProject, runtimeName)
        case RStudioService =>
          (googleProject: GoogleProject, runtimeName: RuntimeName) =>
            rstudioDAO.isProxyAvailable(googleProject, runtimeName)
      }
}
