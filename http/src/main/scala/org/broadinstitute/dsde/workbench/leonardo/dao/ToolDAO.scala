package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.IO
import org.broadinstitute.dsde.workbench.leonardo.RuntimeContainerServiceType.{
  JupyterService,
  RStudioService,
  WelderService
}
import org.broadinstitute.dsde.workbench.leonardo.{RuntimeContainerServiceType, RuntimeName}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

trait ToolDAO[A] {
  def isProxyAvailable(googleProject: GoogleProject, runtimeName: RuntimeName): IO[Boolean]
}

object ToolDAO {
  def clusterToolToToolDao(
    jupyterDAO: JupyterDAO[IO],
    welderDAO: WelderDAO[IO],
    rstudioDAO: RStudioDAO[IO]
  ): RuntimeContainerServiceType => ToolDAO[RuntimeContainerServiceType] =
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
