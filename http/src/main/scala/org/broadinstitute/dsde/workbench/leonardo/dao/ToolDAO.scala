package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.IO
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterContainerServiceType
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterContainerServiceType._
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

trait ToolDAO[A] {
  def isProxyAvailable(googleProject: GoogleProject, clusterName: ClusterName): IO[Boolean]
}

object ToolDAO {
  def clusterToolToToolDao(
    jupyterDAO: JupyterDAO[IO],
    welderDAO: WelderDAO[IO],
    rstudioDAO: RStudioDAO[IO]
  ): ClusterContainerServiceType => ToolDAO[ClusterContainerServiceType] =
    clusterTool =>
      clusterTool match {
        case JupyterService =>
          (googleProject: GoogleProject, clusterName: ClusterName) =>
            jupyterDAO.isProxyAvailable(googleProject, clusterName)
        case WelderService =>
          (googleProject: GoogleProject, clusterName: ClusterName) =>
            welderDAO.isProxyAvailable(googleProject, clusterName)
        case RStudioService =>
          (googleProject: GoogleProject, clusterName: ClusterName) =>
            rstudioDAO.isProxyAvailable(googleProject, clusterName)
      }
}
