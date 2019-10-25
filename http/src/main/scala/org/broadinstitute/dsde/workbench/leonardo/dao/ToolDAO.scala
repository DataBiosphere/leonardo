package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.IO
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterTool
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterTool._

import scala.concurrent.Future

trait ToolDAO[A] {
  def isProxyAvailable(googleProject: GoogleProject, clusterName: ClusterName): Future[Boolean]
}

object ToolDAO {
  def clusterToolToToolDao(jupyterDAO: JupyterDAO,
                           welderDAO: WelderDAO[IO],
                           rstudioDAO: RStudioDAO): ClusterTool => ToolDAO[ClusterTool] =
    clusterTool =>
      clusterTool match {
        case Jupyter =>
          (googleProject: GoogleProject, clusterName: ClusterName) =>
            jupyterDAO.isProxyAvailable(googleProject, clusterName)
        case Welder =>
          (googleProject: GoogleProject, clusterName: ClusterName) =>
            welderDAO.isProxyAvailable(googleProject, clusterName).unsafeToFuture()
        case RStudio =>
          (googleProject: GoogleProject, clusterName: ClusterName) =>
            rstudioDAO.isProxyAvailable(googleProject, clusterName)
      }
}
