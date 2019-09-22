package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.IO
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.Future

trait ToolDAO {
  def isProxyAvailable(googleProject: GoogleProject, clusterName: ClusterName): Future[Boolean]
}

object ToolDAO {
  def jupyterToolDAO(jupyterDAO: JupyterDAO): ToolDAO = (googleProject: GoogleProject, clusterName: ClusterName) => jupyterDAO.isProxyAvailable(googleProject, clusterName)
  def welderToolDAO(welderDAO: WelderDAO[IO]): ToolDAO = (googleProject: GoogleProject, clusterName: ClusterName) => welderDAO.isProxyAvailable(googleProject, clusterName).unsafeToFuture()
  def rSutdioToolDAO(rstudioDAO: RStudioDAO): ToolDAO = (googleProject: GoogleProject, clusterName: ClusterName) => rstudioDAO.isProxyAvailable(googleProject, clusterName)
}