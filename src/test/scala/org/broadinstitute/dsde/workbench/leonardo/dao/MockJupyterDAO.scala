package org.broadinstitute.dsde.workbench.leonardo.dao
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.Future

object MockJupyterDAO extends JupyterDAO {
  override def isProxyAvailable(googleProject: GoogleProject, clusterName: ClusterName): Future[Boolean] = Future.successful(true)

  override def isAllKernalsIdle(googleProject: GoogleProject, clusterName: ClusterName): Future[Boolean] = Future.successful(true)
}

class MockJupyterDAO(isUp: Boolean) extends JupyterDAO {
  val mockReturn = Future.successful(isUp)
  override def isProxyAvailable(googleProject: GoogleProject, clusterName: ClusterName): Future[Boolean] = mockReturn

  override def isAllKernalsIdle(googleProject: GoogleProject, clusterName: ClusterName): Future[Boolean] = mockReturn
}
