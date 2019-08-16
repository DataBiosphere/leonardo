package org.broadinstitute.dsde.workbench.leonardo.dao
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.Future

class MockJupyterDAO(isUp: Boolean = true) extends JupyterDAO {
  override def isProxyAvailable(googleProject: GoogleProject, clusterName: ClusterName): Future[Boolean] = Future.successful(isUp)

  override def isAllKernalsIdle(googleProject: GoogleProject, clusterName: ClusterName): Future[Boolean] = Future.successful(isUp)
}

object MockJupyterDAO extends MockJupyterDAO(isUp = true)
