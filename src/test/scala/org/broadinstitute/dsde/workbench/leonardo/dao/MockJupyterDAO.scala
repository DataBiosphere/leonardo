package org.broadinstitute.dsde.workbench.leonardo.dao
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.Future

class MockJupyterDAO extends ToolDAO{

  override def isProxyAvailable(googleProject: GoogleProject, clusterName: ClusterName): Future[Boolean] = Future.successful(true)
}
