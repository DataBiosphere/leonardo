package org.broadinstitute.dsde.workbench.leonardo.dao

import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import scala.concurrent.Future

trait ToolDAO {
  def isProxyAvailable(googleProject: GoogleProject, clusterName: ClusterName): Future[Boolean]
  def isAllKernalsIdle(googleProject: GoogleProject, clusterName: ClusterName): Future[Boolean]
}
