package org.broadinstitute.dsde.workbench.leonardo.dao

import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.Future

trait WelderDAO {
  def flushCache(googleProject: GoogleProject, clusterName: ClusterName): Future[Unit]
}
