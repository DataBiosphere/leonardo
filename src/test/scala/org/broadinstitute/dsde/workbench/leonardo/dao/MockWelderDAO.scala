package org.broadinstitute.dsde.workbench.leonardo.dao

import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.Future

class MockWelderDAO(isUp: Boolean = true) extends WelderDAO {
  override def isProxyAvailable(googleProject: GoogleProject, clusterName: ClusterName): Future[Boolean] =  Future.successful(isUp)

  override def flushCache(googleProject: GoogleProject, clusterName: ClusterName): Future[Unit] = Future.unit
}

object MockWelderDAO extends MockWelderDAO(isUp = true)