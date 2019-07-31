package org.broadinstitute.dsde.workbench.leonardo.dao

import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.Future

object MockWelderDAO extends WelderDAO {
  override def flushCache(googleProject: GoogleProject, clusterName: ClusterName): Future[Unit] = Future.unit

  override def isProxyAvailable(googleProject: GoogleProject, clusterName: ClusterName): Future[Boolean] = Future.successful(true)
}

class MockWelderDAO(isUp: Boolean) extends WelderDAO {
  override def isProxyAvailable(googleProject: GoogleProject, clusterName: ClusterName): Future[Boolean] =  Future.successful(isUp)

  override def flushCache(googleProject: GoogleProject, clusterName: ClusterName): Future[Unit] = Future.unit
}
