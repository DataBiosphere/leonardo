package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.IO
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

class MockWelderDAO(isUp: Boolean = true) extends WelderDAO[IO] {
  override def isProxyAvailable(googleProject: GoogleProject, clusterName: ClusterName): IO[Boolean] = IO.pure(isUp)

  override def flushCache(googleProject: GoogleProject, clusterName: ClusterName): IO[Unit] = IO.unit
}

object MockWelderDAO extends MockWelderDAO(isUp = true)
