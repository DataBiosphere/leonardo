package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.IO
import org.broadinstitute.dsde.workbench.leonardo.RuntimeName
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

class MockWelderDAO(isUp: Boolean = true) extends WelderDAO[IO] {
  override def isProxyAvailable(googleProject: GoogleProject, clusterName: RuntimeName): IO[Boolean] = IO.pure(isUp)

  override def flushCache(googleProject: GoogleProject, clusterName: RuntimeName): IO[Unit] = IO.unit
}

object MockWelderDAO extends MockWelderDAO(isUp = true)
