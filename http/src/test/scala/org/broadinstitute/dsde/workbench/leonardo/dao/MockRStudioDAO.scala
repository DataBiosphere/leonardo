package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.IO
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

class MockRStudioDAO(isUp: Boolean = true) extends RStudioDAO[IO] {
  override def isProxyAvailable(googleProject: GoogleProject, clusterName: ClusterName): IO[Boolean] =
    IO.pure(isUp)
}

object MockRStudioDAO extends MockRStudioDAO(true)
