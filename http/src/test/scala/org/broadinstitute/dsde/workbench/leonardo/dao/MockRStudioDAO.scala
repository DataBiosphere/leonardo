package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.IO
import org.broadinstitute.dsde.workbench.leonardo.RuntimeName
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

class MockRStudioDAO(isUp: Boolean = true) extends RStudioDAO[IO] {
  override def isProxyAvailable(googleProject: GoogleProject, clusterName: RuntimeName): IO[Boolean] =
    IO.pure(isUp)
}

object MockRStudioDAO extends MockRStudioDAO(true)
