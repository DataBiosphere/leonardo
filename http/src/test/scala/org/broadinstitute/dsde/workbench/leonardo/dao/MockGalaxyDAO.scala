package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.IO
import org.broadinstitute.dsde.workbench.leonardo.AppName
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

class MockGalaxyDAO(isUp: Boolean = true) extends GalaxyDAO[IO] {
  override def isProxyAvailable(googleProject: GoogleProject, appName: AppName): IO[Boolean] = IO.pure(isUp)
}

object MockGalaxyDAO extends MockGalaxyDAO(isUp = true)
