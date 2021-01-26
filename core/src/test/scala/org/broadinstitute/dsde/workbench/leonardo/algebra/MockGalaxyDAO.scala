package org.broadinstitute.dsde.workbench.leonardo
package algebra

import cats.effect.IO
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

class MockGalaxyDAO(isUp: Boolean = true) extends GalaxyDAO[IO] {
  override def isProxyAvailable(googleProject: GoogleProject, appName: AppName): IO[Boolean] = IO.pure(isUp)
}

object MockGalaxyDAO extends MockGalaxyDAO(isUp = true)
