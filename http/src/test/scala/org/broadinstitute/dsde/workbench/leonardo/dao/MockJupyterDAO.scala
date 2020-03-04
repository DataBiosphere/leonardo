package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.IO
import org.broadinstitute.dsde.workbench.leonardo.RuntimeName
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

class MockJupyterDAO(isUp: Boolean = true) extends JupyterDAO[IO] {
  override def isProxyAvailable(googleProject: GoogleProject, clusterName: RuntimeName): IO[Boolean] =
    IO.pure(isUp)

  override def isAllKernelsIdle(googleProject: GoogleProject, clusterName: RuntimeName): IO[Boolean] =
    IO.pure(isUp)
}

object MockJupyterDAO extends MockJupyterDAO(isUp = true)
