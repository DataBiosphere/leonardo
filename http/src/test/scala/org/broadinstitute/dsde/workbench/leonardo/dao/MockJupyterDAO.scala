package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.IO
import org.broadinstitute.dsde.workbench.leonardo.RuntimeName
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

class MockJupyterDAO(isUp: Boolean = true) extends JupyterDAO[IO] {
  override def isProxyAvailable(googleProject: GoogleProject, clusterName: RuntimeName): IO[Boolean] =
    IO.pure(isUp)

  override def isAllKernelsIdle(googleProject: GoogleProject, clusterName: RuntimeName): IO[Boolean] =
    IO.pure(isUp)

  override def createTerminal(googleProject: GoogleProject, runtimeName: RuntimeName): IO[Unit] = IO.unit

  override def terminalExists(googleProject: GoogleProject,
                              runtimeName: RuntimeName,
                              terminalName: TerminalName): IO[Boolean] = IO.pure(true)
}

object MockJupyterDAO extends MockJupyterDAO(isUp = true)
