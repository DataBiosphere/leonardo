package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.IO
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, CloudContext, RuntimeName}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.http4s.Uri
import org.http4s.headers.Authorization

class MockJupyterDAO(isUp: Boolean = true) extends JupyterDAO[IO] {
  override def isProxyAvailable(cloudContext: CloudContext, clusterName: RuntimeName): IO[Boolean] =
    IO.pure(isUp)

  override def isAllKernelsIdle(cloudContext: CloudContext, clusterName: RuntimeName): IO[Boolean] =
    IO.pure(isUp)

  override def createTerminal(googleProject: GoogleProject, runtimeName: RuntimeName): IO[Unit] = IO.unit

  override def terminalExists(googleProject: GoogleProject,
                              runtimeName: RuntimeName,
                              terminalName: TerminalName
  ): IO[Boolean] = IO.pure(true)

  override def getStatus(baseUri: Uri, authHeader: Authorization)(implicit ev: Ask[IO, AppContext]): IO[Boolean] =
    IO.pure(isUp)
}

object MockJupyterDAO extends MockJupyterDAO(isUp = true)
