package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, CloudContext, RuntimeName}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.http4s.Uri
import org.http4s.headers.Authorization

trait JupyterDAO[F[_]] {
  def isAllKernelsIdle(cloudContext: CloudContext, runtimeName: RuntimeName): F[Boolean]
  def isProxyAvailable(cloudContext: CloudContext, runtimeName: RuntimeName): F[Boolean]
  def createTerminal(googleProject: GoogleProject, runtimeName: RuntimeName): F[Unit]
  def terminalExists(googleProject: GoogleProject, runtimeName: RuntimeName, terminalName: TerminalName): F[Boolean]
  def getStatus(baseUri: Uri, authHeader: Authorization)(implicit ev: Ask[F, AppContext]): F[Boolean]
}
