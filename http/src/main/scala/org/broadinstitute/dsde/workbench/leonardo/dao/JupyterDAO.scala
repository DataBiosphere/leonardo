package org.broadinstitute.dsde.workbench.leonardo.dao

import org.broadinstitute.dsde.workbench.leonardo.{CloudContext, RuntimeName}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

trait JupyterDAO[F[_]] {
  def isAllKernelsIdle(cloudContext: CloudContext, runtimeName: RuntimeName): F[Boolean]
  def isProxyAvailable(cloudContext: CloudContext, runtimeName: RuntimeName): F[Boolean]
  def createTerminal(googleProject: GoogleProject, runtimeName: RuntimeName): F[Unit]
  def terminalExists(googleProject: GoogleProject, runtimeName: RuntimeName, terminalName: TerminalName): F[Boolean]
}