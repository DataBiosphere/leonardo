package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.IO
import org.broadinstitute.dsde.workbench.leonardo.{RuntimeContainerServiceType, RuntimeName}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

class MockToolDAO(isProxyAvailable: Boolean) extends ToolDAO[RuntimeContainerServiceType] {
  override def isProxyAvailable(googleProject: GoogleProject, runtimeName: RuntimeName): IO[Boolean] =
    IO.pure(isProxyAvailable)
}

object MockToolDAO {
  def apply(isProxyAvailable: Boolean): MockToolDAO = new MockToolDAO(isProxyAvailable)
}
