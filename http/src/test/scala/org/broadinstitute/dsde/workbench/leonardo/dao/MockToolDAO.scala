package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.IO
import org.broadinstitute.dsde.workbench.leonardo.{CloudContext, RuntimeContainerServiceType, RuntimeName}

class MockToolDAO(isProxyAvailable: Boolean) extends ToolDAO[IO, RuntimeContainerServiceType] {
  override def isProxyAvailable(cloudContext: CloudContext, runtimeName: RuntimeName): IO[Boolean] =
    IO.pure(isProxyAvailable)
}

object MockToolDAO {
  def apply(isProxyAvailable: Boolean): MockToolDAO = new MockToolDAO(isProxyAvailable)
}
