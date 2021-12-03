package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.IO
import org.broadinstitute.dsde.workbench.leonardo.{CloudContext, RuntimeName}

class MockRStudioDAO(isUp: Boolean = true) extends RStudioDAO[IO] {
  override def isProxyAvailable(cloudContext: CloudContext, clusterName: RuntimeName): IO[Boolean] =
    IO.pure(isUp)
}

object MockRStudioDAO extends MockRStudioDAO(true)
