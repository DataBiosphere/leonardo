package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.IO
import org.broadinstitute.dsde.workbench.leonardo.{CloudContext, RuntimeName}

class MockWelderDAO(isUp: Boolean = true) extends WelderDAO[IO] {
  override def isProxyAvailable(cloudContext: CloudContext, clusterName: RuntimeName): IO[Boolean] = IO.pure(isUp)

  override def flushCache(cloudContext: CloudContext, clusterName: RuntimeName): IO[Unit] = IO.unit
}

object MockWelderDAO extends MockWelderDAO(isUp = true)
