package org.broadinstitute.dsde.workbench.leonardo.config

import scala.concurrent.duration.FiniteDuration

case class CacheConfig(cacheExpiryTime: FiniteDuration, cacheMaxSize: Int)
