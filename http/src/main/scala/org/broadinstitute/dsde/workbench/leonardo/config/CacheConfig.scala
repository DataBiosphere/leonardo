package org.broadinstitute.dsde.workbench.leonardo
package config
import scala.concurrent.duration.FiniteDuration

case class CacheConfig(cacheExpiryTime: FiniteDuration, cacheMaxSize: Int)
