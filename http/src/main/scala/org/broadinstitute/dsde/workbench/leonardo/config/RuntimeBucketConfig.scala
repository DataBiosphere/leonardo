package org.broadinstitute.dsde.workbench.leonardo
package config
import scala.concurrent.duration.FiniteDuration

case class RuntimeBucketConfig(
  stagingBucketExpiration: FiniteDuration
)
