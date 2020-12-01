package org.broadinstitute.dsde.workbench.leonardo

import enumeratum.{Enum, EnumEntry}

/**
 * Google Compute Instance Status
 *  See: https://cloud.google.com/compute/docs/instances/checking-instance-status
 */
sealed trait GceInstanceStatus extends EnumEntry with Product with Serializable
object GceInstanceStatus extends Enum[GceInstanceStatus] {
  val values = findValues

  // NOTE: Remember to update the definition of this enum in Swagger when you add new ones
  case object Provisioning
      extends GceInstanceStatus // Resources are being allocated for the instance. The instance is not running yet.
  case object Staging
      extends GceInstanceStatus // Resources have been acquired and the instance is being prepared for first boot.
  case object Running
      extends GceInstanceStatus // The instance is booting up or running. You can connect to the instance shortly after it enters this state.
  case object Stopping
      extends GceInstanceStatus //The instance is being stopped. This can be because a user has made a request to stop the instance or there was a failure. This is a temporary status and the instance will move to TERMINATED once the instance has stopped.
  case object Stopped extends GceInstanceStatus
  case object Suspending extends GceInstanceStatus
  case object Suspended extends GceInstanceStatus
  case object Terminated extends GceInstanceStatus
}
