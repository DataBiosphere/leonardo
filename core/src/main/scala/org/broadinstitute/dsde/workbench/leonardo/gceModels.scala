package org.broadinstitute.dsde.workbench.leonardo

import enumeratum.{Enum, EnumEntry}
import ca.mrvisser.sealerate
import com.google.cloud.compute.v1.Instance.Status

/**
 * Google Compute Instance Status
 *  See: https://cloud.google.com/compute/docs/instances/checking-instance-status
 */
sealed trait GceInstanceStatus extends EnumEntry with Product with Serializable {
  def instanceStatus: Status
}
object GceInstanceStatus extends Enum[GceInstanceStatus] {
  val values = findValues

  // NOTE: Remember to update the definition of this enum in Swagger when you add new ones
  case object Provisioning extends GceInstanceStatus {
    override def instanceStatus: Status = Status.PROVISIONING
  } // Resources are being allocated for the instance. The instance is not running yet.
  case object Staging extends GceInstanceStatus {
    override def instanceStatus: Status = Status.STAGING
  } // Resources have been acquired and the instance is being prepared for first boot.
  case object Running extends GceInstanceStatus {
    override def instanceStatus: Status = Status.RUNNING
  } // The instance is booting up or running. You can connect to the instance shortly after it enters this state.
  case object Stopping extends GceInstanceStatus {
    override def instanceStatus: Status = Status.STOPPING
  } //The instance is being stopped. This can be because a user has made a request to stop the instance or there was a failure. This is a temporary status and the instance will move to TERMINATED once the instance has stopped.
  case object Stopped extends GceInstanceStatus {
    override def instanceStatus: Status = Status.STOPPED
  }
  case object Suspending extends GceInstanceStatus {
    override def instanceStatus: Status = Status.SUSPENDING
  }
  case object Suspended extends GceInstanceStatus {
    override def instanceStatus: Status = Status.SUSPENDED
  }
  case object Terminated extends GceInstanceStatus {
    override def instanceStatus: Status = Status.TERMINATED
  }
}

sealed abstract class GpuType extends Product with Serializable {
  def asString: String
}
object GpuType {
  final case object NvidiaTeslaT4 extends GpuType {
    def asString: String = "nvidia-tesla-t4"
  }
  final case object NvidiaTeslaV100 extends GpuType {
    def asString: String = "nvidia-tesla-v100"
  }
  final case object NvidiaTeslaP100 extends GpuType {
    def asString: String = "nvidia-tesla-p100"
  }
  final case object NvidiaTeslaP4 extends GpuType {
    def asString: String = "nvidia-tesla-p4"
  }
  final case object NvidiaTeslaK80 extends GpuType {
    def asString: String = "nvidia-tesla-k80"
  }

  def values: Set[GpuType] = sealerate.values[GpuType]
  def stringToObject: Map[String, GpuType] = values.map(v => v.asString -> v).toMap
}

final case class GpuConfig(gpuType: GpuType, numOfGpus: Int)
