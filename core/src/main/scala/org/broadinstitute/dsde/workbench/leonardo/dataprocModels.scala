package org.broadinstitute.dsde.workbench.leonardo

import java.time.Instant

import ca.mrvisser.sealerate
import enumeratum.{Enum, EnumEntry}
import org.broadinstitute.dsde.workbench.google2.{InstanceName, ZoneName}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

/** Google Compute Instance Status
 *  See: https://cloud.google.com/compute/docs/instances/checking-instance-status */
sealed trait InstanceStatus extends EnumEntry
object InstanceStatus extends Enum[InstanceStatus] {
  val values = findValues

  // NOTE: Remember to update the definition of this enum in Swagger when you add new ones
  case object Provisioning extends InstanceStatus // Resources are being allocated for the instance. The instance is not running yet.
  case object Staging extends InstanceStatus // Resources have been acquired and the instance is being prepared for first boot.
  case object Running extends InstanceStatus // The instance is booting up or running. You can connect to the instance shortly after it enters this state.
  case object Stopping extends InstanceStatus //The instance is being stopped. This can be because a user has made a request to stop the instance or there was a failure. This is a temporary status and the instance will move to TERMINATED once the instance has stopped.
  case object Stopped extends InstanceStatus
  case object Suspending extends InstanceStatus
  case object Suspended extends InstanceStatus
  case object Terminated extends InstanceStatus
}

/** An instance in a Dataproc cluster */
case class DataprocInstanceKey(project: GoogleProject, zone: ZoneName, name: InstanceName)
case class DataprocInstance(key: DataprocInstanceKey,
                            googleId: BigInt,
                            status: InstanceStatus,
                            ip: Option[IP],
                            dataprocRole: DataprocRole,
                            createdDate: Instant)

/** Dataproc Role (master, worker, secondary worker) */
sealed trait DataprocRole extends EnumEntry with Product with Serializable
object DataprocRole extends Enum[DataprocRole] {
  val values = findValues

  case object Master extends DataprocRole
  case object Worker extends DataprocRole
  case object SecondaryWorker extends DataprocRole
}

/**
 * Dataproc properties
 *
 * See: https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/cluster-properties
 */
sealed abstract class PropertyFilePrefix
object PropertyFilePrefix {
  case object CapacityScheduler extends PropertyFilePrefix {
    override def toString: String = "capacity-scheduler"
  }
  case object Core extends PropertyFilePrefix {
    override def toString: String = "core"
  }
  case object Distcp extends PropertyFilePrefix {
    override def toString: String = "distcp"
  }
  case object HadoopEnv extends PropertyFilePrefix {
    override def toString: String = "hadoop-env"
  }
  case object Hdfs extends PropertyFilePrefix {
    override def toString: String = "hdfs"
  }
  case object Hive extends PropertyFilePrefix {
    override def toString: String = "hive"
  }
  case object Mapred extends PropertyFilePrefix {
    override def toString: String = "mapred"
  }
  case object MapredEnv extends PropertyFilePrefix {
    override def toString: String = "mapred-env"
  }
  case object Pig extends PropertyFilePrefix {
    override def toString: String = "pig"
  }
  case object Presto extends PropertyFilePrefix {
    override def toString: String = "presto"
  }
  case object PrestoJvm extends PropertyFilePrefix {
    override def toString: String = "presto-jvm"
  }
  case object Spark extends PropertyFilePrefix {
    override def toString: String = "spark"
  }
  case object SparkEnv extends PropertyFilePrefix {
    override def toString: String = "spark-env"
  }
  case object Yarn extends PropertyFilePrefix {
    override def toString: String = "yarn"
  }
  case object YarnEnv extends PropertyFilePrefix {
    override def toString: String = "yarn-env"
  }
  case object Zeppelin extends PropertyFilePrefix {
    override def toString: String = "zeppelin"
  }
  case object ZeppelinEnv extends PropertyFilePrefix {
    override def toString: String = "zeppelin-env"
  }
  case object Zookeeper extends PropertyFilePrefix {
    override def toString: String = "zookeeper"
  }
  case object Dataproc extends PropertyFilePrefix {
    override def toString: String = "dataproc"
  }

  def values: Set[PropertyFilePrefix] = sealerate.values[PropertyFilePrefix]

  def stringToObject: Map[String, PropertyFilePrefix] = values.map(v => v.toString -> v).toMap
}

// Dataproc statuses: https://googleapis.github.io/google-cloud-dotnet/docs/Google.Cloud.Dataproc.V1/api/Google.Cloud.Dataproc.V1.ClusterStatus.Types.State.html
sealed trait DataprocClusterStatus extends EnumEntry
object DataprocClusterStatus extends Enum[DataprocClusterStatus] {
  val values = findValues

  case object Creating extends DataprocClusterStatus // The cluster is being created and set up. It is not ready for use.

  case object Deleting extends DataprocClusterStatus // The cluster is being deleted. It cannot be used.

  case object Error extends DataprocClusterStatus // The cluster encountered an error. It is not ready for use.

  case object Running extends DataprocClusterStatus // The cluster is currently running and healthy. It is ready for use.

  case object Unknown extends DataprocClusterStatus // The cluster state is unknown.

  case object Updating extends DataprocClusterStatus // The cluster is being updated. It continues to accept and process jobs.
}
