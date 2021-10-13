package org.broadinstitute.dsde.workbench.leonardo

import java.time.Instant

import ca.mrvisser.sealerate
import enumeratum.{Enum, EnumEntry}
import org.broadinstitute.dsde.workbench.google2.{DataprocRole, InstanceName, ZoneName}
import org.broadinstitute.dsde.workbench.model.IP
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

/** An instance in a Dataproc cluster */
case class DataprocInstanceKey(project: GoogleProject, zone: ZoneName, name: InstanceName)
case class DataprocInstance(key: DataprocInstanceKey,
                            googleId: BigInt,
                            status: GceInstanceStatus,
                            ip: Option[IP],
                            dataprocRole: DataprocRole,
                            createdDate: Instant)

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

  case object Stopped extends DataprocClusterStatus // The cluster is being updated. It continues to accept and process jobs.
}
