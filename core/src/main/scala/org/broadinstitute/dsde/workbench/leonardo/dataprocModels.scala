package org.broadinstitute.dsde.workbench.leonardo

import java.time.Instant

import ca.mrvisser.sealerate
import enumeratum.{Enum, EnumEntry}
import org.broadinstitute.dsde.workbench.google2.{InstanceName, ZoneName}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

final case class CustomDataprocImage(asString: String) extends AnyVal

/** Dataproc Instance Status
 *  See: https://cloud.google.com/compute/docs/instances/checking-instance-status */
sealed trait InstanceStatus extends EnumEntry
object InstanceStatus extends Enum[InstanceStatus] {
  val values = findValues

  // NOTE: Remember to update the definition of this enum in Swagger when you add new ones
  case object Provisioning extends InstanceStatus
  case object Staging extends InstanceStatus
  case object Running extends InstanceStatus
  case object Stopping extends InstanceStatus
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

/** Dataproc properties */
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
