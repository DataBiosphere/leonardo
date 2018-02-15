package org.broadinstitute.dsde.workbench.leonardo.model.google

import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import enumeratum._
import org.broadinstitute.dsde.workbench.model.{ValueObject, ValueObjectFormat}
import org.broadinstitute.dsde.workbench.model.google.GoogleModelJsonSupport._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import spray.json.{DefaultJsonProtocol, DeserializationException, JsString, JsValue, JsonFormat, RootJsonFormat}

import scala.language.implicitConversions

// Primitives
case class ClusterName(value: String) extends ValueObject
case class ZoneUri(value: String) extends ValueObject
case class MachineType(value: String) extends ValueObject

// Cluster machine configuration
case class MachineConfig(numberOfWorkers: Option[Int] = None,
                         masterMachineType: Option[String] = None,
                         masterDiskSize: Option[Int] = None,  //min 10
                         workerMachineType: Option[String] = None,
                         workerDiskSize: Option[Int] = None,   //min 10
                         numberOfWorkerLocalSSDs: Option[Int] = None, //min 0 max 8
                         numberOfPreemptibleWorkers: Option[Int] = None)

// Dataproc Operation
case class OperationName(value: String) extends ValueObject
case class Operation(name: OperationName, uuid: UUID)

// Dataproc Role
sealed trait DataprocRole extends EnumEntry
object DataprocRole extends Enum[DataprocRole] {
  val values = findValues

  case object Master extends DataprocRole
  case object Worker extends DataprocRole
  case object SecondaryWorker extends DataprocRole
}

// Information about error'd clusters
case class ClusterErrorDetails(code: Int, message: Option[String])

// Cluster status
sealed trait ClusterStatus extends EnumEntry
object ClusterStatus extends Enum[ClusterStatus] {
  val values = findValues

  // NOTE: Remember to update the definition of this enum in Swagger when you add new ones
  case object Unknown  extends ClusterStatus
  case object Creating extends ClusterStatus
  case object Running  extends ClusterStatus
  case object Updating extends ClusterStatus
  case object Error    extends ClusterStatus
  case object Deleting extends ClusterStatus
  case object Deleted  extends ClusterStatus
  case object Stopping extends ClusterStatus
  case object Stopped  extends ClusterStatus
  case object Starting extends ClusterStatus

  // TODO explain these better
  val activeStatuses: Set[ClusterStatus] = Set(Unknown, Creating, Running, Updating, Stopping, Stopped, Starting)
  val deletableStatuses: Set[ClusterStatus] = Set(Unknown, Creating, Running, Updating, Error, Stopping, Stopped, Starting)
  val monitoredStatuses: Set[ClusterStatus] = Set(Unknown, Creating, Updating, Deleting, Stopping, Starting)
  val stoppableStatuses: Set[ClusterStatus] = Set(Unknown, Running, Updating, Error, Starting)
  val startableStatuses: Set[ClusterStatus] = Set(Stopped, Stopping)

  implicit class EnrichedClusterStatus(status: ClusterStatus) {
    def isActive: Boolean = activeStatuses contains status
    def isDeletable: Boolean = deletableStatuses contains status
    def isMonitored: Boolean = monitoredStatuses contains status
    def isStoppable: Boolean = stoppableStatuses contains status
    def isStartable: Boolean = startableStatuses contains status
  }
}

// VPC networking
case class IP(value: String) extends ValueObject
case class NetworkTag(value: String) extends ValueObject
case class FirewallRuleName(value: String) extends ValueObject
case class FirewallRulePort(value: String) extends ValueObject
case class FirewallRuleNetwork(value: String) extends ValueObject
case class FirewallRuleProtocol(value: String) extends ValueObject
case class FirewallRule(name: FirewallRuleName, protocol: FirewallRuleProtocol, ports: List[FirewallRulePort], network: FirewallRuleNetwork, targetTags: List[NetworkTag])

// Instances
case class InstanceName(value: String) extends ValueObject

sealed trait InstanceStatus extends EnumEntry
object InstanceStatus extends Enum[InstanceStatus] {
  val values = findValues

  case object Provisioning extends InstanceStatus
  case object Staging      extends InstanceStatus
  case object Running      extends InstanceStatus
  case object Stopping     extends InstanceStatus
  case object Stopped      extends InstanceStatus
  case object Suspending   extends InstanceStatus
  case object Suspended    extends InstanceStatus
  case object Terminated   extends InstanceStatus

  case object Deleting     extends InstanceStatus
  case object Deleted      extends InstanceStatus
}

case class InstanceKey(project: GoogleProject,
                       zone: ZoneUri,
                       name: InstanceName)

case class Instance(key: InstanceKey,
                    googleId: BigInt,
                    status: InstanceStatus,
                    ip: Option[IP],
                    dataprocRole: Option[DataprocRole],
                    createdDate: Instant,
                    destroyedDate: Option[Instant])

object GoogleJsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  implicit object UUIDFormat extends JsonFormat[UUID] {
    def write(obj: UUID) = JsString(obj.toString)

    def read(json: JsValue): UUID = json match {
      case JsString(uuid) => UUID.fromString(uuid)
      case other => throw DeserializationException("Expected UUID, got: " + other)
    }
  }

  implicit val ClusterNameFormat = ValueObjectFormat(ClusterName)
  implicit val MachineTypeFormat = ValueObjectFormat(MachineType)
  implicit val ZoneUriFormat = ValueObjectFormat(ZoneUri)

  implicit val MachineConfigFormat = jsonFormat7(MachineConfig.apply)

  implicit val OperationNameFormat = ValueObjectFormat(OperationName)
  implicit val OperationFormat = jsonFormat2(Operation)

  implicit val ClusterErrorDetailsFormat = jsonFormat2(ClusterErrorDetails)

  case class EnumEntryFormat[T <: EnumEntry](create: String => T) extends RootJsonFormat[T] {
    def read(obj: JsValue): T = obj match {
      case JsString(value) => create(value)
      case _ => throw new DeserializationException(s"could not deserialize $obj")
    }

    def write(obj: T): JsValue = JsString(obj.entryName)
  }

  implicit val ClusterStatusFormat = EnumEntryFormat(ClusterStatus.withName)

  implicit val IPFormat = ValueObjectFormat(IP)
  implicit val NetworkTagFormat = ValueObjectFormat(NetworkTag)
  implicit val FirewallRuleNameFormat = ValueObjectFormat(FirewallRuleName)
  implicit val FirewallRulePortFormat = ValueObjectFormat(FirewallRulePort)
  implicit val FirewallRuleProtocolFormat = ValueObjectFormat(FirewallRuleProtocol)
  implicit val FirewallRuleNetworkFormat = ValueObjectFormat(FirewallRuleNetwork)
  implicit val FirewallRuleFormat = jsonFormat5(FirewallRule)

  implicit val InstanceNameFormat = ValueObjectFormat(InstanceName)
  implicit val DataprocRoleFormat = EnumEntryFormat(DataprocRole.withName)
  implicit val InstanceStatusFormat = EnumEntryFormat(InstanceStatus.withName)
  implicit val InstanceKeyFormat = jsonFormat3(InstanceKey)
  implicit val InstanceFormat = jsonFormat7(Instance)
}