package org.broadinstitute.dsde.workbench.leonardo.model.google

import java.util.UUID

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import enumeratum._
import org.broadinstitute.dsde.workbench.model.{ValueObject, ValueObjectFormat, WorkbenchException}
import spray.json.{DefaultJsonProtocol, DeserializationException, JsString, JsValue, JsonFormat, RootJsonFormat}

import scala.language.implicitConversions

// Primitives
case class ClusterName(value: String) extends ValueObject
case class ZoneUri(value: String) extends ValueObject

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

  val deletableStatuses: Set[ClusterStatus] = Set(Unknown, Creating, Running, Updating, Error)

  implicit class EnrichedClusterStatus(status: ClusterStatus) {
    def isDeletable: Boolean = deletableStatuses contains status
  }
}


//object ClusterStatus extends Enumeration {
//  type ClusterStatus = Value
//  //NOTE: Remember to update the definition of this enum in Swagger when you add new ones
//  val Unknown, Creating, Running, Updating, Error, Deleting, Deleted = Value
//
//  val activeStatuses = Set(Unknown, Creating, Running, Updating)
//  val deletableStatuses = Set(Unknown, Creating, Running, Updating, Error)
//  val monitoredStatuses = Set(Unknown, Creating, Updating, Deleting)
//
//  class StatusValue(status: ClusterStatus) {
//    def isActive: Boolean = activeStatuses contains status
//    def isMonitored: Boolean = monitoredStatuses contains status
//    def isDeletable: Boolean = deletableStatuses contains status
//  }
//  implicit def enumConvert(status: ClusterStatus): StatusValue = new StatusValue(status)
//
//  def withNameOpt(s: String): Option[ClusterStatus] = values.find(_.toString == s)
//
//  def withNameIgnoreCase(str: String): ClusterStatus = {
//    values.find(_.toString.equalsIgnoreCase(str)).getOrElse(throw new IllegalArgumentException(s"Unknown cluster status: $str"))
//  }
//}

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
object DataprocRoles {
  sealed trait DataprocRole extends ValueObject
  case object Master extends DataprocRole { val value: String = "Master" }
  case object Worker extends DataprocRole { val value: String = "Worker" }

  def withName(name: String): DataprocRole = name.toLowerCase() match {
    case "master" => Master
    case "worker" => Worker
    case _ => throw new WorkbenchException(s"Invalid Dataproc role: $name")
  }
}
object InstanceStatuses {
  sealed trait InstanceStatus
}
//PROVISIONING, STAGING, RUNNING, STOPPING, STOPPED, SUSPENDING, SUSPENDED, and TERMINATED.

// status


object GoogleJsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  implicit object UUIDFormat extends JsonFormat[UUID] {
    def write(obj: UUID) = JsString(obj.toString)

    def read(json: JsValue): UUID = json match {
      case JsString(uuid) => UUID.fromString(uuid)
      case other => throw DeserializationException("Expected UUID, got: " + other)
    }
  }

  implicit val ClusterNameFormat = ValueObjectFormat(ClusterName)
  implicit val InstanceNameFormat = ValueObjectFormat(InstanceName)
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
}