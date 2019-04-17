package org.broadinstitute.dsde.workbench.leonardo.model.google

import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import enumeratum._
import org.broadinstitute.dsde.workbench.model.{ValueObject, ValueObjectFormat, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.GoogleModelJsonSupport._
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsPath, GoogleProject}
import spray.json.{DefaultJsonProtocol, DeserializationException, JsString, JsValue, JsonFormat, RootJsonFormat}

// Primitives
case class ClusterName(value: String) extends ValueObject
case class InstanceName(value: String) extends ValueObject
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

final case class CreateClusterConfig(
                                      machineConfig: MachineConfig,
                                      initScript: GcsPath,
                                      clusterServiceAccount: Option[WorkbenchEmail],
                                      credentialsFileName: Option[String],
                                      stagingBucket: GcsBucketName,
                                      clusterScopes: Set[String],
                                      vpcNetwork: Option[VPCNetworkName],
                                      vpcSubnet: Option[VPCSubnetName],
                                      properties: Map[String, String]) //valid properties are https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/cluster-properties
// Dataproc Operation
case class OperationName(value: String) extends ValueObject
case class Operation(name: OperationName, uuid: UUID)

// Dataproc Role (master, worker, secondary worker)
sealed trait DataprocRole extends EnumEntry with Product with Serializable
object DataprocRole extends Enum[DataprocRole] {
  val values = findValues

  case object Master extends DataprocRole
  case object Worker extends DataprocRole
  case object SecondaryWorker extends DataprocRole
}

// Information about error'd clusters
case class ClusterErrorDetails(code: Int, message: Option[String])

// Cluster status
// https://cloud.google.com/dataproc/docs/reference/rest/v1beta2/projects.regions.clusters#ClusterStatus
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

  // note: the below are Leo-specific statuses, not Dataproc statuses
  case object Deleted  extends ClusterStatus
  case object Stopping extends ClusterStatus
  case object Stopped  extends ClusterStatus
  case object Starting extends ClusterStatus

  // A user might need to connect to this notebook in the future. Keep it warm in the DNS cache.
  val activeStatuses: Set[ClusterStatus] = Set(Unknown, Creating, Running, Updating, Stopping, Stopped, Starting)

  // Can a user delete this cluster? Contains everything except Creating, Deleting, Deleted.
  val deletableStatuses: Set[ClusterStatus] = Set(Unknown, Running, Updating, Error, Stopping, Stopped, Starting)

  // Non-terminal statuses. Requires cluster monitoring via ClusterMonitorActor.
  val monitoredStatuses: Set[ClusterStatus] = Set(Unknown, Creating, Updating, Deleting, Stopping, Starting)

  // Can a user stop this cluster?
  val stoppableStatuses: Set[ClusterStatus] = Set(Unknown, Running, Updating, Starting)

  // Can a user start this cluster?
  val startableStatuses: Set[ClusterStatus] = Set(Stopped, Stopping)

  // Can a user update (i.e. resize) this cluster?
  val updatableStatuses: Set[ClusterStatus] = Set(Running, Stopped)

  implicit class EnrichedClusterStatus(status: ClusterStatus) {
    def isActive: Boolean = activeStatuses contains status
    def isDeletable: Boolean = deletableStatuses contains status
    def isMonitored: Boolean = monitoredStatuses contains status
    def isStoppable: Boolean = stoppableStatuses contains status
    def isStartable: Boolean = startableStatuses contains status
    def isUpdatable: Boolean = updatableStatuses contains status
  }
}

// VPC networking
case class IP(value: String) extends ValueObject
case class NetworkTag(value: String) extends ValueObject
case class FirewallRuleName(value: String) extends ValueObject
case class FirewallRulePort(value: String) extends ValueObject
case class FirewallRuleProtocol(value: String) extends ValueObject
case class VPCNetworkName(value: String) extends ValueObject
case class VPCSubnetName(value: String) extends ValueObject
case class FirewallRule(name: FirewallRuleName, protocol: FirewallRuleProtocol, ports: List[FirewallRulePort], network: Option[VPCNetworkName], targetTags: List[NetworkTag])

// Instance status
// See: https://cloud.google.com/compute/docs/instances/checking-instance-status
sealed trait InstanceStatus extends EnumEntry
object InstanceStatus extends Enum[InstanceStatus] {
  val values = findValues

  // NOTE: Remember to update the definition of this enum in Swagger when you add new ones
  case object Provisioning extends InstanceStatus
  case object Staging      extends InstanceStatus
  case object Running      extends InstanceStatus
  case object Stopping     extends InstanceStatus
  case object Stopped      extends InstanceStatus
  case object Suspending   extends InstanceStatus
  case object Suspended    extends InstanceStatus
  case object Terminated   extends InstanceStatus
}

case class InstanceKey(project: GoogleProject,
                       zone: ZoneUri,
                       name: InstanceName)

case class Instance(key: InstanceKey,
                    googleId: BigInt,
                    status: InstanceStatus,
                    ip: Option[IP],
                    dataprocRole: Option[DataprocRole],
                    createdDate: Instant)

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
  implicit val VPCNetworkNameFormat = ValueObjectFormat(VPCNetworkName)
  implicit val FirewallRuleFormat = jsonFormat5(FirewallRule)

  implicit val InstanceNameFormat = ValueObjectFormat(InstanceName)
  implicit val DataprocRoleFormat = EnumEntryFormat(DataprocRole.withName)
  implicit val InstanceStatusFormat = EnumEntryFormat(InstanceStatus.withName)
  implicit val InstanceKeyFormat = jsonFormat3(InstanceKey)
  implicit val InstanceFormat = jsonFormat6(Instance)
}