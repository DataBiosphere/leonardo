package org.broadinstitute.dsde.workbench.leonardo
package dao

import _root_.io.circe._
import _root_.io.circe.syntax._
import ca.mrvisser.sealerate
import cats.mtl.Ask
import com.azure.resourcemanager.compute.models.VirtualMachineSizeTypes
import org.broadinstitute.dsde.workbench.azure._
import org.broadinstitute.dsde.workbench.google2.RegionName
//TODO: IA-4175 prune
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec.{
  azureImageEncoder,
  azureMachineTypeEncoder,
  googleProjectDecoder,
  regionDecoder,
  runtimeNameEncoder,
  storageContainerNameDecoder,
  storageContainerNameEncoder,
  workspaceIdDecoder,
  wsmControlledResourceIdDecoder,
  wsmControlledResourceIdEncoder,
  wsmJobIdDecoder,
  wsmJobIdEncoder
}
import org.broadinstitute.dsde.workbench.leonardo.dao.LandingZoneResourcePurpose.LandingZoneResourcePurpose
import org.broadinstitute.dsde.workbench.leonardo.http.service.VMCredential
import org.broadinstitute.dsde.workbench.leonardo.util.PollDiskParams
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.http4s.headers.Authorization

import java.time.ZonedDateTime
import java.util.UUID

trait WsmDao[F[_]] {
  def getCreateVmJobResult(request: GetJobResultRequest, authorization: Authorization)(implicit
    ev: Ask[F, AppContext]
  ): F[GetCreateVmJobResult]

  def getLandingZoneResources(billingProfileId: BillingProfileId, userToken: Authorization)(implicit
    ev: Ask[F, AppContext]
  ): F[LandingZoneResources]

  // TODO: pt2
  def getWorkspaceStorageContainer(workspaceId: WorkspaceId, authorization: Authorization)(implicit
    ev: Ask[F, AppContext]
  ): F[Option[StorageContainerResponse]]
}

final case class StorageContainerRequest(storageContainerName: ContainerName)
final case class CreateStorageContainerRequest(workspaceId: WorkspaceId,
                                               commonFields: InternalDaoControlledResourceCommonFields,
                                               storageContainerReq: StorageContainerRequest
)
final case class CreateStorageContainerResult(resourceId: WsmControlledResourceId)
final case class WorkspaceDescription(id: WorkspaceId,
                                      displayName: String,
                                      spendProfile: String,
                                      azureContext: Option[AzureCloudContext],
                                      gcpContext: Option[GoogleProject]
)

//Landing Zone models
final case class LandingZone(landingZoneId: UUID,
                             billingProfileId: UUID,
                             definition: String,
                             version: String,
                             createdDate: String
)
final case class ListLandingZonesResult(landingzones: List[LandingZone])

// A LandingZoneResource will have either a resourceId or a resourceName + resourceParentId
final case class LandingZoneResource(resourceId: Option[String],
                                     resourceType: String,
                                     resourceName: Option[String],
                                     resourceParentId: Option[String],
                                     region: String,
                                     tags: Option[Map[String, String]]
)

object LandingZoneResourcePurpose extends Enumeration {
  type LandingZoneResourcePurpose = Value
  val SHARED_RESOURCE, WLZ_RESOURCE = Value
  val WORKSPACE_COMPUTE_SUBNET, WORKSPACE_STORAGE_SUBNET, AKS_NODE_POOL_SUBNET, POSTGRESQL_SUBNET, POSTGRES_ADMIN,
    WORKSPACE_BATCH_SUBNET = Value
}

final case class LandingZoneResourcesByPurpose(purpose: LandingZoneResourcePurpose,
                                               deployedResources: List[LandingZoneResource]
)
final case class ListLandingZoneResourcesResult(id: UUID, resources: List[LandingZoneResourcesByPurpose])

//Azure Vm Models
final case class CreateVmRequest(workspaceId: WorkspaceId,
                                 common: InternalDaoControlledResourceCommonFields,
                                 vmData: CreateVmRequestData,
                                 jobControl: WsmJobControl
)

final case class ProtectedSettings(fileUris: List[String], commandToExecute: String)
final case class CustomScriptExtension(name: String,
                                       publisher: String,
                                       `type`: String,
                                       version: String,
                                       minorVersionAutoUpgrade: Boolean,
                                       protectedSettings: ProtectedSettings
)
final case class StorageContainerResponse(name: ContainerName, resourceId: WsmControlledResourceId)
final case class CreateVmRequestData(name: RuntimeName,
                                     vmSize: VirtualMachineSizeTypes,
                                     vmImage: AzureImage,
                                     customScriptExtension: CustomScriptExtension,
                                     vmUserCredential: VMCredential,
                                     diskId: WsmControlledResourceId
)

final case class WsmVMMetadata(resourceId: WsmControlledResourceId)
final case class WsmVMAttributes(region: RegionName)
final case class WsmVm(metadata: WsmVMMetadata, attributes: WsmVMAttributes)

final case class DeleteWsmResourceRequest(workspaceId: WorkspaceId,
                                          resourceId: WsmControlledResourceId,
                                          deleteRequest: WsmDaoDeleteControlledAzureResourceRequest
)
final case class CreateVmResult(jobReport: WsmJobReport, errorReport: Option[WsmErrorReport])

final case class GetCreateVmJobResult(vm: Option[WsmVm], jobReport: WsmJobReport, errorReport: Option[WsmErrorReport])

final case class GetDeleteJobResult(jobReport: WsmJobReport, errorReport: Option[WsmErrorReport])

sealed trait ResourceAttributes extends Serializable with Product
object ResourceAttributes {
  final case class StorageContainerResourceAttributes(name: ContainerName) extends ResourceAttributes
}

final case class WsmResourceMetadata(resourceId: WsmControlledResourceId)
final case class WsmResource(metadata: WsmResourceMetadata, resourceAttributes: ResourceAttributes)
final case class GetWsmResourceResponse(resources: List[WsmResource])
final case class GetJobResultRequest(workspaceId: WorkspaceId, jobId: WsmJobId)

// Azure Disk models
final case class CreateDiskRequest(workspaceId: WorkspaceId,
                                   common: InternalDaoControlledResourceCommonFields,
                                   diskData: CreateDiskRequestData
)

final case class CreateDiskRequestData(name: AzureDiskName, size: DiskSize)

final case class CreateDiskResponse(resourceId: WsmControlledResourceId)

final case class CreateDiskForRuntimeResult(resourceId: WsmControlledResourceId, pollParams: Option[PollDiskParams])

// Common Controlled resource models
final case class InternalDaoControlledResourceCommonFields(name: ControlledResourceName,
                                                           description: ControlledResourceDescription,
                                                           cloningInstructions: CloningInstructions,
                                                           accessScope: AccessScope,
                                                           managedBy: ManagedBy,
                                                           privateResourceUser: Option[PrivateResourceUser],
                                                           resourceId: Option[WsmControlledResourceId]
)

final case class ControlledResourceName(value: String) extends AnyVal
final case class ControlledResourceDescription(value: String) extends AnyVal
final case class PrivateResourceUser(userName: WorkbenchEmail, privateResourceIamRoles: ControlledResourceIamRole)

final case class WsmErrorReport(message: String, statusCode: Int, causes: List[String])
final case class WsmJobReport(id: WsmJobId,
                              description: String,
                              status: WsmJobStatus,
                              statusCode: Int,
                              submitted: ZonedDateTime,
                              completed: Option[ZonedDateTime],
                              resultUrl: String
)

final case class WsmJobControl(id: WsmJobId)
final case class WsmDaoDeleteControlledAzureResourceRequest(jobControl: WsmJobControl)

final case class DeleteWsmResourceResult(jobReport: WsmJobReport, errorReport: Option[WsmErrorReport])

final case class WsmGcpContext(projectId: GoogleProject)

sealed abstract class WsmJobStatus
object WsmJobStatus {
  case object Running extends WsmJobStatus {
    override def toString: String = "RUNNING"
  }
  case object Succeeded extends WsmJobStatus {
    override def toString: String = "SUCCEEDED"
  }
  case object Failed extends WsmJobStatus {
    override def toString: String = "FAILED"
  }

  def values: Set[WsmJobStatus] = sealerate.values[WsmJobStatus]

  def stringToObject: Map[String, WsmJobStatus] = values.map(v => v.toString -> v).toMap
}

sealed abstract class ControlledResourceIamRole
object ControlledResourceIamRole {
  case object Reader extends ControlledResourceIamRole {
    override def toString: String = "READER"
  }
  case object Writer extends ControlledResourceIamRole {
    override def toString: String = "WRITER"
  }
  case object Editor extends ControlledResourceIamRole {
    override def toString: String = "EDITOR"
  }

  def values: Set[ControlledResourceIamRole] = sealerate.values[ControlledResourceIamRole]

  def stringToObject: Map[String, ControlledResourceIamRole] = values.map(v => v.toString -> v).toMap
}

sealed abstract class CloningInstructions
object CloningInstructions {
  case object Nothing extends CloningInstructions {
    override def toString: String = "COPY_NOTHING"
  }
  case object Definition extends CloningInstructions {
    override def toString: String = "COPY_DEFINITION"
  }
  case object Resource extends CloningInstructions {
    override def toString: String = "COPY_RESOURCE"
  }
  case object Reference extends CloningInstructions {
    override def toString: String = "COPY_REFERENCE"
  }

  def values: Set[CloningInstructions] = sealerate.values[CloningInstructions]

  def stringToObject: Map[String, CloningInstructions] = values.map(v => v.toString -> v).toMap
}

sealed abstract class AccessScope

object AccessScope {
  case object SharedAccess extends AccessScope {
    override def toString: String = "SHARED_ACCESS"
  }

  case object PrivateAccess extends AccessScope {
    override def toString: String = "PRIVATE_ACCESS"
  }

  def values: Set[AccessScope] = sealerate.values[AccessScope]

  def stringToObject: Map[String, AccessScope] = values.map(v => v.toString -> v).toMap
}

sealed abstract class ManagedBy

object ManagedBy {
  case object User extends ManagedBy {
    override def toString: String = "USER"
  }

  case object Application extends ManagedBy {
    override def toString: String = "APPLICATION"
  }

  def values: Set[ManagedBy] = sealerate.values[ManagedBy]

  def stringToObject: Map[String, ManagedBy] = values.map(v => v.toString -> v).toMap
}
// End Common Controlled resource models

object WsmDecoders {
  implicit val createDiskResponseDecoder: Decoder[CreateDiskResponse] = Decoder.instance { c =>
    for {
      id <- c.downField("resourceId").as[UUID]
    } yield CreateDiskResponse(WsmControlledResourceId(id))
  }

  implicit val metadataDecoder: Decoder[WsmVMMetadata] = Decoder.instance { c =>
    for {
      id <- c.downField("resourceId").as[UUID]
    } yield WsmVMMetadata(WsmControlledResourceId(id))
  }

  implicit val vmAttributesDecoder: Decoder[WsmVMAttributes] = Decoder.instance { a =>
    for {
      region <- a.downField("region").as[RegionName]
    } yield WsmVMAttributes(region)
  }

  implicit val createVmResponseDecoder: Decoder[WsmVm] = Decoder.instance { c =>
    for {
      m <- c.downField("metadata").as[WsmVMMetadata]
      a <- c.downField("attributes").as[WsmVMAttributes]
    } yield WsmVm(m, a)
  }

  implicit val createStorageContainerResultDecoder: Decoder[CreateStorageContainerResult] =
    Decoder.forProduct1("resourceId")(CreateStorageContainerResult.apply)

  implicit val azureContextDecoder: Decoder[AzureCloudContext] = Decoder.instance { c =>
    for {
      tenantId <- c.downField("tenantId").as[String]
      subscriptionId <- c.downField("subscriptionId").as[String]
      resourceGroupId <- c.downField("resourceGroupId").as[String]
    } yield AzureCloudContext(TenantId(tenantId),
                              SubscriptionId(subscriptionId),
                              ManagedResourceGroupName(resourceGroupId)
    )
  }

  implicit val landingZoneDecoder: Decoder[LandingZone] =
    Decoder.forProduct5("landingZoneId", "billingProfileId", "definition", "version", "createdDate")(LandingZone.apply)
  implicit val listLandingZonesResultDecoder: Decoder[ListLandingZonesResult] =
    Decoder.forProduct1("landingzones")(ListLandingZonesResult.apply)

  implicit val landingZoneResourceDecoder: Decoder[LandingZoneResource] =
    Decoder.forProduct6("resourceId", "resourceType", "resourceName", "resourceParentId", "region", "tags")(
      LandingZoneResource.apply
    )

  implicit val landingZoneResourcePurposeDecoder: Decoder[LandingZoneResourcePurpose] =
    Decoder.decodeString.emap(s =>
      LandingZoneResourcePurpose.values.find(_.toString == s).toRight(s"Invalid LandingZoneResourcePurpose found: ${s}")
    )
  implicit val landingZoneResourcesByPurposeDecoder: Decoder[LandingZoneResourcesByPurpose] =
    Decoder.forProduct2("purpose", "deployedResources")(LandingZoneResourcesByPurpose.apply)
  implicit val listLandingZoneResourcesResultDecoder: Decoder[ListLandingZoneResourcesResult] =
    Decoder.forProduct2("id", "resources")(ListLandingZoneResourcesResult.apply)

  implicit val wsmGcpContextDecoder: Decoder[WsmGcpContext] =
    Decoder.forProduct1("gcpContext")(WsmGcpContext.apply)

  implicit val getWorkspaceResponseDecoder: Decoder[WorkspaceDescription] = Decoder.instance { c =>
    for {
      id <- c.downField("id").as[WorkspaceId]
      displayName <- c.downField("displayName").as[String]
      spendProfile <- c.downField("spendProfile").as[String]
      azureContext <- c.downField("azureContext").as[Option[AzureCloudContext]]
      gcpContext <- c.downField("gcpContext").as[Option[WsmGcpContext]]
    } yield WorkspaceDescription(id, displayName, spendProfile, azureContext, gcpContext.map(_.projectId))
  }

  implicit val wsmJobStatusDecoder: Decoder[WsmJobStatus] =
    Decoder.decodeString.emap(s => WsmJobStatus.stringToObject.get(s).toRight(s"Invalid WsmJobStatus found: $s"))

  implicit val wsmJobReportDecoder: Decoder[WsmJobReport] = Decoder.instance { c =>
    for {
      id <- c.downField("id").as[WsmJobId]
      description <- c.downField("description").as[String]
      status <- c.downField("status").as[WsmJobStatus]
      statusCode <- c.downField("statusCode").as[Int]
      submitted <- c.downField("submitted").as[ZonedDateTime]
      completed <- c.downField("completed").as[Option[ZonedDateTime]]
      resultUrl <- c.downField("resultURL").as[String]
    } yield WsmJobReport(id, description, status, statusCode, submitted, completed, resultUrl)
  }

  implicit val wsmErrorReportDecoder: Decoder[WsmErrorReport] =
    Decoder.forProduct3("message", "statusCode", "causes")(WsmErrorReport.apply)

  implicit val deleteControlledAzureResourceResponseDecoder: Decoder[DeleteWsmResourceResult] = Decoder.instance { c =>
    for {
      jobReport <- c.downField("jobReport").as[WsmJobReport]
      errorReport <- c.downField("errorReport").as[Option[WsmErrorReport]]
    } yield DeleteWsmResourceResult(jobReport, errorReport)
  }

  implicit val createVmResultDecoder: Decoder[CreateVmResult] =
    Decoder.forProduct2("jobReport", "errorReport")(CreateVmResult.apply)

  implicit val storageContainerResourceAttributesDecoder
    : Decoder[ResourceAttributes.StorageContainerResourceAttributes] =
    Decoder.forProduct1("storageContainerName")(ResourceAttributes.StorageContainerResourceAttributes.apply)
  implicit val resourceAttributesDecoder: Decoder[ResourceAttributes] =
    Decoder.instance { x =>
      x.downField("azureStorageContainer").as[ResourceAttributes.StorageContainerResourceAttributes]
    }
  implicit val wsmResourceMetadataDecoder: Decoder[WsmResourceMetadata] =
    Decoder.forProduct1("resourceId")(WsmResourceMetadata.apply)
  implicit val wsmResourceeDecoder: Decoder[WsmResource] =
    Decoder.forProduct2("metadata", "resourceAttributes")(WsmResource.apply)
  implicit val getRelayNamespaceDecoder: Decoder[GetWsmResourceResponse] =
    Decoder.forProduct1("resources")(GetWsmResourceResponse.apply)

  implicit val getCreateVmResultDecoder: Decoder[GetCreateVmJobResult] =
    Decoder.forProduct3("azureVm", "jobReport", "errorReport")(GetCreateVmJobResult.apply)
  implicit val getDeleteVmResultDecoder: Decoder[GetDeleteJobResult] =
    Decoder.forProduct2("jobReport", "errorReport")(GetDeleteJobResult.apply)
}

object WsmEncoders {
  implicit val controlledResourceIamRoleEncoder: Encoder[ControlledResourceIamRole] =
    Encoder.encodeString.contramap(x => x.toString)
  implicit val privateResourceUserEncoder: Encoder[PrivateResourceUser] =
    Encoder.forProduct2("userName", "privateResourceIamRole")(x => (x.userName.value, x.privateResourceIamRoles))
  implicit val wsmCommonFieldsEncoder: Encoder[InternalDaoControlledResourceCommonFields] =
    Encoder.forProduct7("name",
                        "description",
                        "cloningInstructions",
                        "accessScope",
                        "managedBy",
                        "privateResourceUser",
                        "resourceId"
    )(x =>
      (x.name.value,
       x.description.value,
       x.cloningInstructions.toString,
       x.accessScope.toString,
       x.managedBy.toString,
       x.privateResourceUser,
       x.resourceId
      )
    )

  implicit val diskRequestDataEncoder: Encoder[CreateDiskRequestData] =
    Encoder.forProduct2("name", "size")(x => (x.name.value, x.size.gb))
  implicit val createDiskRequestEncoder: Encoder[CreateDiskRequest] =
    Encoder.forProduct2("common", "azureDisk")(x => (x.common, x.diskData))

  implicit val protectedSettingsEncoder: Encoder[ProtectedSettings] = Encoder.instance { x =>
    val fileUrisMap = Map(
      "key" -> "fileUris".asJson,
      "value" -> x.fileUris.asJson
    )
    val cmdToExecuteMap = Map(
      "key" -> "commandToExecute".asJson,
      "value" -> x.commandToExecute.asJson
    )
    List(
      fileUrisMap,
      cmdToExecuteMap
    ).asJson
  }
  implicit val customScriptExtensionEncoder: Encoder[CustomScriptExtension] =
    Encoder.forProduct6("name", "publisher", "type", "version", "minorVersionAutoUpgrade", "protectedSettings")(x =>
      (x.name, x.publisher, x.`type`, x.version, x.minorVersionAutoUpgrade, x.protectedSettings)
    )
  implicit val vmCrendentialnEncoder: Encoder[VMCredential] =
    Encoder.forProduct2("name", "password")(x => (x.username, x.password))

  implicit val vmRequestDataEncoder: Encoder[CreateVmRequestData] =
    Encoder.forProduct6("name", "vmSize", "vmImage", "customScriptExtension", "vmUser", "diskId")(x =>
      (x.name, x.vmSize, x.vmImage, x.customScriptExtension, x.vmUserCredential, x.diskId)
    )
  implicit val wsmJobControlEncoder: Encoder[WsmJobControl] = Encoder.forProduct1("id")(x => x.id)

  implicit val createVmRequestEncoder: Encoder[CreateVmRequest] =
    Encoder.forProduct3("common", "azureVm", "jobControl")(x => (x.common, x.vmData, x.jobControl))

  implicit val storageContainerRequestEncoder: Encoder[StorageContainerRequest] =
    Encoder.forProduct1("storageContainerName")(x => x.storageContainerName)

  implicit val createStorageContainerRequestEncoder: Encoder[CreateStorageContainerRequest] =
    Encoder.forProduct2("common", "azureStorageContainer")(x => (x.commonFields, x.storageContainerReq))

  implicit val deleteControlledAzureResourceRequestEncoder: Encoder[WsmDaoDeleteControlledAzureResourceRequest] =
    Encoder.forProduct1("jobControl")(x => x.jobControl)
}

final case class WsmException(traceId: TraceId, message: String) extends Exception(message)
