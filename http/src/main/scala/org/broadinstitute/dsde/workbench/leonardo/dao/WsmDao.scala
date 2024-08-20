package org.broadinstitute.dsde.workbench.leonardo
package dao

import _root_.io.circe._
import ca.mrvisser.sealerate
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.azure._
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec.{
  googleProjectDecoder,
  storageContainerNameDecoder,
  wsmControlledResourceIdDecoder
}
import org.broadinstitute.dsde.workbench.leonardo.dao.LandingZoneResourcePurpose.LandingZoneResourcePurpose
import org.broadinstitute.dsde.workbench.leonardo.util.PollDiskParams
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.http4s.headers.Authorization

import java.util.UUID

trait WsmDao[F[_]] {

  def getLandingZoneResources(billingProfileId: BillingProfileId, userToken: Authorization)(implicit
    ev: Ask[F, AppContext]
  ): F[LandingZoneResources]

  def getWorkspaceStorageContainer(workspaceId: WorkspaceId, authorization: Authorization)(implicit
    ev: Ask[F, AppContext]
  ): F[Option[StorageContainerResponse]]
}

final case class StorageContainerRequest(storageContainerName: ContainerName)
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
final case class StorageContainerResponse(name: ContainerName, resourceId: WsmControlledResourceId)

sealed trait ResourceAttributes extends Serializable with Product
object ResourceAttributes {
  final case class StorageContainerResourceAttributes(name: ContainerName) extends ResourceAttributes
}

final case class WsmResourceMetadata(resourceId: WsmControlledResourceId)
final case class WsmResource(metadata: WsmResourceMetadata, resourceAttributes: ResourceAttributes)
final case class GetWsmResourceResponse(resources: List[WsmResource])

// Azure Disk models

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

  implicit val wsmJobStatusDecoder: Decoder[WsmJobStatus] =
    Decoder.decodeString.emap(s => WsmJobStatus.stringToObject.get(s).toRight(s"Invalid WsmJobStatus found: $s"))

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
}

final case class WsmException(traceId: TraceId, message: String) extends Exception(message)
