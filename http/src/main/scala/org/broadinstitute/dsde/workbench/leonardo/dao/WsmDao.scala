package org.broadinstitute.dsde.workbench.leonardo.dao

import java.util.UUID

import ca.mrvisser.sealerate
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.{WorkspaceId, AppContext}
import com.azure.core.management.Region
import com.azure.resourcemanager.compute.models.VirtualMachineSizeTypes
import _root_.io.circe._
import org.broadinstitute.dsde.workbench.model.TraceId

trait WsmDao[F[_]] {

//  def createVm()(implicit ev: Ask[F, AppContext]): F[CreateVmResponse]
//
//  def getCreateVmResult

  def createIp(request: CreateIpRequest)(implicit ev: Ask[F, AppContext]): F[CreateIpResponse]

  def createDisk(request: CreateDiskRequest)(implicit ev: Ask[F, AppContext]): F[CreateDiskResponse]

  def createNetwork(request: CreateNetworkRequest)(implicit ev: Ask[F, AppContext]): F[CreateNetworkResponse]

  def createVm(request: CreateVmRequestData)(implicit ev: Ask[F, AppContext]): F[CreateVmResponse]
  //persist resourceId

//  def getCreateVmResult(id)
}

//Azure Vm Models
final case class CreateVmRequest(workspaceId: WorkspaceId,
                                 common: ControlledResourceCommonFields,
                                 vmData: CreateVmRequestData)

final case class CreateVmRequestData(workspaceId: WorkspaceId, name: AzureVmName, region: Region, vmSize: VirtualMachineSizeTypes, vmImageUri: AzureImageUri, ipId: WsmControlledResourceId, diskId: WsmControlledResourceId, networkId: WsmControlledResourceId)

final case class AzureVmName(value: String) extends AnyVal
final case class AzureImageUri(value: String) extends AnyVal

final case class CreateVmResponse(resourceId: WsmControlledResourceId)

// Azure IP models
final case class CreateIpRequest(workspaceId: WorkspaceId,
                                 common: ControlledResourceCommonFields,
                                 ipData: CreateIpRequestData)

final case class CreateIpRequestData(name: AzureIpName, region: Region)

final case class AzureIpName(value: String) extends AnyVal

final case class CreateIpResponse(resourceId: WsmControlledResourceId)

// Azure Disk models
final case class CreateDiskRequest(workspaceId: WorkspaceId,
                                   common: ControlledResourceCommonFields,
                                   diskData: CreateDiskRequestData)

final case class CreateDiskRequestData(name: AzureDiskName, size: AzureDiskSize, region: Region)

final case class AzureDiskName(value: String) extends AnyVal
final case class AzureDiskSize(GB: Int) extends AnyVal

final case class CreateDiskResponse(resourceId: WsmControlledResourceId)

//Network models
final case class CreateNetworkRequest(workspaceId: WorkspaceId,
                                      common: ControlledResourceCommonFields,
                                      networkData: CreateNetworkRequestData)

final case class CreateNetworkRequestData(networkName: AzureNetworkName, subnetName: AzureSubnetName, addressSpaceCidr: AddressSpaceCidr, subnetAddressCidr: SubnetAddressCidr, region: Region)

final case class AzureNetworkName(value: String) extends AnyVal
final case class AzureSubnetName(value: String) extends AnyVal

//should be in the format 0.0.0.0/0
final case class AddressSpaceCidr(value: String) extends AnyVal
final case class SubnetAddressCidr(value: String) extends AnyVal

// TODO: The other fields should just be an echo of the original request, i.e. the WSM model. Decoding not needed
final case class CreateNetworkResponse(resourceId: WsmControlledResourceId)

//End network models

// Begin Common Controlled resource models
final case class WsmControlledResourceId(id: UUID)

final case class ControlledResourceCommonFields(name: ControlledResourceName,
                                                description: ControlledResourceDescription,
                                                cloningInstructions: CloningInstructions,
                                                accessScope: AccessScope,
                                                managedBy: ManagedBy,
                                                privateResourceUser: Option[PrivateResourceUser])
//TODO is PrivateResourceUser relevant?

final case class ControlledResourceName(value: String) extends AnyVal
final case class ControlledResourceDescription(value: String) extends AnyVal
final case class PrivateResourceUser(userName: UserEmail, privateResourceIamRoles: List[ControlledResourceIamRole])

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

final case class UserEmail(value: String) extends AnyVal

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

final object WsmDecoders {
  implicit val createIpResponseDecoder: Decoder[CreateIpResponse] = Decoder.instance { c =>
    for {
      id <- c.downField("resourceId").as[UUID]
    } yield CreateIpResponse(WsmControlledResourceId(id))
  }

  implicit val createDiskResponseDecoder: Decoder[CreateDiskResponse] = Decoder.instance { c =>
    for {
      id <- c.downField("resourceId").as[UUID]
    } yield CreateDiskResponse(WsmControlledResourceId(id))
  }

  implicit val createNetworkResponseDecoder: Decoder[CreateNetworkResponse] = Decoder.instance { c =>
    for {
      id <- c.downField("resourceId").as[UUID]
    } yield CreateNetworkResponse(WsmControlledResourceId(id))
  }

  implicit val createVmResponseDecoder: Decoder[CreateVmResponse] = Decoder.instance { c =>
    for {
      id <- c.downField("resourceId").as[UUID]
    } yield CreateVmResponse(WsmControlledResourceId(id))
  }
}

final object WsmEncoders {
  implicit val controlledResourceIamRoleEncoder: Encoder[ControlledResourceIamRole] = Encoder.encodeString.contramap(x => x.toString)
  implicit val privateResourceUserEncoder: Encoder[PrivateResourceUser] = Encoder.forProduct2("userName", "privateResourceIamRoles")(x => (x.userName.value, x.privateResourceIamRoles))
  implicit val wsmCommonFieldsEncoder: Encoder[ControlledResourceCommonFields] = Encoder.forProduct6("name", "description", "cloningInstructions", "accessScope", "managedBy", "privateResourceUser")(x => (x.name.value, x.description.value, x.cloningInstructions.toString, x.accessScope.toString, x.managedBy.toString, x.privateResourceUser))

  implicit val ipRequestDataEncoder: Encoder[CreateIpRequestData] = Encoder.forProduct2("name", "region")(x => (x.name.value, x.region.toString))
  implicit val createIpRequestEncoder: Encoder[CreateIpRequest] = Encoder.forProduct2("common", "azureIp")(x => (x.common, x.ipData))

  implicit val diskRequestDataEncoder: Encoder[CreateDiskRequestData] = Encoder.forProduct3("name", "size", "region")(x => (x.name.value, x.size.GB, x.region.toString))
  implicit val createDiskRequestEncoder: Encoder[CreateDiskRequest] = Encoder.forProduct2("common", "azureDisk")(x => (x.common, x.diskData))

  implicit val networkRequestDataEncoder: Encoder[CreateNetworkRequestData] = Encoder.forProduct5("networkName", "subnetName", "addressSpaceCidr", "subnetAddressCidr", "region")(x => (x.networkName.value, x.subnetName.value, x.addressSpaceCidr.value, x.subnetAddressCidr.value, x.region.toString))
  implicit val createNetworkRequestEncoder: Encoder[CreateNetworkRequest] = Encoder.forProduct2("common", "azureNetwork")(x => (x.common, x.networkData))

  implicit val vmRequestDataEncoder: Encoder[CreateVmRequestData] = Encoder.forProduct7("name", "region", "vmSize", "vmImageUri", "ipId", "diskId", "networkId")(x => (x.name.value, x.region.toString, x.vmSize.toString, x.vmImageUri.toString, x.ipId.id.toString, x.diskId.id.toString, x.networkId.id.toString))
  implicit val createVmRequestEncoder: Encoder[CreateVmRequest] = Encoder.forProduct2("common", "azureVm")(x => (x.common, x.vmData))
}

final case class WsmException(traceId: TraceId, message: String) extends Exception(message)
