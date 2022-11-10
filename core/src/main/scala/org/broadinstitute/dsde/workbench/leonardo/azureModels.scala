package org.broadinstitute.dsde.workbench.leonardo

import io.circe.{Decoder, Encoder}
import org.broadinstitute.dsde.workbench.azure.{AKSClusterName, RelayNamespace}
import org.broadinstitute.dsde.workbench.google2.{NetworkName, SubnetworkName}

import java.util.UUID

final case class WsmControlledResourceId(value: UUID) extends AnyVal

final case class AzureUnimplementedException(message: String) extends Exception {
  override def getMessage: String = message
}

final case class StorageAccountName(value: String) extends AnyVal

final case class WsmJobId(value: String) extends AnyVal

final case class ManagedIdentityName(value: String) extends AnyVal
final case class BatchAccountName(value: String) extends AnyVal

final case class LandingZoneResources(clusterName: AKSClusterName,
                                      batchAccountName: BatchAccountName,
                                      relayNamespace: RelayNamespace,
                                      storageAccountName: StorageAccountName,
                                      vnetName: NetworkName,
                                      batchNodesSubnetName: SubnetworkName,
                                      aksSubnetName: SubnetworkName
)

trait AzureObjectCodec {
  implicit val aksClusterNameDecoder: Decoder[AKSClusterName] = Decoder.forProduct1("value")(AKSClusterName.apply)
  implicit val batchAccountNameDecoder: Decoder[BatchAccountName] = Decoder.forProduct1("value")(BatchAccountName.apply)
  implicit val relayNamespaceDecoder: Decoder[RelayNamespace] = Decoder.forProduct1("value")(RelayNamespace.apply)
  implicit val storageAccountNameDecoder: Decoder[StorageAccountName] = Decoder.forProduct1("value")(StorageAccountName.apply)
  implicit val vnetNameDecoder: Decoder[NetworkName] = Decoder.forProduct1("value")(NetworkName.apply)
  implicit val subnetworkNameDecoder: Decoder[SubnetworkName] = Decoder.forProduct1("value")(SubnetworkName.apply)

  implicit val landingZoneResourcesDecoder: Decoder[LandingZoneResources] =
    Decoder.forProduct7("clusterName",
      "batchAccountName",
      "relayNamespace",
      "storageAccountName",
      "vnetName",
      "batchNodesSubnetName",
      "aksSubnetName")(LandingZoneResources.apply)

  implicit val aksClusterNameEncoder: Encoder[AKSClusterName] = Encoder.encodeString.contramap(_.value.toString)
  implicit val batchAccountNameEncoder: Encoder[BatchAccountName] = Encoder.encodeString.contramap(_.value.toString)
  implicit val relayNamespaceEncoder: Encoder[RelayNamespace] = Encoder.encodeString.contramap(_.value.toString)
  implicit val storageAccountNameEncoder: Encoder[StorageAccountName] = Encoder.encodeString.contramap(_.value.toString)
  implicit val vnetNameEncoder: Encoder[NetworkName] = Encoder.encodeString.contramap(_.value.toString)
  implicit val subnetworkNameEncoder: Encoder[SubnetworkName] = Encoder.encodeString.contramap(_.value.toString)

  implicit val landingZoneResourceEncoder: Encoder[LandingZoneResources] = Encoder.forProduct7(
    "clusterName",
    "batchAccountName",
    "relayNamespace",
    "storageAccountName",
    "vnetName",
    "batchNodesSubnetName",
    "aksSubnetName"
  )(x => (x.clusterName,
          x.batchAccountName,
          x.relayNamespace,
          x.storageAccountName,
          x.vnetName,
          x.batchNodesSubnetName,
          x.aksSubnetName))
}

