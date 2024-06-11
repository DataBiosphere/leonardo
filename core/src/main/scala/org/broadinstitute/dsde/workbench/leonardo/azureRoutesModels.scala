package org.broadinstitute.dsde.workbench.leonardo

import bio.terra.workspace.model.AzureVmImage
import com.azure.resourcemanager.compute.models.VirtualMachineSizeTypes

final case class CreateAzureRuntimeRequest(labels: LabelMap,
                                           machineSize: VirtualMachineSizeTypes,
                                           customEnvironmentVariables: Map[String, String],
                                           azureDiskConfig: CreateAzureDiskRequest,
                                           autopauseThreshold: Option[Int]
)

final case class CreateAzureDiskRequest(labels: LabelMap,
                                        name: AzureDiskName,
                                        size: Option[DiskSize],
                                        diskType: Option[DiskType]
)

//TODO: implement
final case class UpdateAzureRuntimeRequest(machineSize: VirtualMachineSizeTypes)

//TODO: delete this case class when current pd.diskName is no longer coupled to google2 diskService
final case class AzureDiskName(value: String) extends AnyVal

final case class AzureImage(publisher: String, offer: String, sku: String, version: String) {
  def asString: String = s"${publisher}, ${offer}, ${sku}, ${version}"

  def toWsm(): AzureVmImage =
    new AzureVmImage()
      .offer(offer)
      .publisher(publisher)
      .sku(sku)
      .version(version)

}
