package org.broadinstitute.dsde.workbench.leonardo

import com.azure.resourcemanager.compute.models.VirtualMachineSizeTypes

final case class CreateAzureRuntimeRequest(labels: LabelMap,
                                           machineSize: VirtualMachineSizeTypes,
                                           customEnvironmentVariables: Map[String, String],
                                           azureDiskConfig: CreateAzureDiskRequest,
                                           autoPauseThreshold: Option[Int]
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
}
