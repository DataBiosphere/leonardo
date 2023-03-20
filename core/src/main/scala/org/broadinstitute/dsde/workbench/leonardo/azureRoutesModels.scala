package org.broadinstitute.dsde.workbench.leonardo

import com.azure.resourcemanager.compute.models.VirtualMachineSizeTypes

final case class CreateAzureRuntimeRequest(labels: LabelMap,
                                           machineSize: VirtualMachineSizeTypes,
                                           customEnvironmentVariables: Map[String, String],
                                           azureDiskConfig: CreateAzureDiskRequest,
                                           autoPauseThreshold: Option[Int]
)

final case class CreateAzureDiskRequest(labels: LabelMap,
                                        name: DiskName,
                                        size: Option[DiskSize],
                                        diskType: Option[DiskType]
)

//TODO: implement
final case class UpdateAzureRuntimeRequest(machineSize: VirtualMachineSizeTypes)

final case class AzureImage(publisher: String, offer: String, sku: String, version: String) {
  def asString: String = s"${publisher}, ${offer}, ${sku}, ${version}"
}
