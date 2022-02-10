package org.broadinstitute.dsde.workbench.leonardo

import com.azure.core.management.Region
import com.azure.resourcemanager.compute.models.VirtualMachineSizeTypes

final case class CreateAzureRuntimeRequest(labels: LabelMap,
                                           region: Region,
                                           machineSize: VirtualMachineSizeTypes,
                                           imageUri: Option[AzureImageUri],
                                           customEnvironmentVariables: Map[String, String],
                                           azureDiskConfig: CreateAzureDiskRequest)

final case class CreateAzureDiskRequest(labels: LabelMap,
                                        name: AzureDiskName,
                                        size: Option[DiskSize],
                                        diskType: Option[DiskType])

//TODO: implement
final case class UpdateAzureRuntimeRequest(machineSize: VirtualMachineSizeTypes)

//TODO: delete this case class when current pd.diskName is no longer coupled to google2 diskService
final case class AzureDiskName(value: String) extends AnyVal

final case class AzureImageUri(value: String) extends AnyVal
