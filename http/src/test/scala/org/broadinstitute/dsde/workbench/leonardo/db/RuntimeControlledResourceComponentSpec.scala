package org.broadinstitute.dsde.workbench.leonardo
package db

import com.azure.resourcemanager.compute.models.VirtualMachineSizeTypes
import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.scalatest.flatspec.AnyFlatSpecLike

import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global

class RuntimeControlledResourceComponentSpec extends AnyFlatSpecLike with TestComponent {

  it should "save controlled resources for a runtime" in isolatedDbTest {
    val res = for {
      disk <- makePersistentDisk().copy(status = DiskStatus.Ready).save()
      azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                     Some(disk.id),
                                                     None
      )
      runtime = makeCluster(1)
        .copy(
          cloudContext = CloudContext.Azure(azureCloudContext)
        )
        .saveWithRuntimeConfig(azureRuntimeConfig)

      _ <- controlledResourceQuery
        .save(runtime.id, WsmControlledResourceId(UUID.randomUUID()), WsmResourceType.AzureDatabase)
        .transaction
      _ <- controlledResourceQuery
        .save(runtime.id, WsmControlledResourceId(UUID.randomUUID()), WsmResourceType.AzureStorageContainer)
        .transaction
      controlledResources <- controlledResourceQuery.getAllForRuntime(runtime.id).transaction
      controlledStorageContainerResource <- controlledResourceQuery
        .getWsmRecordForRuntime(runtime.id, WsmResourceType.AzureStorageContainer)
        .transaction
    } yield {
      controlledResources.length shouldBe 2
      controlledResources.map(_.resourceType) should contain(WsmResourceType.AzureDatabase)
      controlledStorageContainerResource.isDefined shouldBe true
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "not allow controlled resources of the same type for a runtime" in isolatedDbTest {
    val res = for {
      disk <- makePersistentDisk().copy(status = DiskStatus.Ready).save()
      azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                     Some(disk.id),
                                                     None
      )
      runtime = makeCluster(1)
        .copy(
          cloudContext = CloudContext.Azure(azureCloudContext)
        )
        .saveWithRuntimeConfig(azureRuntimeConfig)

      _ <- controlledResourceQuery
        .save(runtime.id, WsmControlledResourceId(UUID.randomUUID()), WsmResourceType.AzureDatabase)
        .transaction
      _ <- controlledResourceQuery
        .save(runtime.id, WsmControlledResourceId(UUID.randomUUID()), WsmResourceType.AzureDatabase)
        .transaction
    } yield ()

    the[Exception] thrownBy {
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "update runtime" in isolatedDbTest {
    val resourceId = WsmControlledResourceId(UUID.randomUUID())
    val res = for {
      disk <- makePersistentDisk().copy(status = DiskStatus.Ready).save()
      azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                     Some(disk.id),
                                                     None
      )
      runtime1 = makeCluster(1)
        .copy(
          cloudContext = CloudContext.Azure(azureCloudContext)
        )
        .saveWithRuntimeConfig(azureRuntimeConfig)

      runtime2 = makeCluster(2)
        .copy(
          cloudContext = CloudContext.Azure(azureCloudContext)
        )
        .saveWithRuntimeConfig(azureRuntimeConfig)

      _ <- controlledResourceQuery
        .save(runtime1.id, resourceId, WsmResourceType.AzureDisk)
        .transaction

      _ <- controlledResourceQuery.updateRuntime(resourceId, WsmResourceType.AzureDisk, runtime2.id).transaction

      controlledResources <- controlledResourceQuery.getAllForRuntime(runtime2.id).transaction
      controlledStorageContainerResource <- controlledResourceQuery
        .getWsmRecordForRuntime(runtime2.id, WsmResourceType.AzureDisk)
        .transaction
    } yield {
      controlledResources.length shouldBe 1
      controlledResources.map(_.resourceType) should contain(WsmResourceType.AzureDisk)
      controlledStorageContainerResource.isDefined shouldBe true
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }
}
