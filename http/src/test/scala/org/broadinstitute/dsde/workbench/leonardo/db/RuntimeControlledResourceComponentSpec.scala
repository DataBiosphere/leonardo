package org.broadinstitute.dsde.workbench.leonardo
package http
package db

import java.util.UUID

import com.azure.resourcemanager.compute.models.VirtualMachineSizeTypes
import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.db.{controlledResourceQuery, TestComponent, WsmResourceType}
import org.scalatest.flatspec.AnyFlatSpecLike
import scala.concurrent.ExecutionContext.Implicits.global

class RuntimeControlledResourceComponentSpec extends AnyFlatSpecLike with TestComponent {

  it should "save controlled resources for a runtime" in isolatedDbTest {
    val res = for {
      disk <- makePersistentDisk().copy(status = DiskStatus.Ready).save()
      azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                     disk.id,
                                                     azureRegion
      )
      runtime = makeCluster(1)
        .copy(
          cloudContext = CloudContext.Azure(azureCloudContext)
        )
        .saveWithRuntimeConfig(azureRuntimeConfig)

      _ <- controlledResourceQuery
        .save(runtime.id, WsmControlledResourceId(UUID.randomUUID()), WsmResourceType.AzureNetwork)
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
      controlledResources.map(_.resourceType) should contain(WsmResourceType.AzureNetwork)
      controlledStorageContainerResource.isDefined shouldBe true
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "not allow controlled resources of the same type for a runtime" in isolatedDbTest {
    val res = for {
      disk <- makePersistentDisk().copy(status = DiskStatus.Ready).save()
      azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                     disk.id,
                                                     azureRegion
      )
      runtime = makeCluster(1)
        .copy(
          cloudContext = CloudContext.Azure(azureCloudContext)
        )
        .saveWithRuntimeConfig(azureRuntimeConfig)

      _ <- controlledResourceQuery
        .save(runtime.id, WsmControlledResourceId(UUID.randomUUID()), WsmResourceType.AzureNetwork)
        .transaction
      _ <- controlledResourceQuery
        .save(runtime.id, WsmControlledResourceId(UUID.randomUUID()), WsmResourceType.AzureNetwork)
        .transaction
    } yield ()

    the[Exception] thrownBy {
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }
}
