package org.broadinstitute.dsde.workbench.leonardo.dao

import bio.terra.workspace.api.ControlledAzureResourceApi
import bio.terra.workspace.model.{
  AzureDatabaseResource,
  AzureDiskResource,
  AzureKubernetesNamespaceResource,
  AzureManagedIdentityResource,
  AzureVmResource,
  ResourceMetadata,
  ResourceType,
  State
}
import cats.effect.IO
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.{tokenValue, workspaceId, workspaceId2, wsmResourceId}
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.db.WsmResourceType
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, LeonardoTestSuite}
import org.http4s._
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class WsmApiClientProviderSpec extends AnyFlatSpec with LeonardoTestSuite with BeforeAndAfterAll with MockitoSugar {

  def newWsmProvider() =
    new HttpWsmClientProvider[IO](baseWorkspaceManagerUrl = Uri.unsafeFromString("test")) {
      override def getControlledAzureResourceApi(token: String)(implicit
        ev: Ask[IO, AppContext]
      ): IO[ControlledAzureResourceApi] = IO.pure(setUpMockResourceApi)
    }

  val wsmProvider = newWsmProvider()

  it should "return disk metadata" in {
    val res = for {
      md <- wsmProvider.getDisk(tokenValue, workspaceId, wsmResourceId)
    } yield md.get.getResourceType shouldBe ResourceType.AZURE_DISK
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }
  it should "return vm metadata" in {
    val res = for {
      md <- wsmProvider.getVm(tokenValue, workspaceId, wsmResourceId)
    } yield md.get.getResourceType shouldBe ResourceType.AZURE_VM
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }
  it should "return database metadata" in {
    val res = for {
      md <- wsmProvider.getDatabase(tokenValue, workspaceId, wsmResourceId)
    } yield md.get.getResourceType shouldBe ResourceType.AZURE_DATABASE
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }
  it should "return namespace metadata" in {
    val res = for {
      md <- wsmProvider.getNamespace(tokenValue, workspaceId, wsmResourceId)
    } yield md.get.getResourceType shouldBe ResourceType.AZURE_KUBERNETES_NAMESPACE
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }
  it should "return managed identity metadata" in {
    val res = for {
      md <- wsmProvider.getIdentity(tokenValue, workspaceId, wsmResourceId)
    } yield md.get.getResourceType shouldBe ResourceType.AZURE_MANAGED_IDENTITY
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  for (
    resourceType <- List(
      WsmResourceType.AzureDisk,
      WsmResourceType.AzureKubernetesNamespace,
      WsmResourceType.AzureManagedIdentity,
      WsmResourceType.AzureDatabase,
      WsmResourceType.AzureVm
    )
  )
    it should s"return a WsmState for a ${resourceType.toString} state" in {
      val res = for {
        md <- wsmProvider.getWsmState(tokenValue, workspaceId, wsmResourceId, resourceType)
      } yield md.value shouldBe "CREATING"
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }

  it should "a NONE state if an Azure resource doesn't exist" in {
    val res = for {
      md <- wsmProvider.getWsmState(tokenValue, workspaceId2, wsmResourceId, WsmResourceType.AzureDisk)
    } yield md.value shouldBe "NONE"
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  private def setUpMockResourceApi: ControlledAzureResourceApi = {
    val api = mock[ControlledAzureResourceApi]
    when {
      api.getAzureDisk(workspaceId2.value, wsmResourceId.value)
    } thenAnswer { _ =>
      throw new Exception("Resource not found")
    }
    when {
      api.getAzureDisk(workspaceId.value, wsmResourceId.value)
    } thenAnswer { _ =>
      new AzureDiskResource().metadata(
        new ResourceMetadata()
          .resourceId(wsmResourceId.value)
          .resourceType(ResourceType.AZURE_DISK)
          .state(State.CREATING)
      )
    }
    when {
      api.getAzureVm(workspaceId.value, wsmResourceId.value)
    } thenAnswer { _ =>
      new AzureVmResource().metadata(
        new ResourceMetadata()
          .resourceId(wsmResourceId.value)
          .resourceType(ResourceType.AZURE_VM)
          .state(State.CREATING)
      )
    }
    when {
      api.getAzureDatabase(workspaceId.value, wsmResourceId.value)
    } thenAnswer { _ =>
      new AzureDatabaseResource().metadata(
        new ResourceMetadata()
          .resourceId(wsmResourceId.value)
          .resourceType(ResourceType.AZURE_DATABASE)
          .state(State.CREATING)
      )
    }
    when {
      api.getAzureKubernetesNamespace(workspaceId.value, wsmResourceId.value)
    } thenAnswer { _ =>
      new AzureKubernetesNamespaceResource().metadata(
        new ResourceMetadata()
          .resourceId(wsmResourceId.value)
          .resourceType(ResourceType.AZURE_KUBERNETES_NAMESPACE)
          .state(State.CREATING)
      )
    }
    when {
      api.getAzureManagedIdentity(workspaceId.value, wsmResourceId.value)
    } thenAnswer { _ =>
      new AzureManagedIdentityResource().metadata(
        new ResourceMetadata()
          .resourceId(wsmResourceId.value)
          .resourceType(ResourceType.AZURE_MANAGED_IDENTITY)
          .state(State.CREATING)
      )
    }
    api
  }
}
