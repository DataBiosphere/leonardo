package org.broadinstitute.dsde.workbench.leonardo.dao.sam

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi
import org.broadinstitute.dsde.workbench.client.sam.model.FullyQualifiedResourceId
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.{project, userInfo, workspaceId}
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.{LeonardoTestSuite, SamResourceType}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID

class SamServiceInterpSpec extends AnyFunSpecLike with LeonardoTestSuite with BeforeAndAfterAll with MockitoSugar {
  describe("SamServiceInterp") {
    describe("lookupWorkspaceParentForGoogleProject") {
      it("should successfully lookup a google project's parent workspace") {
        // test
        val result =
          newSamService(setUpMockResourceApi).lookupWorkspaceParentForGoogleProject(userInfo, project).unsafeRunSync()

        // assert
        result shouldBe Some(workspaceId)
      }

      it("should handle Sam errors") {
        // setup
        val resourcesApi = setUpMockResourceApi
        when(resourcesApi.getResourceParent(SamResourceType.Project.asString, project.value))
          .thenThrow(new RuntimeException("error!"))

        // test
        val result =
          newSamService(resourcesApi).lookupWorkspaceParentForGoogleProject(userInfo, project).unsafeRunSync()

        // assert
        result shouldBe None
      }

      it("should handle no workspace returned") {
        // setup
        val resourcesApi = setUpMockResourceApi
        when(resourcesApi.getResourceParent(SamResourceType.Project.asString, project.value))
          .thenReturn(null)

        // test
        val result =
          newSamService(resourcesApi).lookupWorkspaceParentForGoogleProject(userInfo, project).unsafeRunSync()

        // assert
        result shouldBe None
      }

      it("should handle an invalid id") {
        // setup
        val resourcesApi = setUpMockResourceApi
        when(resourcesApi.getResourceParent(SamResourceType.Project.asString, project.value))
          .thenReturn(
            new FullyQualifiedResourceId()
              .resourceTypeName(SamResourceType.Workspace.asString)
              .resourceId("not-a-uuid")
          )

        // test
        val result =
          newSamService(resourcesApi).lookupWorkspaceParentForGoogleProject(userInfo, project).unsafeRunSync()

        // assert
        result shouldBe None
      }

      it("should handle an unexpected parent type") {
        // setup
        val resourcesApi = setUpMockResourceApi
        when(resourcesApi.getResourceParent(SamResourceType.Project.asString, project.value))
          .thenReturn(
            new FullyQualifiedResourceId()
              .resourceTypeName(SamResourceType.PersistentDisk.asString)
              .resourceId(UUID.randomUUID.toString)
          )

        // test
        val result =
          newSamService(resourcesApi).lookupWorkspaceParentForGoogleProject(userInfo, project).unsafeRunSync()

        // assert
        result shouldBe None
      }
    }
  }

  def newSamService(resourcesApi: ResourcesApi): SamService[IO] = new SamServiceInterp(
    setUpMockSamApiClientProvider(resourcesApi)
  )

  def setUpMockSamApiClientProvider(resourcesApi: ResourcesApi): SamApiClientProvider[IO] = {
    val provider = mock[SamApiClientProvider[IO]]
    when(provider.resourcesApi(any)(any)).thenReturn(IO.pure(resourcesApi))
    provider
  }

  def setUpMockResourceApi: ResourcesApi = {
    val resourcesApi = mock[ResourcesApi]
    when(resourcesApi.getResourceParent(SamResourceType.Project.asString, project.value))
      .thenReturn(
        new FullyQualifiedResourceId()
          .resourceTypeName(SamResourceType.Workspace.asString)
          .resourceId(workspaceId.value.toString)
      )
    resourcesApi
  }

}
