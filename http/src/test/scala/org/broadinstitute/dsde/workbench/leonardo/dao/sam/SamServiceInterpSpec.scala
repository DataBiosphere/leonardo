package org.broadinstitute.dsde.workbench.leonardo.dao.sam

import akka.http.scaladsl.model.StatusCodes
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.client.sam.ApiException
import org.broadinstitute.dsde.workbench.client.sam.api.{AzureApi, GoogleApi, ResourcesApi, UsersApi}
import org.broadinstitute.dsde.workbench.client.sam.model.{FullyQualifiedResourceId, GetOrCreateManagedIdentityRequest}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.auth.CloudAuthTokenProvider
import org.broadinstitute.dsde.workbench.leonardo.{LeonardoTestSuite, SamResourceType}
import org.http4s.headers.Authorization
import org.http4s.{AuthScheme, Credentials}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{any, anyList}
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID

class SamServiceInterpSpec extends AnyFunSpecLike with LeonardoTestSuite with BeforeAndAfterAll with MockitoSugar {
  describe("SamServiceInterp") {

    describe("getPetServiceAccount") {
      it("should retrieve a pet service account from Sam") {
        // test
        val result = newSamService().getPetServiceAccount(userInfo, project).unsafeRunSync()

        // assert
        result shouldBe serviceAccountEmail
      }

      it("should fail with a SamException") {
        // setup
        val googleApi = mock[GoogleApi]
        when(googleApi.getPetServiceAccount(project.value)).thenThrow(new ApiException(500, "sam error"))

        // test
        val result =
          the[SamException] thrownBy newSamService(googleApi = googleApi)
            .getPetServiceAccount(userInfo, project)
            .unsafeRunSync()

        // assert
        result.getMessage should include("sam error")
        result.statusCode shouldBe StatusCodes.InternalServerError
      }
    }

    describe("getPetManagedIdentity") {
      it("should retrieve a pet managed identity from Sam") {
        // test
        val result = newSamService().getPetManagedIdentity(userInfo, azureCloudContext).unsafeRunSync()

        // assert
        result shouldBe managedIdentityEmail
      }

      it("should fail with a SamException") {
        // setup
        val azureApi = mock[AzureApi]
        when(azureApi.getPetManagedIdentity(any)).thenThrow(new ApiException(400, "bad request"))

        // test
        val result =
          the[SamException] thrownBy newSamService(azureApi = azureApi)
            .getPetManagedIdentity(userInfo, azureCloudContext)
            .unsafeRunSync()

        // assert
        result.getMessage should include("bad request")
        result.statusCode shouldBe StatusCodes.BadRequest
      }
    }

    describe("getProxyGroup") {
      it("should retrieve a proxy group from Sam") {
        // test
        val result = newSamService().getProxyGroup(userEmail).unsafeRunSync()

        // assert
        result shouldBe proxyGroupEmail
      }

      it("should fail with a SamException") {
        // setup
        val googleApi = mock[GoogleApi]
        when(googleApi.getProxyGroup(userEmail.value)).thenThrow(new ApiException(500, "error"))

        // test
        val result =
          the[SamException] thrownBy newSamService(googleApi = googleApi).getProxyGroup(userEmail).unsafeRunSync()

        // assert
        result.getMessage should include("error")
        result.statusCode shouldBe StatusCodes.InternalServerError
      }
    }

    describe("getPetServiceAccountToken") {
      it("should successfully request a pet token") {
        // test
        val result =
          newSamService().getPetServiceAccountToken(userEmail, project).unsafeRunSync()

        // assert
        result shouldBe tokenValue
      }

      it("should fail with a SamException") {
        // setup
        val googleApi = mock[GoogleApi]
        when(
          googleApi.getUserPetServiceAccountToken(ArgumentMatchers.eq(project.value),
                                                  ArgumentMatchers.eq(userEmail.value),
                                                  anyList()
          )
        )
          .thenThrow(new ApiException(404, "not found"))

        // test
        val result = the[SamException] thrownBy newSamService(googleApi = googleApi)
          .getPetServiceAccountToken(userEmail, project)
          .unsafeRunSync()

        // assert
        result.getMessage should include("not found")
        result.statusCode shouldBe StatusCodes.NotFound
      }
    }

    describe("lookupWorkspaceParentForGoogleProject") {
      it("should successfully lookup a google project's parent workspace") {
        // test
        val result =
          newSamService().lookupWorkspaceParentForGoogleProject(userInfo, project).unsafeRunSync()

        // assert
        result shouldBe Some(workspaceId)
      }

      it("should handle Sam errors") {
        // setup
        val resourcesApi = mock[ResourcesApi]
        when(setUpMockResourceApi.getResourceParent(SamResourceType.Project.asString, project.value))
          .thenThrow(new RuntimeException("error!"))

        // test
        val result =
          newSamService(resourcesApi)
            .lookupWorkspaceParentForGoogleProject(userInfo, project)
            .unsafeRunSync()

        // assert
        result shouldBe None
      }

      it("should handle no workspace returned") {
        // setup
        val resourcesApi = mock[ResourcesApi]
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
        val resourcesApi = mock[ResourcesApi]
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
        val resourcesApi = mock[ResourcesApi]
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

  def newSamService(resourcesApi: ResourcesApi = setUpMockResourceApi,
                    usersApi: UsersApi = setUpMockUserApi,
                    googleApi: GoogleApi = setUpMockGoogleApi,
                    azureApi: AzureApi = setUpMockAzureApi
  ): SamService[IO] = new SamServiceInterp(
    setUpMockSamApiClientProvider(resourcesApi, usersApi, googleApi, azureApi),
    setUpMockCloudAuthTokenProvider
  )

  def setUpMockCloudAuthTokenProvider: CloudAuthTokenProvider[IO] = {
    val provider = mock[CloudAuthTokenProvider[IO]]
    when(provider.getAuthToken).thenReturn(IO.pure(Authorization(Credentials.Token(AuthScheme.Bearer, "some-token"))))
    provider
  }

  def setUpMockSamApiClientProvider(resourcesApi: ResourcesApi,
                                    usersApi: UsersApi,
                                    googleApi: GoogleApi,
                                    azureApi: AzureApi
  ): SamApiClientProvider[IO] = {
    val provider = mock[SamApiClientProvider[IO]]
    when(provider.resourcesApi(any)(any)).thenReturn(IO.pure(resourcesApi))
    when(provider.usersApi(any)(any)).thenReturn(IO.pure(usersApi))
    when(provider.googleApi(any)(any)).thenReturn(IO.pure(googleApi))
    when(provider.azureApi(any)(any)).thenReturn(IO.pure(azureApi))
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

  def setUpMockUserApi: UsersApi = {
    val usersApi = mock[UsersApi]
    usersApi
  }

  def setUpMockGoogleApi: GoogleApi = {
    val googleApi = mock[GoogleApi]
    when(googleApi.getPetServiceAccount(project.value)).thenReturn(serviceAccountEmail.value)
    when(googleApi.getProxyGroup(userEmail.value)).thenReturn(proxyGroupEmail.value)
    when(
      googleApi.getUserPetServiceAccountToken(ArgumentMatchers.eq(project.value),
                                              ArgumentMatchers.eq(userEmail.value),
                                              anyList()
      )
    )
      .thenReturn(tokenValue)
    googleApi
  }

  def setUpMockAzureApi: AzureApi = {
    val azureApi = mock[AzureApi]
    when(
      azureApi.getPetManagedIdentity(
        new GetOrCreateManagedIdentityRequest()
          .tenantId(azureCloudContext.tenantId.value)
          .subscriptionId(azureCloudContext.subscriptionId.value)
          .managedResourceGroupName(azureCloudContext.managedResourceGroupName.value)
      )
    ).thenReturn(managedIdentityEmail.value)
    azureApi
  }

}
