package org.broadinstitute.dsde.workbench.leonardo.dao.sam

import akka.http.scaladsl.model.StatusCodes
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.client.sam.ApiException
import org.broadinstitute.dsde.workbench.client.sam.api.{AzureApi, GoogleApi, ResourcesApi, UsersApi}
import org.broadinstitute.dsde.workbench.client.sam.model._
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.auth.CloudAuthTokenProvider
import org.broadinstitute.dsde.workbench.leonardo.model.LeoInternalServerError
import org.broadinstitute.dsde.workbench.leonardo.{
  LeonardoTestSuite,
  RuntimeAction,
  SamPolicyData,
  SamResourceType,
  SamRole
}
import org.http4s.headers.Authorization
import org.http4s.{AuthScheme, Credentials}
import org.mockito.ArgumentMatchers.{any, anyList, anyString}
import org.mockito.Mockito.{doNothing, doThrow, verify, when}
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import scala.jdk.CollectionConverters._

class SamServiceInterpSpec extends AnyFunSpecLike with LeonardoTestSuite with BeforeAndAfterAll with MockitoSugar {
  describe("SamServiceInterp") {

    describe("getPetServiceAccount") {
      it("should retrieve a pet service account from Sam") {
        // test
        val result = newSamService().getPetServiceAccount(tokenValue, project).unsafeRunSync()

        // assert
        result shouldBe serviceAccountEmail
      }

      it("should fail with a SamException") {
        // setup
        val googleApi = mock[GoogleApi]
        when(googleApi.getPetServiceAccount(project.value)).thenThrow(new ApiException(400, "sam error"))

        // test
        val result =
          the[SamException] thrownBy newSamService(googleApi = googleApi)
            .getPetServiceAccount(tokenValue, project)
            .unsafeRunSync()

        // assert
        result.getMessage should include("sam error")
        result.statusCode shouldBe StatusCodes.BadRequest
      }
    }

    describe("getPetManagedIdentity") {
      it("should retrieve a pet managed identity from Sam") {
        // test
        val result = newSamService().getPetManagedIdentity(tokenValue, azureCloudContext).unsafeRunSync()

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
            .getPetManagedIdentity(tokenValue, azureCloudContext)
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
        when(googleApi.getProxyGroup(userEmail.value)).thenThrow(new ApiException(400, "bad request"))

        // test
        val result =
          the[SamException] thrownBy newSamService(googleApi = googleApi).getProxyGroup(userEmail).unsafeRunSync()

        // assert
        result.getMessage should include("bad request")
        result.statusCode shouldBe StatusCodes.BadRequest
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
          newSamService().lookupWorkspaceParentForGoogleProject(tokenValue, project).unsafeRunSync()

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
            .lookupWorkspaceParentForGoogleProject(tokenValue, project)
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
          newSamService(resourcesApi).lookupWorkspaceParentForGoogleProject(tokenValue, project).unsafeRunSync()

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
          newSamService(resourcesApi).lookupWorkspaceParentForGoogleProject(tokenValue, project).unsafeRunSync()

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
          newSamService(resourcesApi).lookupWorkspaceParentForGoogleProject(tokenValue, project).unsafeRunSync()

        // assert
        result shouldBe None
      }
    }

    describe("checkAuthorized") {
      it("should successfully check authorization") {
        // test
        newSamService()
          .checkAuthorized(tokenValue, runtimeSamResource, RuntimeAction.GetRuntimeStatus.asString)
          .unsafeRunSync()
      }

      it("should fail with SamException if unauthorized") {
        // setup
        val resourcesApi = mock[ResourcesApi]
        when(
          resourcesApi.resourcePermissionV2(runtimeSamResource.resourceType.asString,
                                            runtimeSamResource.resourceId,
                                            RuntimeAction.GetRuntimeStatus.asString
          )
        ).thenReturn(false)

        // test
        val result = the[SamException] thrownBy newSamService(resourcesApi = resourcesApi)
          .checkAuthorized(tokenValue, runtimeSamResource, RuntimeAction.GetRuntimeStatus.asString)
          .unsafeRunSync()

        // assert
        result.getMessage should include("is not authorized to perform action")
        result.getMessage should include(userEmail.value)
        result.statusCode shouldBe StatusCodes.Forbidden
      }

      it("should fail with SamException on Sam errors") {
        // setup
        val resourcesApi = mock[ResourcesApi]
        when(resourcesApi.resourcePermissionV2(anyString, anyString, anyString))
          .thenThrow(new ApiException(400, "sam error"))

        // test
        val result = the[SamException] thrownBy newSamService(resourcesApi = resourcesApi)
          .checkAuthorized(tokenValue, runtimeSamResource, RuntimeAction.GetRuntimeStatus.asString)
          .unsafeRunSync()

        // assert
        result.getMessage should include("Error checking resource permission in Sam")
        result.statusCode shouldBe StatusCodes.BadRequest
      }
    }

    describe("listResources") {
      it("should successfully list resources") {
        // test
        val result = newSamService().listResources(tokenValue, runtimeSamResource.resourceType).unsafeRunSync()

        // assert
        result shouldBe List(runtimeSamResource.resourceId)
      }

      it("should handle Sam errors") {
        // setup
        val resourcesApi = mock[ResourcesApi]
        when(resourcesApi.listResourcesAndPoliciesV2(anyString)).thenThrow(new ApiException(400, "error"))

        // test
        val result = the[SamException] thrownBy newSamService(resourcesApi = resourcesApi)
          .listResources(tokenValue, runtimeSamResource.resourceType)
          .unsafeRunSync()

        // assert
        result.getMessage should include("Error listing resources from Sam")
        result.statusCode shouldBe StatusCodes.BadRequest
      }
    }

    describe("createResource") {
      it("should create a Sam resource with a google project parent") {
        // setup
        val resourcesApi = mock[ResourcesApi]
        doNothing()
          .when(resourcesApi)
          .createResourceV2(ArgumentMatchers.eq(runtimeSamResource.resourceType.asString), any)

        // test
        newSamService(resourcesApi = resourcesApi)
          .createResource(tokenValue, runtimeSamResource, Some(project), None, Map.empty)
          .unsafeRunSync()

        // assert
        val captor = ArgumentCaptor.forClass(classOf[CreateResourceRequestV2])
        verify(resourcesApi).createResourceV2(ArgumentMatchers.eq(runtimeSamResource.resourceType.asString),
                                              captor.capture()
        )
        assert(captor.getValue.isInstanceOf[CreateResourceRequestV2])
        val createResourceRequest = captor.getValue.asInstanceOf[CreateResourceRequestV2]
        createResourceRequest.getResourceId shouldBe runtimeSamResource.resourceId
        createResourceRequest.getParent shouldBe new FullyQualifiedResourceId()
          .resourceTypeName(SamResourceType.Project.asString)
          .resourceId(project.value)
        createResourceRequest.getAuthDomain shouldBe List.empty.asJava
        createResourceRequest.getPolicies shouldBe Map.empty.asJava
      }

      it("should create a Sam resource with a workspace parent") {
        // setup
        val resourcesApi = mock[ResourcesApi]
        doNothing()
          .when(resourcesApi)
          .createResourceV2(ArgumentMatchers.eq(runtimeSamResource.resourceType.asString), any)

        // test
        newSamService(resourcesApi = resourcesApi)
          .createResource(tokenValue, runtimeSamResource, None, Some(workspaceId), Map.empty)
          .unsafeRunSync()

        // assert
        val captor = ArgumentCaptor.forClass(classOf[CreateResourceRequestV2])
        verify(resourcesApi).createResourceV2(ArgumentMatchers.eq(runtimeSamResource.resourceType.asString),
                                              captor.capture()
        )
        assert(captor.getValue.isInstanceOf[CreateResourceRequestV2])
        val createResourceRequest = captor.getValue.asInstanceOf[CreateResourceRequestV2]
        createResourceRequest.getResourceId shouldBe runtimeSamResource.resourceId
        createResourceRequest.getParent shouldBe new FullyQualifiedResourceId()
          .resourceTypeName(SamResourceType.Workspace.asString)
          .resourceId(workspaceId.value.toString)
        createResourceRequest.getAuthDomain shouldBe List.empty.asJava
        createResourceRequest.getPolicies shouldBe Map.empty.asJava
      }

      it("should not allow resource creation with no parent") {
        // test
        val result = the[LeoInternalServerError] thrownBy newSamService()
          .createResource(tokenValue, runtimeSamResource, None, None, Map.empty)
          .unsafeRunSync()

        // assert
        result.msg should include("google project or workspace parent is required")
      }

      it("should not allow specifying a google project and workspace parent") {
        // test
        val result = the[LeoInternalServerError] thrownBy newSamService()
          .createResource(tokenValue, runtimeSamResource, Some(project), Some(workspaceId), Map.empty)
          .unsafeRunSync()

        // assert
        result.msg should include("google project or workspace parent is required")
      }

      it("should create a Sam resource with a policy") {
        // setup
        val resourcesApi = mock[ResourcesApi]
        doNothing()
          .when(resourcesApi)
          .createResourceV2(ArgumentMatchers.eq(runtimeSamResource.resourceType.asString), any)

        // test
        newSamService(resourcesApi = resourcesApi)
          .createResource(tokenValue,
                          runtimeSamResource,
                          None,
                          Some(workspaceId),
                          Map("aPolicy" -> SamPolicyData(List(userEmail), List(SamRole.Creator)))
          )
          .unsafeRunSync()

        // assert
        val captor = ArgumentCaptor.forClass(classOf[CreateResourceRequestV2])
        verify(resourcesApi).createResourceV2(ArgumentMatchers.eq(runtimeSamResource.resourceType.asString),
                                              captor.capture()
        )
        assert(captor.getValue.isInstanceOf[CreateResourceRequestV2])
        val createResourceRequest = captor.getValue.asInstanceOf[CreateResourceRequestV2]
        createResourceRequest.getResourceId shouldBe runtimeSamResource.resourceId
        createResourceRequest.getParent shouldBe new FullyQualifiedResourceId()
          .resourceTypeName(SamResourceType.Workspace.asString)
          .resourceId(workspaceId.value.toString)
        createResourceRequest.getAuthDomain shouldBe List.empty.asJava
        val policies = createResourceRequest.getPolicies.asScala
        policies should have size 1
        policies should contain key "aPolicy"
        policies("aPolicy") shouldBe new AccessPolicyMembershipRequest()
          .addMemberEmailsItem(userEmail.value)
          .addRolesItem("creator")
      }

      it("should handle Sam errors") {
        // setup
        val resourcesApi = mock[ResourcesApi]
        doThrow(new ApiException(400, "bad request")).when(resourcesApi).createResourceV2(anyString, any)

        // test
        val result = the[SamException] thrownBy newSamService(resourcesApi = resourcesApi)
          .createResource(tokenValue, runtimeSamResource, Some(project), None, Map.empty)
          .unsafeRunSync()

        result.getMessage should include("bad request")
        result.statusCode shouldBe StatusCodes.BadRequest
      }
    }

    describe("deleteResource") {
      it("should delete a Sam resource") {
        // setup
        val resourcesApi = mock[ResourcesApi]
        doNothing()
          .when(resourcesApi)
          .deleteResourceV2(runtimeSamResource.resourceType.asString, runtimeSamResource.resourceId)

        // test
        newSamService(resourcesApi = resourcesApi).deleteResource(tokenValue, runtimeSamResource).unsafeRunSync()

        // assert
        verify(resourcesApi).deleteResourceV2(runtimeSamResource.resourceType.asString, runtimeSamResource.resourceId)
      }

      it("should handle Sam errors") {
        // setup
        val resourcesApi = mock[ResourcesApi]
        doThrow(new ApiException(400, "bad request")).when(resourcesApi).deleteResourceV2(anyString, anyString)

        // test
        val result = the[SamException] thrownBy newSamService(resourcesApi = resourcesApi)
          .deleteResource(tokenValue, runtimeSamResource)
          .unsafeRunSync()

        // assert
        result.getMessage should include("bad request")
        result.statusCode shouldBe StatusCodes.BadRequest
      }
    }

    describe("getUserEmail") {
      it("should get a user email") {
        // test
        val result = newSamService().getUserEmail(tokenValue).unsafeRunSync()

        // assert
        result shouldBe userEmail
      }

      it("should handle Sam errors") {
        // setup
        val usersApi = mock[UsersApi]
        when(usersApi.getUserStatusInfo).thenThrow(new ApiException(400, "bad request"))

        // test
        val result =
          the[SamException] thrownBy newSamService(usersApi = usersApi).getUserEmail(tokenValue).unsafeRunSync()

        // assert
        result.getMessage should include("bad request")
        result.statusCode shouldBe StatusCodes.BadRequest
      }
    }
  }

  def newSamService(resourcesApi: ResourcesApi = setUpMockResourceApi,
                    usersApi: UsersApi = setUpMockUsersApi,
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
    when(
      resourcesApi.resourcePermissionV2(runtimeSamResource.resourceType.asString,
                                        runtimeSamResource.resourceId,
                                        RuntimeAction.GetRuntimeStatus.asString
      )
    ).thenReturn(true)
    when(resourcesApi.listResourcesAndPoliciesV2(runtimeSamResource.resourceType.asString))
      .thenReturn(List(new UserResourcesResponse().resourceId(runtimeSamResource.resourceId)).asJava)
    resourcesApi
  }

  def setUpMockUsersApi: UsersApi = {
    val usersApi = mock[UsersApi]
    when(usersApi.getUserStatusInfo).thenReturn(new UserStatusInfo().userEmail(userEmail.value))
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
