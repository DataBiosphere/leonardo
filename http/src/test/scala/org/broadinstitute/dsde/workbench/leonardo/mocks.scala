package org.broadinstitute.dsde.workbench.leonardo

import cats.data.NonEmptyList
import cats.effect.IO
import cats.mtl.Ask
import cats.syntax.all._
import com.google.api.gax.longrunning.OperationFuture
import com.google.auth.Credentials
import com.google.cloud.compute.v1.Operation
import com.google.cloud.storage.Blob
import com.google.cloud.storage.Storage.{BlobGetOption, BucketSourceOption}
import fs2.Stream
import io.circe.{Decoder, Encoder}
import io.kubernetes.client.openapi.models.{V1ObjectMeta, V1PersistentVolumeClaim}
import org.broadinstitute.dsde.workbench.RetryConfig
import org.broadinstitute.dsde.workbench.azure.AzureCloudContext
import org.broadinstitute.dsde.workbench.google2.GKEModels.KubernetesClusterId
import org.broadinstitute.dsde.workbench.google2.KubernetesModels.{KubernetesNamespace, KubernetesPodStatus, PodStatus}
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.PodName
import org.broadinstitute.dsde.workbench.google2.mock.BaseFakeGoogleStorage
import org.broadinstitute.dsde.workbench.google2.{GKEModels, GcsBlobName, GetMetadataResponse, KubernetesModels, PvName}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.{AppSamResourceId, WorkspaceResourceSamResourceId}
import org.broadinstitute.dsde.workbench.leonardo.dao.sam.SamService
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoAuthProvider, SamResource, SamResourceAction}
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.util2.messaging.{CloudSubscriber, ReceivedMessage}
import org.broadinstitute.dsp.Release

object FakeGoogleStorageService extends BaseFakeGoogleStorage {
  override def getObjectMetadata(bucketName: GcsBucketName,
                                 blobName: GcsBlobName,
                                 traceId: Option[TraceId],
                                 retryConfig: RetryConfig,
                                 blobGetOption: List[BlobGetOption]
  ): fs2.Stream[IO, GetMetadataResponse] =
    fs2.Stream.empty.covary[IO]

  override def getBlob(bucketName: GcsBucketName,
                       blobName: GcsBlobName,
                       credentials: scala.Option[Credentials],
                       traceId: Option[TraceId],
                       retryConfig: RetryConfig,
                       blobGetOption: List[BlobGetOption]
  ): fs2.Stream[IO, Blob] =
    bucketName match {
      case GcsBucketName("nonExistent") => fs2.Stream.empty
      case GcsBucketName("failure") =>
        super.createBlob(bucketName, blobName, Array.empty[Byte], "text/plain", Map("passed" -> "false")) >> super
          .getBlob(bucketName, blobName)
      case GcsBucketName("success") =>
        super.createBlob(bucketName, blobName, Array.empty[Byte], "text/plain", Map("passed" -> "true")) >> super
          .getBlob(bucketName, blobName)
      case GcsBucketName("staging_bucket") =>
        if (blobName.value == "failed_userstartupscript_output.txt")
          super.createBlob(bucketName, blobName, Array.empty[Byte], "text/plain", Map("passed" -> "false")) >> super
            .getBlob(bucketName, blobName)
        else
          super.createBlob(bucketName, blobName, Array.empty[Byte], "text/plain", Map("passed" -> "true")) >> super
            .getBlob(bucketName, blobName)
      case _ => super.getBlob(bucketName, blobName, None, traceId)
    }
}

object NoDeleteGoogleStorage extends BaseFakeGoogleStorage {
  override def deleteBucket(googleProject: GoogleProject,
                            bucketName: GcsBucketName,
                            isRecursive: Boolean,
                            bucketSourceOptions: List[BucketSourceOption],
                            traceId: Option[TraceId],
                            retryConfig: RetryConfig
  ): fs2.Stream[IO, Boolean] =
    throw new Exception("this shouldn't get called")
}

class BaseMockAuthProvider extends LeoAuthProvider[IO] {
  override def hasPermission[R, A](samResource: R, action: A, userInfo: UserInfo)(implicit
    sr: SamResourceAction[R, A],
    ev: Ask[IO, TraceId]
  ): IO[Boolean] = IO.pure(true)

  override def hasPermissionWithProjectFallback[R, A](
    samResource: R,
    action: A,
    projectAction: ProjectAction,
    userInfo: UserInfo,
    googleProject: GoogleProject
  )(implicit sr: SamResourceAction[R, A], ev: Ask[IO, TraceId]): IO[Boolean] = IO.pure(true)

  override def getActions[R, A](samResource: R, userInfo: UserInfo)(implicit
    sr: SamResourceAction[R, A],
    ev: Ask[IO, TraceId]
  ): IO[List[A]] = ???

  override def getActionsWithProjectFallback[R, A](samResource: R, googleProject: GoogleProject, userInfo: UserInfo)(
    implicit
    sr: SamResourceAction[R, A],
    ev: Ask[IO, TraceId]
  ): IO[(List[A], List[ProjectAction])] = ???

  override def listResourceIds[R <: SamResourceId](
    hasOwnerRole: Boolean,
    userInfo: UserInfo
  )(implicit
    resourceDefinition: SamResource[R],
    appDefinition: SamResource[AppSamResourceId],
    resourceIdDecoder: Decoder[R],
    ev: Ask[IO, TraceId]
  ): IO[Set[R]] = ???

  override def filterUserVisible[R](
    resources: NonEmptyList[R],
    userInfo: UserInfo
  )(implicit sr: SamResource[R], decoder: Decoder[R], ev: Ask[IO, TraceId]): IO[List[R]] = ???

  override def filterResourceProjectVisible[R](
    resources: NonEmptyList[(GoogleProject, R)],
    userInfo: UserInfo
  )(implicit sr: SamResource[R], decoder: Decoder[R], ev: Ask[IO, TraceId]): IO[List[(GoogleProject, R)]] =
    ???

  override def filterUserVisibleWithProjectFallback[R](
    resources: NonEmptyList[(GoogleProject, R)],
    userInfo: UserInfo
  )(implicit sr: SamResource[R], decoder: Decoder[R], ev: Ask[IO, TraceId]): IO[List[(GoogleProject, R)]] =
    ???

  override def notifyResourceCreated[R](samResource: R, creatorEmail: WorkbenchEmail, googleProject: GoogleProject)(
    implicit
    sr: SamResource[R],
    encoder: Encoder[R],
    ev: Ask[IO, TraceId]
  ): IO[Unit] = IO.unit

  override def notifyResourceDeleted[R](samResource: R, creatorEmail: WorkbenchEmail, googleProject: GoogleProject)(
    implicit
    sr: SamResource[R],
    ev: Ask[IO, TraceId]
  ): IO[Unit] = IO.unit

  override def isUserProjectReader(cloudContext: CloudContext, userInfo: UserInfo)(implicit
    ev: Ask[IO, TraceId]
  ): IO[Boolean] = ???

  override def isUserWorkspaceOwner(workspaceResource: WorkspaceResourceSamResourceId, userInfo: UserInfo)(implicit
    ev: Ask[IO, TraceId]
  ): IO[Boolean] = ???

  override def isUserWorkspaceReader(workspaceResource: WorkspaceResourceSamResourceId, userInfo: UserInfo)(implicit
    ev: Ask[IO, TraceId]
  ): IO[Boolean] = ???

  override def lookupOriginatingUserEmail[R](petOrUserInfo: UserInfo)(implicit
    ev: Ask[IO, TraceId]
  ): IO[WorkbenchEmail] = ???

  override def checkUserEnabled(petOrUserInfo: UserInfo)(implicit ev: Ask[IO, TraceId]): IO[Unit] = ???

  override def isCustomAppAllowed(userEmail: WorkbenchEmail)(implicit ev: Ask[IO, TraceId]): IO[Boolean] = ???

  override def notifyResourceCreatedV2[R](samResource: R,
                                          creatorEmail: WorkbenchEmail,
                                          cloudContext: CloudContext,
                                          workspaceId: WorkspaceId,
                                          userInfo: UserInfo
  )(implicit sr: SamResource[R], encoder: Encoder[R], ev: Ask[IO, TraceId]): IO[Unit] = ???

  override def notifyResourceDeletedV2[R](samResource: R, userInfo: UserInfo)(implicit
    sr: SamResource[R],
    ev: Ask[IO, TraceId]
  ): IO[Unit] = ???

  override def filterWorkspaceOwner(resources: NonEmptyList[WorkspaceResourceSamResourceId], userInfo: UserInfo)(
    implicit ev: Ask[IO, TraceId]
  ): IO[Set[WorkspaceResourceSamResourceId]] = ???

  override def filterWorkspaceReader(resources: NonEmptyList[WorkspaceResourceSamResourceId], userInfo: UserInfo)(
    implicit ev: Ask[IO, TraceId]
  ): IO[Set[WorkspaceResourceSamResourceId]] = ???

  override def isAdminUser(userInfo: UserInfo)(implicit ev: Ask[IO, TraceId]): IO[Boolean] = ???

  override def isSasAppAllowed(userEmail: WorkbenchEmail)(implicit ev: Ask[IO, TraceId]): IO[Boolean] = ???

  override def getLeoAuthToken: IO[String] = ???
}

object MockAuthProvider extends BaseMockAuthProvider

class FakeGoogleSubcriber[A] extends CloudSubscriber[IO, A] {
  def messages: Stream[IO, ReceivedMessage[A]] = Stream.empty
  // If you use `start`, make sure to hook up `messages` somewhere as well on the same instance for consuming the messages; Otherwise, messages will be left nacked
  def start: IO[Unit] = IO.unit
  def stop: IO[Unit] = IO.unit
}

class BaseMockRuntimeAlgebra extends RuntimeAlgebra[IO] {
  override def createRuntime(params: CreateRuntimeParams)(implicit
    ev: Ask[IO, AppContext]
  ): IO[Option[CreateGoogleRuntimeResponse]] = ???

  override def deleteRuntime(params: DeleteRuntimeParams)(implicit
    ev: Ask[IO, AppContext]
  ): IO[Option[OperationFuture[Operation, Operation]]] = IO.pure(None)

  override def finalizeDelete(params: FinalizeDeleteParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] = ???

  override def stopRuntime(params: StopRuntimeParams)(implicit
    ev: Ask[IO, AppContext]
  ): IO[Option[OperationFuture[Operation, Operation]]] = IO.pure(None)

  override def startRuntime(params: StartRuntimeParams)(implicit
    ev: Ask[IO, AppContext]
  ): IO[Option[OperationFuture[Operation, Operation]]] = IO.pure(None)

  override def updateMachineType(params: UpdateMachineTypeParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] =
    IO.unit

  override def updateDiskSize(params: UpdateDiskSizeParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] = IO.unit

  override def resizeCluster(params: ResizeClusterParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] = ???
}

object MockRuntimeAlgebra extends BaseMockRuntimeAlgebra

class MockKubernetesService(podStatus: PodStatus = PodStatus.Running, appRelease: List[Release] = List.empty)
    extends org.broadinstitute.dsde.workbench.google2.mock.MockKubernetesService {
  override def listPodStatus(clusterId: GKEModels.KubernetesClusterId, namespace: KubernetesModels.KubernetesNamespace)(
    implicit ev: Ask[IO, TraceId]
  ): IO[List[KubernetesModels.KubernetesPodStatus]] =
    IO(List(KubernetesPodStatus.apply(PodName("test"), podStatus)))

  override def listPersistentVolumeClaims(clusterId: KubernetesClusterId, namespace: KubernetesNamespace)(implicit
    ev: Ask[IO, TraceId]
  ): IO[List[V1PersistentVolumeClaim]] =
    appRelease.flatTraverse { r =>
      val nfcMetadata = new V1ObjectMeta()
      nfcMetadata.setName(s"${r.asString}-galaxy-galaxy-pvc")
      nfcMetadata.setUid(s"nfs-pvc-id1")
      val nfsPvc = new io.kubernetes.client.openapi.models.V1PersistentVolumeClaim()
      nfsPvc.setMetadata(nfcMetadata)

      val cvmfsMetadata = new V1ObjectMeta()
      cvmfsMetadata.setName(s"cvmfs-alien-cache")
      cvmfsMetadata.setUid(s"cvmfs-pvc-id1")
      val cvmfsPvc = new io.kubernetes.client.openapi.models.V1PersistentVolumeClaim()
      cvmfsPvc.setMetadata(cvmfsMetadata)

      IO.pure(
        List(nfsPvc, cvmfsPvc)
      )
    }

  override def deletePv(clusterId: KubernetesClusterId, pv: PvName)(implicit ev: Ask[IO, TraceId]): IO[Unit] =
    IO.unit
}

class MockGKEService extends GKEAlgebra[IO] {

  /** Creates a GKE cluster but doesn't wait for its completion. */
  override def createCluster(params: CreateClusterParams)(implicit
    ev: Ask[IO, AppContext]
  ): IO[Option[CreateClusterResult]] = IO.pure(None)

  /**
   * Polls a creating GKE cluster for its completion and also does other cluster-wide set-up like
   * install nginx ingress controller.
   */
  override def pollCluster(params: PollClusterParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] = IO.unit

  /** Creates a GKE nodepool but doesn't wait for its completion. */
  override def createAndPollNodepool(params: CreateNodepoolParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] = IO.unit

  /** Creates an app and polls it for completion. */
  override def createAndPollApp(params: CreateAppParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] = IO.unit

  /** Updates an app and polls it for completion. */
  override def updateAndPollApp(params: UpdateAppParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] = IO.unit

  /** Deletes a cluster and polls for completion */
  override def deleteAndPollCluster(params: DeleteClusterParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] = IO.unit

  /** Deletes a nodepool and polls for completion */
  override def deleteAndPollNodepool(params: DeleteNodepoolParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] = IO.unit

  /** Deletes an app and polls for completion */
  override def deleteAndPollApp(params: DeleteAppParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] = IO.unit

  /** Stops an app and polls for completion */
  override def stopAndPollApp(params: StopAppParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] = IO.unit

  /** Starts an app and polls for completion */
  override def startAndPollApp(params: StartAppParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] = IO.unit
}

class MockAKSInterp extends AKSAlgebra[IO] {

  /** Creates an app and polls it for completion */
  override def createAndPollApp(params: CreateAKSAppParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] = IO.unit

  /** Updates an app and polls it for completion */
  override def updateAndPollApp(params: UpdateAKSAppParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] = IO.unit

  override def deleteApp(params: DeleteAKSAppParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] = IO.unit
}

class BaseMockSamService extends SamService[IO] {
  override def getPetServiceAccount(userInfo: UserInfo, googleProject: GoogleProject)(implicit
    ev: Ask[IO, AppContext]
  ): IO[WorkbenchEmail] = IO.pure(serviceAccountEmail)

  override def getPetManagedIdentity(userInfo: UserInfo, azureCloudContext: AzureCloudContext)(implicit
    ev: Ask[IO, AppContext]
  ): IO[WorkbenchEmail] = IO.pure(managedIdentityEmail)

  override def getProxyGroup(userEmail: WorkbenchEmail)(implicit ev: Ask[IO, AppContext]): IO[WorkbenchEmail] =
    IO.pure(proxyGroupEmail)

  override def getPetServiceAccountToken(userEmail: WorkbenchEmail, googleProject: GoogleProject)(implicit
    ev: Ask[IO, AppContext]
  ): IO[String] =
    IO.pure(tokenValue)

  override def lookupWorkspaceParentForGoogleProject(userInfo: UserInfo, googleProject: GoogleProject)(implicit
    ev: Ask[IO, AppContext]
  ): IO[Option[WorkspaceId]] = IO.pure(Some(workspaceId))
}
object MockSamService extends BaseMockSamService
