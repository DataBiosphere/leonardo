package org.broadinstitute.dsde.workbench.leonardo

import cats.data.NonEmptyList
import cats.effect.IO
import cats.mtl.ApplicativeAsk
import com.google.auth.Credentials
import com.google.cloud.compute.v1.Operation
import com.google.cloud.storage.Blob
import com.google.cloud.storage.Storage.BucketSourceOption
import com.google.pubsub.v1.PubsubMessage
import fs2.{Pipe, Stream}
import io.circe.{Decoder, Encoder}
import org.broadinstitute.dsde.workbench.RetryConfig
import org.broadinstitute.dsde.workbench.google2.KubernetesModels.{KubernetesPodStatus, PodStatus}
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.PodName
import org.broadinstitute.dsde.workbench.google2.mock.BaseFakeGoogleStorage
import org.broadinstitute.dsde.workbench.google2.{
  Event,
  GKEModels,
  GcsBlobName,
  GetMetadataResponse,
  GooglePublisher,
  GoogleSubscriber,
  KubernetesModels
}
import org.broadinstitute.dsde.workbench.leonardo.model.{
  LeoAuthProvider,
  SamResource,
  SamResourceAction,
  ServiceAccountProvider
}
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}

object FakeGoogleStorageService extends BaseFakeGoogleStorage {
  override def getObjectMetadata(bucketName: GcsBucketName,
                                 blobName: GcsBlobName,
                                 traceId: Option[TraceId],
                                 retryConfig: RetryConfig): fs2.Stream[IO, GetMetadataResponse] =
    fs2.Stream.empty.covary[IO]

  override def getBlob(bucketName: GcsBucketName,
                       blobName: GcsBlobName,
                       credentials: scala.Option[Credentials],
                       traceId: Option[TraceId],
                       retryConfig: RetryConfig): fs2.Stream[IO, Blob] =
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
                            retryConfig: RetryConfig): fs2.Stream[IO, Boolean] =
    throw new Exception("this shouldn't get called")
}

object MockAuthProvider extends LeoAuthProvider[IO] {
  override def serviceAccountProvider: ServiceAccountProvider[IO] = ???

  override def hasPermission[R, A](samResource: R, action: A, userInfo: UserInfo)(
    implicit sr: SamResourceAction[R, A],
    ev: ApplicativeAsk[IO, TraceId]
  ): IO[Boolean] = ???

  override def hasPermissionWithProjectFallback[R, A](
    samResource: R,
    action: A,
    projectAction: ProjectAction,
    userInfo: UserInfo,
    googleProject: GoogleProject
  )(implicit sr: SamResourceAction[R, A], ev: ApplicativeAsk[IO, TraceId]): IO[Boolean] = ???

  override def getActions[R, A](samResource: R, userInfo: UserInfo)(
    implicit sr: SamResourceAction[R, A],
    ev: ApplicativeAsk[IO, TraceId]
  ): IO[List[sr.ActionCategory]] = ???

  override def getActionsWithProjectFallback[R, A](samResource: R, googleProject: GoogleProject, userInfo: UserInfo)(
    implicit sr: SamResourceAction[R, A],
    ev: ApplicativeAsk[IO, TraceId]
  ): IO[(List[sr.ActionCategory], List[ProjectAction])] = ???

  override def filterUserVisible[R](
    resources: NonEmptyList[R],
    userInfo: UserInfo
  )(implicit sr: SamResource[R], decoder: Decoder[R], ev: ApplicativeAsk[IO, TraceId]): IO[List[R]] = ???

  override def filterUserVisibleWithProjectFallback[R](
    resources: NonEmptyList[(GoogleProject, R)],
    userInfo: UserInfo
  )(implicit sr: SamResource[R], decoder: Decoder[R], ev: ApplicativeAsk[IO, TraceId]): IO[List[(GoogleProject, R)]] =
    ???

  override def notifyResourceCreated[R](samResource: R, creatorEmail: WorkbenchEmail, googleProject: GoogleProject)(
    implicit sr: SamResource[R],
    encoder: Encoder[R],
    ev: ApplicativeAsk[IO, TraceId]
  ): IO[Unit] = IO.unit

  override def notifyResourceDeleted[R](samResource: R, creatorEmail: WorkbenchEmail, googleProject: GoogleProject)(
    implicit sr: SamResource[R],
    ev: ApplicativeAsk[IO, TraceId]
  ): IO[Unit] = IO.unit
}

object FakeGooglePublisher extends GooglePublisher[IO] {
  override def publish[MessageType](implicit evidence$1: Encoder[MessageType]): Pipe[IO, MessageType, Unit] =
    in => in.evalMap(_ => IO.unit)

  override def publishNative: Pipe[IO, PubsubMessage, Unit] = in => in.evalMap(_ => IO.unit)

  override def publishString: Pipe[IO, String, Unit] = in => in.evalMap(_ => IO.unit)
}

class FakeGoogleSubcriber[A] extends GoogleSubscriber[IO, A] {
  def messages: Stream[IO, Event[A]] = Stream.empty
  // If you use `start`, make sure to hook up `messages` somewhere as well on the same instance for consuming the messages; Otherwise, messages will be left nacked
  def start: IO[Unit] = IO.unit
  def stop: IO[Unit] = IO.unit
}

object MockRuntimeAlgebra extends RuntimeAlgebra[IO] {
  override def createRuntime(params: CreateRuntimeParams)(
    implicit ev: ApplicativeAsk[IO, AppContext]
  ): IO[CreateRuntimeResponse] = ???

  override def getRuntimeStatus(params: GetRuntimeStatusParams)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[RuntimeStatus] = ???

  override def deleteRuntime(params: DeleteRuntimeParams)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Option[Operation]] = IO.pure(None)

  override def finalizeDelete(params: FinalizeDeleteParams)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] = ???

  override def stopRuntime(params: StopRuntimeParams)(
    implicit ev: ApplicativeAsk[IO, AppContext]
  ): IO[Option[Operation]] = ???

  override def startRuntime(params: StartRuntimeParams)(implicit ev: ApplicativeAsk[IO, AppContext]): IO[Unit] = ???

  override def updateMachineType(params: UpdateMachineTypeParams)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    ???

  override def updateDiskSize(params: UpdateDiskSizeParams)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] = ???

  override def resizeCluster(params: ResizeClusterParams)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] = ???
}

class MockKubernetesService(podStatus: PodStatus = PodStatus.Running)
    extends org.broadinstitute.dsde.workbench.google2.mock.MockKubernetesService {

  override def listPodStatus(clusterId: GKEModels.KubernetesClusterId, namespace: KubernetesModels.KubernetesNamespace)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[List[KubernetesModels.KubernetesPodStatus]] =
    IO(List(KubernetesPodStatus.apply(PodName("test"), podStatus)))

}
