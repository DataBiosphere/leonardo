package org.broadinstitute.dsde.workbench.leonardo

import cats.effect.IO
import cats.mtl.ApplicativeAsk
import com.google.auth.Credentials
import com.google.cloud.storage.Blob
import com.google.cloud.storage.Storage.BucketSourceOption
import com.google.pubsub.v1.PubsubMessage
import fs2.Pipe
import fs2.Stream
import io.circe.Encoder
import org.broadinstitute.dsde.workbench.RetryConfig
import org.broadinstitute.dsde.workbench.google2.{
  Event,
  GcsBlobName,
  GetMetadataResponse,
  GooglePublisher,
  GoogleSubscriber
}
import org.broadinstitute.dsde.workbench.google2.mock.BaseFakeGoogleStorage
import org.broadinstitute.dsde.workbench.leonardo.SamResource.{PersistentDiskSamResource, RuntimeSamResource}
import org.broadinstitute.dsde.workbench.leonardo.model.{
  LeoAuthProvider,
  PersistentDiskAction,
  ProjectAction,
  RuntimeAction,
  ServiceAccountProvider
}
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}

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
  override def hasProjectPermission(userInfo: UserInfo, action: ProjectAction, googleProject: GoogleProject)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Boolean] = ???
  override def hasRuntimePermission(
    samResource: RuntimeSamResource,
    userInfo: UserInfo,
    action: RuntimeAction,
    googleProject: GoogleProject
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Boolean] = ???
  override def hasPersistentDiskPermission(
    samResource: PersistentDiskSamResource,
    userInfo: UserInfo,
    action: PersistentDiskAction,
    googleProject: GoogleProject
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Boolean] = ???
  override def filterUserVisibleRuntimes(userInfo: UserInfo, runtimes: List[(GoogleProject, RuntimeSamResource)])(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[List[(GoogleProject, RuntimeSamResource)]] = ???
  override def filterUserVisiblePersistentDisks(
    userInfo: UserInfo,
    disks: List[(GoogleProject, PersistentDiskSamResource)]
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[List[(GoogleProject, PersistentDiskSamResource)]] = ???
  override def notifyResourceCreated(samResource: SamResource,
                                     creatorEmail: WorkbenchEmail,
                                     googleProject: GoogleProject)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    ???
  override def notifyResourceDeleted(samResource: SamResource,
                                     userEmail: WorkbenchEmail,
                                     creatorEmail: WorkbenchEmail,
                                     googleProject: GoogleProject)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    IO.unit

  /**
   *
   *
   * @param internalId
   * @return returns a list of Cluster Actions that the user has access to
   */

  override def getNotebookClusterActions(internalId: RuntimeInternalId, userInfo: UserInfo)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[List[NotebookClusterAction]] = ???
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
