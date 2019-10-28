package org.broadinstitute.dsde.workbench.leonardo
package util

import akka.actor.ActorSystem
import cats.data.{NonEmptyList, OptionT}
import cats.effect.{ContextShift, IO}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.google.cloud.Identity
import com.typesafe.scalalogging.LazyLogging
import fs2._
import org.broadinstitute.dsde.workbench.google.GoogleStorageDAO
import org.broadinstitute.dsde.workbench.google2.{GcsBlobName, GoogleStorageService, StorageRole}
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleComputeDAO
import org.broadinstitute.dsde.workbench.leonardo.model.ServiceAccountProvider
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.util.Retry

import scala.concurrent.ExecutionContext

class BucketHelper(
  googleComputeDAO: GoogleComputeDAO,
  googleStorageDAO: GoogleStorageDAO,
  google2StorageDAO: GoogleStorageService[IO],
  serviceAccountProvider: ServiceAccountProvider[IO]
)(implicit val executionContext: ExecutionContext, val system: ActorSystem, val contextShift: ContextShift[IO])
    extends LazyLogging
    with Retry {

  val leoEntity = userIdentity(Config.serviceAccountProviderConfig.leoServiceAccount)

  /**
   * Creates the dataproc init bucket and sets the necessary ACLs.
   */
  def createInitBucket(googleProject: GoogleProject,
                       bucketName: GcsBucketName,
                       serviceAccountInfo: ServiceAccountInfo): Stream[IO, Unit] =
    for {
      // The init bucket is created in the cluster's project.
      // Leo service account -> Owner
      // available service accounts ((cluster or default SA) and notebook SA, if they exist) -> Reader
      bucketSAs <- getBucketSAs(googleProject, serviceAccountInfo)

      readerAcl = NonEmptyList
        .fromList(bucketSAs)
        .map(readers => Map(StorageRole.ObjectViewer -> readers))
        .getOrElse(Map.empty)
      ownerAcl = Map(StorageRole.ObjectAdmin -> NonEmptyList.one(leoEntity))

      _ <- google2StorageDAO.insertBucket(googleProject, bucketName)
      _ <- google2StorageDAO.setIamPolicy(bucketName, readerAcl ++ ownerAcl)
    } yield ()

  /**
   * Creates the dataproc staging bucket and sets the necessary ACLs.
   */
  def createStagingBucket(
    userEmail: WorkbenchEmail,
    googleProject: GoogleProject,
    bucketName: GcsBucketName,
    serviceAccountInfo: ServiceAccountInfo
  )(implicit ev: ApplicativeAsk[IO, TraceId]): Stream[IO, Unit] =
    for {
      // The staging bucket is created in the cluster's project.
      // Leo service account -> Owner
      // Available service accounts ((cluster or default SA) and notebook SA, if they exist) -> Owner
      // Additional readers (users and groups) are specified by the service account provider.
      bucketSAs <- getBucketSAs(googleProject, serviceAccountInfo)
      providerReaders <- Stream.eval(
        serviceAccountProvider.listUsersStagingBucketReaders(userEmail).map(_.map(userIdentity))
      )
      providerGroups <- Stream.eval(
        serviceAccountProvider.listGroupsStagingBucketReaders(userEmail).map(_.map(userIdentity))
      )

      readerAcl = NonEmptyList
        .fromList(providerReaders ++ providerGroups)
        .map(readers => Map(StorageRole.ObjectViewer -> readers))
        .getOrElse(Map.empty)
      ownerAcl = Map(StorageRole.ObjectAdmin -> NonEmptyList(leoEntity, bucketSAs))

      _ <- google2StorageDAO.insertBucket(googleProject, bucketName)
      _ <- google2StorageDAO.setIamPolicy(bucketName, readerAcl ++ ownerAcl)
    } yield ()

  def storeObject(bucketName: GcsBucketName,
                  objectName: GcsBlobName,
                  objectContents: Array[Byte],
                  objectType: String): Stream[IO, Unit] =
    google2StorageDAO.createBlob(bucketName, objectName, objectContents, objectType).void

  def deleteInitBucket(initBucketName: GcsBucketName): IO[Unit] =
    // TODO: implement deleteBucket in google2
    IO.fromFuture(IO(googleStorageDAO.deleteBucket(initBucketName, recurse = true)))

  private def getBucketSAs(googleProject: GoogleProject,
                           serviceAccountInfo: ServiceAccountInfo): Stream[IO, List[Identity]] = {
    // cluster SA orElse compute engine default SA
    val clusterOrComputeDefault = OptionT.fromOption[IO](serviceAccountInfo.clusterServiceAccount) orElse
      OptionT(IO.fromFuture(IO(googleComputeDAO.getComputeEngineDefaultServiceAccount(googleProject))))

    // List(cluster or default SA, notebook SA) if they exist
    val identities = clusterOrComputeDefault.value.map { clusterOrDefaultSAOpt =>
      List(clusterOrDefaultSAOpt, serviceAccountInfo.notebookServiceAccount).flatten.map(userIdentity)
    }

    Stream.eval(identities)
  }

  private def userIdentity(email: WorkbenchEmail) = Identity.user(email.value)
  private def groupIdentity(email: WorkbenchEmail) = Identity.group(email.value)
}
