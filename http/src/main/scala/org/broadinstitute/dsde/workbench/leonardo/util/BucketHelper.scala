package org.broadinstitute.dsde.workbench.leonardo.util

import akka.actor.ActorSystem
import cats.data.{NonEmptyList, OptionT}
import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.google.cloud.Identity
import com.typesafe.scalalogging.LazyLogging
import fs2._
import org.broadinstitute.dsde.workbench.google.GoogleStorageDAO
import org.broadinstitute.dsde.workbench.google2.{GcsBlobName, GoogleStorageService, StorageRole}
import org.broadinstitute.dsde.workbench.leonardo.config.DataprocConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{GoogleComputeDAO, GoogleDataprocDAO}
import org.broadinstitute.dsde.workbench.leonardo.model.{ServiceAccountInfo, ServiceAccountProvider}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import org.broadinstitute.dsde.workbench.util.Retry

import scala.concurrent.ExecutionContext

class BucketHelper(dataprocConfig: DataprocConfig,
                   gdDAO: GoogleDataprocDAO,
                   googleComputeDAO: GoogleComputeDAO,
                   googleStorageDAO: GoogleStorageDAO,
                   google2StorageDAO: GoogleStorageService[IO],
                   serviceAccountProvider: ServiceAccountProvider)
                  (implicit val executionContext: ExecutionContext, val system: ActorSystem, contextShift: ContextShift[IO])
  extends LazyLogging with Retry {

  /**
    * Creates the dataproc init bucket and sets the necessary ACLs.
    */
  def createInitBucket(googleProject: GoogleProject, bucketName: GcsBucketName, serviceAccountInfo: ServiceAccountInfo): Stream[IO, Unit] = {
    for {
      // The init bucket is created in the cluster's project.
      // Leo service account -> Owner
      // available service accounts ((cluster or default SA) and notebook SA, if they exist) -> Reader
      bucketSAs <- Stream.eval(getBucketSAs(googleProject, serviceAccountInfo))

      leoEntity = Identity.user(serviceAccountProvider.getLeoServiceAccountAndKey._1.value)
      readerAcl = NonEmptyList.fromList(bucketSAs).map(readers => Map(StorageRole.ObjectViewer -> readers)).getOrElse(Map.empty)
      ownerAcl = Map(StorageRole.ObjectAdmin -> NonEmptyList.one(leoEntity))

      // TODO set retryPolicy?
      _ <- google2StorageDAO.insertBucket(googleProject, bucketName)
      _ <- google2StorageDAO.setIamPolicy(bucketName, readerAcl ++ ownerAcl)
    } yield ()
  }

  /**
    * Creates the dataproc staging bucket and sets the necessary ACLs.
    */
  def createStagingBucket(userEmail: WorkbenchEmail, googleProject: GoogleProject, bucketName: GcsBucketName, serviceAccountInfo: ServiceAccountInfo): Stream[IO, Unit] = {
    for {
      // The staging bucket is created in the cluster's project.
      // Leo service account -> Owner
      // Available service accounts ((cluster or default SA) and notebook SA, if they exist) -> Owner
      // Additional readers (users and groups) are specified by the service account provider.
      bucketSAs <- Stream.eval(getBucketSAs(googleProject, serviceAccountInfo))
      providerReaders <- Stream.eval(IO.fromFuture(IO(serviceAccountProvider.listUsersStagingBucketReaders(userEmail)))).map(_.map(email => Identity.user(email.value)))
      providerGroups <- Stream.eval(IO.fromFuture(IO(serviceAccountProvider.listGroupsStagingBucketReaders(userEmail)))).map(_.map(email => Identity.group(email.value)))

      leoEntity = Identity.user(serviceAccountProvider.getLeoServiceAccountAndKey._1.value)
      readerAcl = NonEmptyList.fromList(providerReaders ++ providerGroups).map(readers => Map(StorageRole.ObjectViewer -> readers)).getOrElse(Map.empty)
      ownerAcl = Map(StorageRole.ObjectAdmin ->  NonEmptyList(leoEntity, bucketSAs))

      // TODO set retryPolicy?
      _ <- google2StorageDAO.insertBucket(googleProject, bucketName)
      _ <- google2StorageDAO.setIamPolicy(bucketName, readerAcl ++ ownerAcl)
    } yield ()
  }

  def storeObject(bucketName: GcsBucketName, objectName: GcsBlobName, objectContents: Array[Byte], objectType: String): Stream[IO, Unit] = {
    google2StorageDAO.createBlob(bucketName, objectName, objectContents, objectType).void
  }

  def deleteInitBucket(initBucketName: GcsBucketName): IO[Unit] = {
    // TODO: implement deleteBucket in google2
    IO.fromFuture(IO(googleStorageDAO.deleteBucket(initBucketName, recurse = true)))
  }

  private def getBucketSAs(googleProject: GoogleProject, serviceAccountInfo: ServiceAccountInfo): IO[List[Identity]] = {
    // cluster SA orElse compute engine default SA
    val clusterOrComputeDefault = OptionT.fromOption[IO](serviceAccountInfo.clusterServiceAccount) orElse
      OptionT(IO.fromFuture(IO(googleComputeDAO.getComputeEngineDefaultServiceAccount(googleProject))))

    // List(cluster or default SA, notebook SA) if they exist
    clusterOrComputeDefault.value.map { clusterOrDefaultSAOpt =>
      List(clusterOrDefaultSAOpt, serviceAccountInfo.notebookServiceAccount).flatten.map(email => Identity.user(email.value))
    }
  }
}