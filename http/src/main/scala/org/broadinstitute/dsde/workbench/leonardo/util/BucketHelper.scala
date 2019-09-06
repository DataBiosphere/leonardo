package org.broadinstitute.dsde.workbench.leonardo.util

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import cats.data.OptionT
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.google.GoogleStorageDAO
import org.broadinstitute.dsde.workbench.leonardo.config.DataprocConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{GoogleComputeDAO, GoogleDataprocDAO}
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoException, ServiceAccountInfo, ServiceAccountProvider}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GcsEntityTypes.{Group, User}
import org.broadinstitute.dsde.workbench.model.google.GcsRoles.{GcsRole, Owner, Reader}
import org.broadinstitute.dsde.workbench.model.google.ProjectTeamTypes.{Editors, Owners, Viewers}
import org.broadinstitute.dsde.workbench.model.google.{EmailGcsEntity, GcsBucketName, GcsEntity, GoogleProject, ProjectGcsEntity, ProjectNumber}
import org.broadinstitute.dsde.workbench.util.Retry

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

case class NoGoogleProjectNumberException(googleProject: GoogleProject)
  extends LeoException(
    s"Project number could not be found for Google project $googleProject",
    StatusCodes.NotFound)

/**
  * Created by rtitle on 2/7/18.
  */
class BucketHelper(dataprocConfig: DataprocConfig,
                   gdDAO: GoogleDataprocDAO,
                   googleComputeDAO: GoogleComputeDAO,
                   googleStorageDAO: GoogleStorageDAO,
                   serviceAccountProvider: ServiceAccountProvider)
                  (implicit val executionContext: ExecutionContext, val system: ActorSystem)
  extends LazyLogging with Retry {

  /**
    * Creates the dataproc init bucket and sets the necessary ACLs.
    */
  def createInitBucket(googleProject: GoogleProject, bucketName: GcsBucketName, serviceAccountInfo: ServiceAccountInfo): Future[GcsBucketName] = {
    for {
      // The init bucket is created in the cluster's project.
      // Leo service account -> Owner
      // available service accounts ((cluster or default SA) and notebook SA, if they exist) -> Reader
      bucketSAs <- getBucketSAs(googleProject, serviceAccountInfo)
      leoEntity = userEntity(serviceAccountProvider.getLeoServiceAccountAndKey._1)
      // When we receive a lot of simultaneous cluster creation requests in the same project,
      // we hit Google bucket creation/deletion request quota of one per approx. two seconds.
      // Therefore, we are adding a second layer of retries on top of the one existing within
      // the googleStorageDAO.createBucket method
      _ <- retryUntilSuccessOrTimeout(failureLogMessage = s"Init bucket creation failed for Google project '$googleProject'")(30 seconds, 5 minutes) { () =>
        googleStorageDAO.createBucket(googleProject, bucketName, bucketSAs, List(leoEntity))
      }
    } yield bucketName
  }

  /**
    * Creates the dataproc staging bucket and sets the necessary ACLs.
    */
  def createStagingBucket(userEmail: WorkbenchEmail, googleProject: GoogleProject, bucketName: GcsBucketName, serviceAccountInfo: ServiceAccountInfo): Future[GcsBucketName] = {
    for {
      // The staging bucket is created in the cluster's project.
      // Leo service account -> Owner
      // Available service accounts ((cluster or default SA) and notebook SA, if they exist) -> Owner
      // Additional readers (users and groups) are specified by the service account provider.
      // Convenience values for projects (to address https://github.com/DataBiosphere/leonardo/issues/317)
      //    viewers-<project number> -> Reader
      //    editors-<project number> -> Owner
      //    owners-<project number> -> Owner
      bucketSAs <- getBucketSAs(googleProject, serviceAccountInfo)
      leoEntity = userEntity(serviceAccountProvider.getLeoServiceAccountAndKey._1)
      providerReaders <- serviceAccountProvider.listUsersStagingBucketReaders(userEmail).map(_.map(userEntity))
      providerGroups <- serviceAccountProvider.listGroupsStagingBucketReaders(userEmail).map(_.map(groupEntity))

      projectNumberOpt <- googleComputeDAO.getProjectNumber(googleProject)
      (projectViewers, projectEditors, projectOwners) <- getConvenienceEntities(googleProject, projectNumberOpt)

      readers = providerReaders ++ providerGroups :+ projectViewers
      owners = List(leoEntity) ++ bucketSAs :+ projectEditors :+ projectOwners

      // When we receive a lot of simultaneous cluster creation requests in the same project,
      // we hit Google bucket creation/deletion request quota of one per approx. two seconds.
      // Therefore, we are adding a second layer of retries on top of the one existing within
      // the googleStorageDAO.createBucket method
      _ <- retryUntilSuccessOrTimeout(failureLogMessage = s"Staging bucket creation failed for Google project '$googleProject'")(30 seconds, 5 minutes) { () =>
        googleStorageDAO.createBucket(googleProject, bucketName, readers, owners)
      }
    } yield bucketName
  }

  /**
    * Sets ACLs on an existing user bucket so that it can be accessed in a dataproc cluster.
    */
  def updateUserBucket(bucketName: GcsBucketName, googleProject: GoogleProject, serviceAccountInfo: ServiceAccountInfo): Future[Unit] = {
    for {
      // available service accounts ((cluster or default SA) and notebook SA, if they exist) -> Reader
      bucketSAs <- getBucketSAs(googleProject, serviceAccountInfo)

      _ <- setBucketAcls(bucketName, bucketSAs, List.empty)
    } yield ()
  }

  private def getConvenienceEntities(googleProject: GoogleProject,
                                     googleProjectNumberOpt: Option[Long]): Future[(GcsEntity, GcsEntity, GcsEntity)] = {

    def createEntities(projectNumber: Long): (GcsEntity, GcsEntity, GcsEntity) = {
      val projectViewers = ProjectGcsEntity(Viewers, ProjectNumber(projectNumber.toString))
      val projectEditors = ProjectGcsEntity(Editors, ProjectNumber(projectNumber.toString))
      val projectOwners = ProjectGcsEntity(Owners, ProjectNumber(projectNumber.toString))

      (projectViewers, projectEditors, projectOwners)
    }

    googleProjectNumberOpt match {
      case Some(projectNumber) => Future(createEntities(projectNumber))
      case _ => Future.failed(NoGoogleProjectNumberException(googleProject))
    }
  }

  private def setBucketAcls(bucketName: GcsBucketName, readers: List[GcsEntity], owners: List[GcsEntity]): Future[Unit] = {
    def setBucketAndDefaultAcls(entity: GcsEntity, role: GcsRole) = {
      for {
        _ <- googleStorageDAO.setBucketAccessControl(bucketName, entity, role)
        _ <- googleStorageDAO.setDefaultObjectAccessControl(bucketName, entity, role)
      } yield ()
    }

    def flatMapList(entities: List[GcsEntity], role: GcsRole): Future[Unit] = {
      entities match {
        case Nil => Future.unit
        case head :: tail =>
          setBucketAndDefaultAcls(head, role).flatMap(_ => flatMapList(tail, role))
      }
    }

    for {
      _ <- flatMapList(readers, Reader)
      _ <- flatMapList(owners, Owner)
    } yield ()
  }

  private def getBucketSAs(googleProject: GoogleProject, serviceAccountInfo: ServiceAccountInfo): Future[List[GcsEntity]] = {
    // cluster SA orElse compute engine default SA
    val clusterOrComputeDefault = OptionT.fromOption[Future](serviceAccountInfo.clusterServiceAccount) orElse OptionT(googleComputeDAO.getComputeEngineDefaultServiceAccount(googleProject))

    // List(cluster or default SA, notebook SA) if they exist
    clusterOrComputeDefault.value.map { clusterOrDefaultSAOpt =>
      List(clusterOrDefaultSAOpt, serviceAccountInfo.notebookServiceAccount).flatten.map(userEntity)
    }
  }

  private def userEntity(email: WorkbenchEmail) = EmailGcsEntity(User, email)
  private def groupEntity(email: WorkbenchEmail) = EmailGcsEntity(Group, email)
}
