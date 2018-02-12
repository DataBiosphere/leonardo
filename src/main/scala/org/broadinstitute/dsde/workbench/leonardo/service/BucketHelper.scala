package org.broadinstitute.dsde.workbench.leonardo.service

import cats.data.OptionT
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.google.GoogleStorageDAO
import org.broadinstitute.dsde.workbench.leonardo.config.DataprocConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.leonardo.model.{ServiceAccountInfo, ServiceAccountProvider}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GcsEntityTypes.{Group, User}
import org.broadinstitute.dsde.workbench.model.google.GcsRoles.{GcsRole, Owner, Reader}
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsEntity, GoogleProject, generateUniqueBucketName}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by rtitle on 2/7/18.
  */
class BucketHelper(dataprocConfig: DataprocConfig,
                   gdDAO: GoogleDataprocDAO,
                   googleStorageDAO: GoogleStorageDAO,
                   serviceAccountProvider: ServiceAccountProvider)
                  (implicit val executionContext: ExecutionContext) extends LazyLogging {

  /**
    * Creates the dataproc init bucket and sets the necessary ACLs.
    */
  def createInitBucket(googleProject: GoogleProject, clusterName: ClusterName, serviceAccountInfo: ServiceAccountInfo): Future[GcsBucketName] = {
    val bucketName = generateUniqueBucketName(clusterName.value+"-init")
    for {
      // The init bucket is created in Leo's project, not the cluster's project.
      _ <- googleStorageDAO.createBucket(dataprocConfig.leoGoogleProject, bucketName)

      // Leo service account -> Owner
      // available service accounts ((cluster or default SA) and notebook SA, if they exist) -> Reader
      bucketSAs <- getBucketSAs(googleProject, serviceAccountInfo)
      leoEntity = userEntity(serviceAccountProvider.getLeoServiceAccountAndKey._1)

      _ <- setBucketAcls(bucketName, bucketSAs, List(leoEntity))
    } yield bucketName
  }

  /**
    * Creates the dataproc staging bucket and sets the necessary ACLs.
    */
  def createStagingBucket(userEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName, serviceAccountInfo: ServiceAccountInfo): Future[GcsBucketName] = {
    val bucketName = generateUniqueBucketName(clusterName.value+"-staging")
    for {
      // The staging bucket is created in the cluster's project.
      _ <- googleStorageDAO.createBucket(googleProject, bucketName)

      // Leo service account -> Owner
      // available service accounts ((cluster or default SA) and notebook SA, if they exist) -> Owner
      // Additional readers (users and groups) are specified by the service account provider.
      leoEntity = userEntity(serviceAccountProvider.getLeoServiceAccountAndKey._1)
      bucketSAs <- getBucketSAs(googleProject, serviceAccountInfo)
      providerReaders <- serviceAccountProvider.listUsersStagingBucketReaders(userEmail).map(_.map(userEntity))
      providerGroups <- serviceAccountProvider.listGroupsStagingBucketReaders(userEmail).map(_.map(groupEntity))

      _ <- setBucketAcls(bucketName, providerReaders ++ providerGroups, List(leoEntity) ++ bucketSAs)
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

  private def setBucketAcls(bucketName: GcsBucketName, readers: List[GcsEntity], owners: List[GcsEntity]): Future[Unit] = {
    def setBucketAndDefaultAcls(entity: GcsEntity, role: GcsRole) = {
      for {
        _ <- googleStorageDAO.setBucketAccessControl(bucketName, entity, role)
        _ <- googleStorageDAO.setDefaultObjectAccessControl(bucketName, entity, role)
      } yield ()
    }

    def flatMapList(entities: List[GcsEntity], role: GcsRole): Future[Unit] = {
      entities match {
        case Nil => Future.successful(())
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
    val clusterOrComputeDefault = OptionT.fromOption[Future](serviceAccountInfo.clusterServiceAccount) orElse OptionT(gdDAO.getComputeEngineDefaultServiceAccount(googleProject))

    // List(cluster or default SA, notebook SA) if they exist
    clusterOrComputeDefault.value.map { clusterOrDefaultSAOpt =>
      List(clusterOrDefaultSAOpt, serviceAccountInfo.notebookServiceAccount).flatten.map(userEntity)
    }
  }

  private def userEntity(email: WorkbenchEmail) = {
    GcsEntity(email, User)
  }

  private def groupEntity(email: WorkbenchEmail) = {
    GcsEntity(email, Group)
  }
}
