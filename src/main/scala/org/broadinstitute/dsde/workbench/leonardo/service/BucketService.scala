package org.broadinstitute.dsde.workbench.leonardo.service

import java.util.concurrent.Executor

import cats.data.OptionT
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.google.GoogleStorageDAO
import org.broadinstitute.dsde.workbench.leonardo.config.DataprocConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.model.{ServiceAccountInfo, ServiceAccountProvider}
import org.broadinstitute.dsde.workbench.model.google.GcsRoles.{GcsRole, Owner, Reader}
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsEntity, GcsObjectName, GoogleProject, generateUniqueBucketName}

import scala.concurrent.{ExecutionContext, Future}
import cats.implicits._
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GcsEntityTypes.{Group, User}

/**
  * Created by rtitle on 2/7/18.
  */
class BucketService(dataprocConfig: DataprocConfig,
                    gdDAO: GoogleDataprocDAO,
                    googleStorageDAO: GoogleStorageDAO,
                    serviceAccountProvider: ServiceAccountProvider)
                   (implicit val executionContext: ExecutionContext) extends LazyLogging {

  def createInitBucket(googleProject: GoogleProject, clusterName: ClusterName, serviceAccountInfo: ServiceAccountInfo): Future[GcsBucketName] = {
    val bucketName = generateUniqueBucketName(clusterName.value+"-init")
    for {
      // Note the bucket is created in Leo's project, not the cluster's project.
      // ACLs are granted so the cluster's service account can access the bucket at initialization time.
      _ <- googleStorageDAO.createBucket(dataprocConfig.leoGoogleProject, bucketName)

      petEntityOpt <- getPetOrDefault(googleProject, serviceAccountInfo.copy(notebookServiceAccount = None))

      leoEntity = userEntity(serviceAccountProvider.getLeoServiceAccountAndKey._1)

      _ <- setBucketAcls(bucketName, petEntityOpt.toList, List(leoEntity))

    //  _ <- Future.traverse(objects) { obj => googleStorageDAO.storeObject(bucketName)}
      //_ <- initializeBucketObjects(userEmail, googleProject, clusterName, initBucketName, clusterRequest, notebookServiceAccountKeyOpt)
    } yield bucketName

  }

  def createStagingBucket(userEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName, serviceAccountInfo: ServiceAccountInfo): Future[GcsBucketName] = {
    val bucketName = generateUniqueBucketName(clusterName.value+"-staging")
    for {
      _ <- googleStorageDAO.createBucket(googleProject, bucketName)

      leoEntity = userEntity(serviceAccountProvider.getLeoServiceAccountAndKey._1)

      petEntityOpt <- getPetOrDefault(googleProject, serviceAccountInfo)

      providerReaders <- serviceAccountProvider.listUsersStagingBucketReaders(userEmail).map(_.map(userEntity))
      providerGroups <- serviceAccountProvider.listGroupsStagingBucketReaders(userEmail).map(_.map(groupEntity))

      _ <- setBucketAcls(bucketName, providerReaders ++ providerGroups, List(leoEntity) ++ petEntityOpt.toList)
    } yield bucketName
  }

  def updateUserBucket(bucketName: GcsBucketName, googleProject: GoogleProject, serviceAccountInfo: ServiceAccountInfo): Future[Unit] = {
    for {
      petEntityOpt <- getPetOrDefault(googleProject, serviceAccountInfo)

      _ <- setBucketAcls(bucketName, petEntityOpt.toList, List.empty)
    } yield ()
  }

  private def setBucketAcls(bucketName: GcsBucketName, readers: List[GcsEntity], owners: List[GcsEntity]): Future[Unit] = {
    implicit val synchronousExecutionContext = ExecutionContext.fromExecutor(new Executor {
      def execute(task: Runnable) = task.run()
    })

    def setAcl(entity: GcsEntity, role: GcsRole) = {
      for {
        _ <- googleStorageDAO.setBucketAccessControl(bucketName, entity, role)
        _ <- googleStorageDAO.setDefaultObjectAccessControl(bucketName, entity, role)
      } yield ()
    }

    for {
      _ <- Future.traverse(readers) { setAcl(_, Reader) }
      _ <- Future.traverse(owners) { setAcl(_, Owner) }
    } yield ()
  }

  private def getPetOrDefault(googleProject: GoogleProject, serviceAccountInfo: ServiceAccountInfo): Future[Option[GcsEntity]] = {
    val transformed =
      OptionT.fromOption[Future](serviceAccountInfo.notebookServiceAccount) orElse
        OptionT.fromOption[Future](serviceAccountInfo.clusterServiceAccount) orElse
          OptionT(gdDAO.getComputeEngineDefaultServiceAccount(googleProject))

    transformed.map(userEntity).value
  }

  private def userEntity(email: WorkbenchEmail) = {
    GcsEntity(email, User)
  }

  private def groupEntity(email: WorkbenchEmail) = {
    GcsEntity(email, Group)
  }
}
