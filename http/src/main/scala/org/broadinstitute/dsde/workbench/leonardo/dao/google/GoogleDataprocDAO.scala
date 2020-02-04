package org.broadinstitute.dsde.workbench.leonardo.dao.google

import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.workbench.leonardo.{
  CreateClusterConfig,
  DataprocRole,
  InstanceKey,
  Operation,
  OperationName,
  RuntimeErrorDetails,
  RuntimeName,
  RuntimeStatus
}
import org.broadinstitute.dsde.workbench.leonardo.model.LeoException
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.model.google._

import scala.concurrent.Future

case class DataprocDisabledException(errorMsg: String) extends LeoException(s"${errorMsg}", StatusCodes.Forbidden)

trait GoogleDataprocDAO {
  def createCluster(googleProject: GoogleProject,
                    clusterName: RuntimeName,
                    createClusterConfig: CreateClusterConfig): Future[Operation]

  def deleteCluster(googleProject: GoogleProject, clusterName: RuntimeName): Future[Unit]

  def getClusterStatus(googleProject: GoogleProject, clusterName: RuntimeName): Future[RuntimeStatus]

  def listClusters(googleProject: GoogleProject): Future[List[UUID]]

  def getClusterMasterInstance(googleProject: GoogleProject, clusterName: RuntimeName): Future[Option[InstanceKey]]

  def getClusterInstances(googleProject: GoogleProject,
                          clusterName: RuntimeName): Future[Map[DataprocRole, Set[InstanceKey]]]

  def getClusterStagingBucket(googleProject: GoogleProject, clusterName: RuntimeName): Future[Option[GcsBucketName]]

  def getClusterErrorDetails(operationName: Option[OperationName]): Future[Option[RuntimeErrorDetails]]

  def resizeCluster(googleProject: GoogleProject,
                    clusterName: RuntimeName,
                    numWorkers: Option[Int] = None,
                    numPreemptibles: Option[Int] = None): Future[Unit]

  def getUserInfoAndExpirationFromAccessToken(accessToken: String): Future[(UserInfo, Instant)]
}
