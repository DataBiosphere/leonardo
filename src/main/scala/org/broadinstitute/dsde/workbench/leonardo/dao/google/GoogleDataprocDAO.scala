package org.broadinstitute.dsde.workbench.leonardo.dao.google

import java.time.Instant
import java.util.UUID

import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google._

import scala.concurrent.Future

trait GoogleDataprocDAO {
  def createCluster(googleProject: GoogleProject, clusterName: ClusterName, machineConfig: MachineConfig, initScript: GcsPath, clusterServiceAccount: Option[WorkbenchEmail], credentialsFileName: Option[String], stagingBucket: GcsBucketName): Future[Operation]

  def deleteCluster(googleProject: GoogleProject, clusterName: ClusterName): Future[Unit]

  def getClusterStatus(googleProject: GoogleProject, clusterName: ClusterName): Future[ClusterStatus]

  def listClusters(googleProject: GoogleProject): Future[List[UUID]]

  def getClusterMasterInstance(googleProject: GoogleProject, clusterName: ClusterName): Future[Option[InstanceKey]]

  def getClusterInstances(googleProject: GoogleProject, clusterName: ClusterName): Future[Map[DataprocRole, Set[InstanceKey]]]

  def getClusterStagingBucket(googleProject: GoogleProject, clusterName: ClusterName): Future[Option[GcsBucketName]]

  def getClusterErrorDetails(operationName: OperationName): Future[Option[ClusterErrorDetails]]

  def getUserInfoAndExpirationFromAccessToken(accessToken: String): Future[(UserInfo, Instant)]
}