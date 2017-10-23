package org.broadinstitute.dsde.workbench.leonardo.dao

import java.io.File

import org.broadinstitute.dsde.workbench.google.gcs.{GcsBucketName, GcsPath}
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterStatus.{ClusterStatus => LeoClusterStatus}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.model.WorkbenchUserEmail

import scala.concurrent.{ExecutionContext, Future}

trait DataprocDAO {
  def createCluster(googleProject: GoogleProject, clusterName: ClusterName, clusterRequest: ClusterRequest, bucketName: GcsBucketName)(implicit executionContext: ExecutionContext): Future[Cluster]

  def deleteCluster(googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Unit]

  def updateFirewallRule(googleProject: GoogleProject): Future[Unit]

  def createBucket(googleProject: GoogleProject, bucketName: GcsBucketName): Future[GcsBucketName]

  def uploadToBucket(googleProject: GoogleProject, bucketPath: GcsPath, content: File): Future[Unit]

  def uploadToBucket(googleProject: GoogleProject, bucketPath: GcsPath, content: String): Future[Unit]

  def bucketObjectExists(googleProject: GoogleProject, bucketPath: GcsPath): Future[Boolean]

  def getClusterStatus(googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[LeoClusterStatus]

  def getClusterMasterInstanceIp(googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Option[IP]]

  def getClusterErrorDetails(operationName: OperationName)(implicit executionContext: ExecutionContext): Future[Option[ClusterErrorDetails]]

  def deleteBucket(googleProject: GoogleProject, gcsBucketName: GcsBucketName)(implicit executionContext: ExecutionContext): Future[Unit]

  def getEmailFromAccessToken(accessToken: String)(implicit executionContext: ExecutionContext): Future[WorkbenchUserEmail]
}
