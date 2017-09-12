package org.broadinstitute.dsde.workbench.leonardo.dao

import java.io.File
import org.broadinstitute.dsde.workbench.leonardo.model.ModelTypes.GoogleProject
import org.broadinstitute.dsde.workbench.leonardo.model._

import scala.concurrent.{ExecutionContext, Future}

trait DataprocDAO {
  def createCluster(googleProject: GoogleProject, clusterName: String, clusterRequest: ClusterRequest, bucketName: String)(implicit executionContext: ExecutionContext): Future[ClusterResponse]

  def deleteCluster(googleProject: String, clusterName: String)(implicit executionContext: ExecutionContext): Future[Unit]

  def updateFirewallRule(googleProject: GoogleProject): Future[Unit]

  def createBucket(googleProject: GoogleProject, bucketName: String): Future[Unit]

  def uploadToBucket(googleProject: GoogleProject, bucketName: String, fileName: String, content: File): Future[Unit]

  def uploadToBucket(googleProject: GoogleProject, bucketName: String, fileName: String, content: String): Future[Unit]
}
