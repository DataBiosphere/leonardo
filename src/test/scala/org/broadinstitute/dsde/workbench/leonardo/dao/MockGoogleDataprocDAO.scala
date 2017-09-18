package org.broadinstitute.dsde.workbench.leonardo.dao

import java.io.File
import java.util.UUID
import org.broadinstitute.dsde.workbench.leonardo.config.DataprocConfig
import org.broadinstitute.dsde.workbench.leonardo.model.ModelTypes.GoogleProject
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterStatus._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

class MockGoogleDataprocDAO(protected val dataprocConfig: DataprocConfig) extends DataprocDAO {

  val clusters: mutable.Map[String, Cluster] = new TrieMap()  // Cluster Name and Cluster
  val firewallRules: mutable.Map[GoogleProject, String] = new TrieMap()  // Google Project and Rule Name
  val buckets: mutable.Set[String] = mutable.Set() // Set of bucket names - not keeping track of google projects since it's all in leo's project
  val bucketObjects: mutable.Set[(String, String)] = mutable.Set()  // Set of Bucket Name and File Name
  val extensionUri = "gs://aBucket/my_extension.tar.gz"


  private def googleID = UUID.randomUUID().toString

  override def createCluster(googleProject: String, clusterName: String, clusterRequest: ClusterRequest, bucketName: String)(implicit executionContext: ExecutionContext): Future[ClusterResponse] = {
    val clusterResponse = ClusterResponse(clusterName, googleProject, googleID, "status", "desc", "op-name")
    clusters += clusterName -> Cluster(clusterRequest, clusterResponse)

    Future.successful(clusterResponse)
  }

  override def deleteCluster(googleProject: String, clusterName: String)(implicit executionContext: ExecutionContext): Future[Unit] = {
    clusters.remove(clusterName)
    Future.successful(())
  }

  override def updateFirewallRule(googleProject: GoogleProject): Future[Unit] = {
    if (!firewallRules.contains(googleProject)) {
      firewallRules += googleProject -> dataprocConfig.clusterFirewallRuleName
    }
    Future.successful(())
  }

  override def createBucket(googleProject: GoogleProject, bucketName: String): Future[Unit] = {
    if (!buckets.contains(bucketName)) {
      buckets += bucketName
    }
    Future.successful(())
  }

  override def uploadToBucket(googleProject: GoogleProject, bucketName: String, fileName: String, content: File): Future[Unit] = {
    addToBucket(googleProject, bucketName, fileName)
  }

  override def uploadToBucket(googleProject: GoogleProject, bucketName: String, fileName: String, content: String): Future[Unit] = {
    addToBucket(googleProject, bucketName, fileName)
  }

  private def addToBucket(googleProject: GoogleProject, bucketName: String, fileName: String): Future[Unit] = {
    if (buckets.contains(bucketName)) {
      bucketObjects += ((bucketName, fileName))
    }
    Future.successful(())
  }

  override def getClusterStatus(googleProject: GoogleProject, clusterName: String)(implicit executionContext: ExecutionContext): Future[ClusterStatus] = {
    Future.successful {
      if (clusters.contains(clusterName)) ClusterStatus.Running
      else ClusterStatus.Unknown
    }
  }

  def getClusterMasterInstanceIp(googleProject: GoogleProject, clusterName: String)(implicit executionContext: ExecutionContext): Future[Option[String]] = {
    Future.successful {
      if (clusters.contains(clusterName)) Some("1.2.3.4")
      else None
    }
  }

  def getClusterErrorDetails(operationName: String)(implicit executionContext: ExecutionContext): Future[Option[ClusterErrorDetails]] = {
    Future.successful(None)
  }

  override def bucketObjectExists(googleProject: GoogleProject, bucketName: String, bucketObject: String): Future[Boolean] = {
    Future.successful(bucketName == "aBucket" && bucketObject == "my_extension.tar.gz")
  }
}
