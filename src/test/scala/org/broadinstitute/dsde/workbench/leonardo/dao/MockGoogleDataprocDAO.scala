package org.broadinstitute.dsde.workbench.leonardo.dao

import java.io.File
import java.util.UUID

import org.broadinstitute.dsde.workbench.google.gcs.{GcsBucketName, GcsPath, GcsRelativePath}
import org.broadinstitute.dsde.workbench.leonardo.config.{DataprocConfig, ProxyConfig}
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterStatus._
import org.broadinstitute.dsde.workbench.leonardo.model._

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

class MockGoogleDataprocDAO(protected val dataprocConfig: DataprocConfig, protected val proxyConfig: ProxyConfig) extends DataprocDAO {

  val clusters: mutable.Map[ClusterName, Cluster] = new TrieMap()  // Cluster Name and Cluster
  val firewallRules: mutable.Map[GoogleProject, String] = new TrieMap()  // Google Project and Rule Name
  val buckets: mutable.Set[GcsBucketName] = mutable.Set() // Set of bucket names - not keeping track of google projects since it's all in leo's project
  val bucketObjects: mutable.Set[GcsPath] = mutable.Set()  // Set of Bucket Name and Object
  val extensionPath = GcsPath(GcsBucketName("bucket"), GcsRelativePath("my_extension.tar.gz"))
  val badClusterName = ClusterName("badCluster")

  private def googleID = UUID.randomUUID()

  override def createCluster(googleProject: GoogleProject, clusterName: ClusterName, clusterRequest: ClusterRequest, bucketName: GcsBucketName)(implicit executionContext: ExecutionContext): Future[Cluster] = {
    if (clusterName == badClusterName) {
      Future.failed(CallToGoogleApiFailedException(googleProject, clusterName.string, 500, "Bad Cluster!"))
    } else {
      val cluster = Cluster.create(clusterRequest, clusterName, googleProject, googleID, OperationName("op-name"))
      clusters += clusterName -> cluster
      Future.successful(cluster)
    }
  }

  override def deleteCluster(googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Unit] = {
    clusters.remove(clusterName)
    Future.successful(())
  }

  override def updateFirewallRule(googleProject: GoogleProject): Future[Unit] = {
    if (!firewallRules.contains(googleProject)) {
      firewallRules += googleProject -> proxyConfig.firewallRuleName
    }
    Future.successful(())
  }

  override def createBucket(googleProject: GoogleProject, bucketName: GcsBucketName): Future[GcsBucketName] = {
    if (!buckets.contains(bucketName)) {
      buckets += bucketName
    }
    Future.successful(bucketName)
  }

  override def deleteBucket(googleProject: GoogleProject, bucketName: GcsBucketName)(implicit executionContext: ExecutionContext): Future[Unit] = {
    buckets -= bucketName
    Future.successful(())
  }

  override def uploadToBucket(googleProject: GoogleProject, bucketPath: GcsPath, content: File): Future[Unit] = {
    addToBucket(googleProject, bucketPath)
  }

  override def uploadToBucket(googleProject: GoogleProject, bucketPath: GcsPath, content: String): Future[Unit] = {
    addToBucket(googleProject, bucketPath)
  }

  private def addToBucket(googleProject: GoogleProject, bucketPath: GcsPath): Future[Unit] = {
    if (buckets.contains(bucketPath.bucketName)) {
      bucketObjects += bucketPath
    }
    Future.successful(())
  }

  override def getClusterStatus(googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[ClusterStatus] = {
    Future.successful {
      if (clusters.contains(clusterName)) ClusterStatus.Running
      else ClusterStatus.Unknown
    }
  }

  override def getClusterMasterInstanceIp(googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Option[IP]] = {
    Future.successful {
      if (clusters.contains(clusterName)) Some(IP("1.2.3.4"))
      else None
    }
  }

  override def getClusterErrorDetails(operationName: OperationName)(implicit executionContext: ExecutionContext): Future[Option[ClusterErrorDetails]] = {
    Future.successful(None)
  }

  override def bucketObjectExists(googleProject: GoogleProject, bucketPath: GcsPath): Future[Boolean] = {
    Future.successful(bucketPath.bucketName == GcsBucketName("bucket") && bucketPath.relativePath == GcsRelativePath("my_extension.tar.gz"))
  }
}
