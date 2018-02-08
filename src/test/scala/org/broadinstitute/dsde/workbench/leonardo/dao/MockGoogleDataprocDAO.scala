package org.broadinstitute.dsde.workbench.leonardo.dao

import java.io.File
import java.net.URL
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.workbench.google.gcs.{GcsBucketName, GcsPath, GcsRelativePath}
import org.broadinstitute.dsde.workbench.leonardo.config.{ClusterDefaultsConfig, DataprocConfig, ProxyConfig}
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterStatus._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail, WorkbenchUserId}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

class MockGoogleDataprocDAO(protected val dataprocConfig: DataprocConfig, protected val proxyConfig: ProxyConfig, protected val clusterDefaultsConfig: ClusterDefaultsConfig, ok: Boolean = true) extends DataprocDAO {

  val clusters: mutable.Map[ClusterName, Cluster] = new TrieMap()  // Cluster Name and Cluster
  val firewallRules: mutable.Map[GoogleProject, String] = new TrieMap()  // Google Project and Rule Name
  val buckets: mutable.Set[GcsBucketName] = mutable.Set() // Set of bucket names - not keeping track of google projects since it's all in leo's project
  val bucketObjects: mutable.Set[GcsPath] = mutable.Set()  // Set of Bucket Name and Object
  val extensionPath = GcsPath(GcsBucketName("bucket"), GcsRelativePath("my_extension.tar.gz"))
  val badClusterName = ClusterName("badCluster")
  val errorClusterName = ClusterName("erroredCluster")

  private def googleID = UUID.randomUUID()

  def getUserInfoAndExpirationFromAccessToken(accessToken: String)(implicit executionContext: ExecutionContext): Future[(UserInfo, Instant)] = {
    Future {
      accessToken match {
        case "expired" =>
          (UserInfo(OAuth2BearerToken(accessToken), WorkbenchUserId("1234567890"), WorkbenchEmail("expiredUser@example.com"), -10), Instant.now.minusSeconds(10))
        case "unauthorized" =>
          (UserInfo(OAuth2BearerToken(accessToken), WorkbenchUserId("1234567890"), WorkbenchEmail("non_whitelisted@example.com"), (1 hour).toMillis), Instant.now.plus(1, ChronoUnit.HOURS))
        case _ =>
          (UserInfo(OAuth2BearerToken(accessToken), WorkbenchUserId("1234567890"), WorkbenchEmail("user1@example.com"), (1 hour).toMillis), Instant.now.plus(1, ChronoUnit.HOURS))
      }
    }
  }

  override def createCluster(userEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName, clusterRequest: ClusterRequest, initBucketName: GcsBucketName, serviceAccountInfo: ServiceAccountInfo, stagingBucket: GcsBucketName)(implicit executionContext: ExecutionContext): Future[Cluster] = {
    if (clusterName == badClusterName) {
      Future.failed(CallToGoogleApiFailedException(googleProject, clusterName.string, 500, "Bad Cluster!"))
    } else if(clusterName == errorClusterName){
      val cluster = Cluster(clusterName, googleID, googleProject, serviceAccountInfo, MachineConfig(clusterRequest.machineConfig, clusterDefaultsConfig),new URL("https://www.broadinstitute.org"), OperationName("op-name"), ClusterStatus.Error, None, userEmail, Instant.now(),None,clusterRequest.labels,clusterRequest.jupyterExtensionUri, clusterRequest.jupyterUserScriptUri, None)
      clusters += clusterName -> cluster
      Future.successful(cluster)
    } else {
      val cluster = Cluster.create(clusterRequest, userEmail, clusterName, googleProject, googleID, OperationName("op-name"), serviceAccountInfo, clusterDefaultsConfig, GcsBucketName("someBucket"))
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

  override def createInitBucket(bucketGoogleProject: GoogleProject, clusterGoogleProject: GoogleProject, bucketName: GcsBucketName, serviceAccountInfo: ServiceAccountInfo): Future[GcsBucketName] = {
    if (!buckets.contains(bucketName)) {
      buckets += bucketName
    }
    Future.successful(bucketName)
  }

  override def createStagingBucket(bucketGoogleProject: GoogleProject, clusterGoogleProject: GoogleProject, bucketName: GcsBucketName, serviceAccountInfo: ServiceAccountInfo, groupStagingAcl: List[WorkbenchEmail], userStagingAcl: List[WorkbenchEmail]): Future[GcsBucketName] = {
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

  override def setStagingBucketOwnership(cluster: Cluster): Future[Unit] = {
    Future.successful(())
  }

  override def getClusterStatus(googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[ClusterStatus] = {
    Future.successful {
      if (clusters.contains(clusterName) && clusterName == errorClusterName) ClusterStatus.Error
      else if(clusters.contains(clusterName)) ClusterStatus.Running
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

  override def listClusters(googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[List[UUID]] = {
    if (!ok) Future.failed(new Exception("bad project"))
    else Future.successful(Stream.continually(UUID.randomUUID).take(5).toList)
  }
}
