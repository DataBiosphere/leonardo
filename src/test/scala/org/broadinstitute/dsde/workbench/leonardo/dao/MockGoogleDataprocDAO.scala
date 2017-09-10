package org.broadinstitute.dsde.workbench.leonardo.dao

import java.io.File
import java.util.UUID

import org.broadinstitute.dsde.workbench.leonardo.config.DataprocConfig
import org.broadinstitute.dsde.workbench.leonardo.model.ModelTypes.GoogleProject
import org.broadinstitute.dsde.workbench.leonardo.model._

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}

class MockGoogleDataprocDAO(protected val dataprocConfig: DataprocConfig) extends DataprocDAO {

  val clusters: mutable.Map[String, Cluster] = new TrieMap()  // Cluster Name and Cluster
  val firewallRules: mutable.Map[GoogleProject, String] = new TrieMap()  // Google Project and Rule Name
  val buckets: mutable.ArrayBuffer[String] = new ArrayBuffer() // Array of bucket names - not keeping track of google projects since it's all in leo's project
  val bucketObjects: mutable.Map[String, String] = new TrieMap()   // Bucket Name and File Name


  private def googleID = UUID.randomUUID().toString

  override def createCluster(googleProject: String, clusterName: String, clusterRequest: ClusterRequest, bucketName: String)(implicit executionContext: ExecutionContext): Future[ClusterResponse] = {
    updateFirewallRule(googleProject)
    createBucket(googleProject, bucketName)
    uploadToBucket(googleProject, bucketName, "fileName", "fileContent")

    val clusterResponse = ClusterResponse(clusterName, googleProject, googleID, "status", "desc", "op-name")

    clusters += clusterName -> Cluster(clusterRequest, clusterResponse)

    Future.successful(clusterResponse)
  }

  override def deleteCluster(googleProject: String, clusterName: String)(implicit executionContext: ExecutionContext): Future[Unit] = {
    if(clusters.contains(clusterName))
      clusters.remove(clusterName)
    Future(())
  }

  override def updateFirewallRule(googleProject: GoogleProject): Future[Unit] = {
    Future.successful(
      if (!firewallRules.contains(googleProject)) {
        firewallRules += googleProject -> dataprocConfig.clusterFirewallRuleName
      })
  }

  override def createBucket(googleProject: GoogleProject, bucketName: String): Future[Unit] = {
    Future.successful{
      if (!buckets.contains(bucketName)) {
        buckets += bucketName
      }
    }
  }

  override def uploadToBucket(googleProject: GoogleProject, bucketName: String, fileName: String, content: File): Future[Unit] = {
    addToBucket(googleProject, bucketName, fileName)
  }

  override def uploadToBucket(googleProject: GoogleProject, bucketName: String, fileName: String, content: String): Future[Unit] = {
    addToBucket(googleProject, bucketName, fileName)
  }

  private def addToBucket(googleProject: GoogleProject, bucketName: String, fileName: String): Future[Unit] = {
    Future.successful(if (buckets.contains(bucketName)) {
      bucketObjects += bucketName -> fileName
    })
  }

}
