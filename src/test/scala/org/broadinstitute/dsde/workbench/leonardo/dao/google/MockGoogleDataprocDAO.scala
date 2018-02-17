package org.broadinstitute.dsde.workbench.google.mock

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.model.google.DataprocRole.{Master, Worker}
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail, WorkbenchUserId}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._

class MockGoogleDataprocDAO(ok: Boolean = true) extends GoogleDataprocDAO {

  val clusters: mutable.Map[ClusterName, Operation] = new TrieMap()
  val badClusterName = ClusterName("badCluster")
  val errorClusterName = ClusterName("erroredCluster")

  private def googleID = UUID.randomUUID()

  override def createCluster(googleProject: GoogleProject, clusterName: ClusterName, machineConfg: MachineConfig, initScript: GcsPath, clusterServiceAccount: Option[WorkbenchEmail], credentialsFileName: Option[String], stagingBucket: GcsBucketName): Future[Operation] = {
    if (clusterName == badClusterName) {
      Future.failed(new Exception("bad cluster!"))
    } else {
      val operation = Operation(OperationName("op-name"), UUID.randomUUID())
      clusters += clusterName -> operation
      Future.successful(operation)
    }
  }

  override def deleteCluster(googleProject: GoogleProject, clusterName: ClusterName): Future[Unit] = {
    clusters.remove(clusterName)
    Future.successful(())
  }

  override def getClusterStatus(googleProject: GoogleProject, clusterName: ClusterName): Future[ClusterStatus] = {
    Future.successful {
      if (clusters.contains(clusterName) && clusterName == errorClusterName) ClusterStatus.Error
      else if(clusters.contains(clusterName)) ClusterStatus.Running
      else ClusterStatus.Unknown
    }
  }

  override def listClusters(googleProject: GoogleProject): Future[List[UUID]] = {
    if (!ok) Future.failed(new Exception("bad project"))
    else Future.successful(Stream.continually(UUID.randomUUID).take(5).toList)
  }

  override def getClusterMasterInstance(googleProject: GoogleProject, clusterName: ClusterName): Future[Option[InstanceKey]] = {
    Future.successful {
      if (clusters.contains(clusterName)) Some(InstanceKey(googleProject, ZoneUri("my-zone"), InstanceName("master-instance")))
      else None
    }
  }

  override def getClusterInstances(googleProject: GoogleProject, clusterName: ClusterName): Future[Map[DataprocRole, Set[InstanceKey]]] = {
    Future.successful {
      if (clusters.contains(clusterName))
        Map(Master -> Set(InstanceKey(googleProject, ZoneUri("my-zone"), InstanceName("master-instance"))),
            Worker -> Set(InstanceKey(googleProject, ZoneUri("my-zone"), InstanceName("worker-instance-1")),
                          InstanceKey(googleProject, ZoneUri("my-zone"), InstanceName("worker-instance-2"))))
      else Map.empty
    }
  }

  override def getClusterErrorDetails(operationName: OperationName): Future[Option[ClusterErrorDetails]] = {
    Future.successful(None)
  }

  override def getUserInfoAndExpirationFromAccessToken(accessToken: String): Future[(UserInfo, Instant)] = {
    Future.successful {
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

  override def getClusterStagingBucket(googleProject: GoogleProject, clusterName: ClusterName): Future[Option[GcsBucketName]] = {
    Future.successful(Some(GcsBucketName("staging-bucket")))
  }
}