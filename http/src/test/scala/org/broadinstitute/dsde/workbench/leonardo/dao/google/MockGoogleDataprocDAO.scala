package org.broadinstitute.dsde.workbench.google.mock

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.model.google.DataprocRole.{Master, SecondaryWorker, Worker}
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail, WorkbenchUserId}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._

class MockGoogleDataprocDAO(ok: Boolean = true) extends GoogleDataprocDAO {

  val clusters: mutable.Map[ClusterName, Operation] = new TrieMap()
  val instances: mutable.Map[ClusterName, mutable.Map[DataprocRole, Set[InstanceKey]]] = new TrieMap()
  val badClusterName = ClusterName("badCluster")
  val errorClusterName1 = ClusterName("erroredCluster1")
  val errorClusterName2 = ClusterName("erroredCluster2")
  val errorClusters = Seq(errorClusterName1, errorClusterName2)

  private def googleID = UUID.randomUUID()

  override def createCluster(googleProject: GoogleProject, clusterName: ClusterName, config: CreateClusterConfig): Future[Operation] = {
    if (clusterName == badClusterName) {
      Future.failed(new Exception("bad cluster!"))
    } else {
      val operation = Operation(OperationName("op-name"), UUID.randomUUID())
      clusters += clusterName -> operation

      val masterInstance = Set(InstanceKey(googleProject, ZoneUri("my-zone"), InstanceName("master-instance")))
      val workerInstances = config.machineConfig.numberOfWorkers.map(num => List.tabulate(num) { i => InstanceKey(googleProject, ZoneUri("my-zone"), InstanceName(s"worker-instance-$i")) }.toSet).getOrElse(Set.empty)
      val secondaryWorkerInstances = config.machineConfig.numberOfPreemptibleWorkers.map(num => List.tabulate(num) { i => InstanceKey(googleProject, ZoneUri("my-zone"), InstanceName(s"secondary-worker-instance-$i")) }.toSet).getOrElse(Set.empty)
      instances += clusterName -> mutable.Map(Master -> masterInstance, Worker -> workerInstances, SecondaryWorker -> secondaryWorkerInstances)
      Future.successful(operation)
    }
  }

  override def deleteCluster(googleProject: GoogleProject, clusterName: ClusterName): Future[Unit] = {
    clusters.remove(clusterName)
    Future.successful(())
  }

  override def getClusterStatus(googleProject: GoogleProject, clusterName: ClusterName): Future[ClusterStatus] = {
    Future.successful {
      if (clusters.contains(clusterName) && errorClusters.contains(clusterName)) ClusterStatus.Error
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
        instances.getOrElse(clusterName, mutable.Map[DataprocRole, Set[InstanceKey]]()).toMap
      else Map.empty
    }
  }

  override def getClusterErrorDetails(operationName: Option[OperationName]): Future[Option[ClusterErrorDetails]] = {
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

  override def resizeCluster(googleProject: GoogleProject, clusterName: ClusterName, numWorkers: Option[Int], numPreemptibles: Option[Int]): Future[Unit] = {
    if(numWorkers.isDefined) {
      val workerInstances = numWorkers.map(num => List.tabulate(num) { i => InstanceKey(googleProject, ZoneUri("my-zone"), InstanceName(s"worker-instance-$i")) }.toSet).getOrElse(Set.empty)
      val existingSecondaryInstances = instances(clusterName)(SecondaryWorker)
      val existingMasterInstance = instances(clusterName)(Master)

      instances += (clusterName -> mutable.Map(Master -> existingMasterInstance, Worker -> workerInstances, SecondaryWorker -> existingSecondaryInstances))
    }

    if(numPreemptibles.isDefined) {
      val secondaryWorkerInstances = numPreemptibles.map(num => List.tabulate(num) { i => InstanceKey(googleProject, ZoneUri("my-zone"), InstanceName(s"secondary-worker-instance-$i")) }.toSet).getOrElse(Set.empty)
      val existingWorkerInstances = instances(clusterName)(Worker)
      val existingMasterInstance = instances(clusterName)(Master)

      instances += (clusterName -> mutable.Map(Master -> existingMasterInstance, Worker -> existingWorkerInstances, SecondaryWorker -> secondaryWorkerInstances))
    }

    Future.successful(())
  }
}
