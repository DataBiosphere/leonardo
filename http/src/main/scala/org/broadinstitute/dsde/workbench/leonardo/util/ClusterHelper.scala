package org.broadinstitute.dsde.workbench.leonardo.util

import java.nio.charset.Charset

import akka.actor.ActorSystem
import cats.effect._
import cats.implicits._
import com.google.api.client.http.HttpResponseException
import com.typesafe.scalalogging.LazyLogging
import fs2._
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO
import org.broadinstitute.dsde.workbench.leonardo.config.{ClusterFilesConfig, ClusterResourcesConfig, DataprocConfig, ProxyConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{GoogleComputeDAO, GoogleDataprocDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, ClusterInitValues, LeoException, ServiceAccountInfo}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.google.GoogleUtilities.RetryPredicates._
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.leonardo.model.google.DataprocRole.Master
import org.broadinstitute.dsde.workbench.leonardo.model.google.{FirewallRule, FirewallRuleName, FirewallRulePort, FirewallRuleProtocol, MachineType, NetworkTag, VPCNetworkName}
import org.broadinstitute.dsde.workbench.leonardo.util.TemplateHelper.GcsResource
import org.broadinstitute.dsde.workbench.util.Retry

import scala.concurrent.{ExecutionContext, Future}

case class ClusterIamSetupException(googleProject: GoogleProject)
  extends LeoException(s"Error occurred setting up IAM roles in project ${googleProject.value}")

class ClusterHelper(dbRef: DbReference,
                    dataprocConfig: DataprocConfig,
                    proxyConfig: ProxyConfig,
                    clusterResourcesConfig: ClusterResourcesConfig,
                    clusterFilesConfig: ClusterFilesConfig,
                    bucketHelper: BucketHelper,
                    gdDAO: GoogleDataprocDAO,
                    googleComputeDAO: GoogleComputeDAO,
                    googleIamDAO: GoogleIamDAO,
                    contentSecurityPolicy: String)
                   (implicit val executionContext: ExecutionContext, val system: ActorSystem, val contextShift: ContextShift[IO]) extends LazyLogging with Retry {

  def createClusterIamRoles(googleProject: GoogleProject, serviceAccountInfo: ServiceAccountInfo): IO[Unit] = {
    updateClusterIamRoles(googleProject, serviceAccountInfo, true)
  }

  def removeClusterIamRoles(googleProject: GoogleProject, serviceAccountInfo: ServiceAccountInfo): IO[Unit] = {
    updateClusterIamRoles(googleProject, serviceAccountInfo, false)
  }

  private def updateClusterIamRoles(googleProject: GoogleProject, serviceAccountInfo: ServiceAccountInfo, create: Boolean): IO[Unit] = {
    val retryIam: (GoogleProject, WorkbenchEmail, Set[String]) => IO[Boolean] = (project, email, roles) =>
      IO.fromFuture[Boolean](IO(retryExponentially(when409, s"IAM policy change failed for Google project '$project'") { () =>
        if (create) {
          googleIamDAO.addIamRolesForUser(project, email, roles)
        } else {
          googleIamDAO.removeIamRolesForUser(project, email, roles)
        }
      }))

    // Add the Dataproc Worker role in the user's project to the cluster service account, if present.
    // This is needed to be able to spin up Dataproc clusters using a custom service account.
    // If the Google Compute default service account is being used, this is not necessary.
    val dataprocWorkerIO = serviceAccountInfo.clusterServiceAccount.map { email =>
      // Note: don't remove the role if there are existing active clusters owned by the same user,
      // because it could potentially break other clusters. We only check this for the 'remove' case,
      // it's ok to re-add the roles.
      IO.fromFuture(IO(dbRef.inTransaction { _.clusterQuery.countActiveByClusterServiceAccount(email) })).flatMap { count =>
        if (count > 0 && create == false) {
          IO.unit
        } else {
          retryIam(googleProject, email, Set("roles/dataproc.worker"))
        }
      }
    } getOrElse IO.unit

    // TODO: replace this logic with a group based approach so we don't have to manipulate IAM directly in the image project.
    // See https://broadworkbench.atlassian.net/browse/IA-1364
    //
    // Add the Compute Image User role in the image project to the Google API service account.
    // This is needed in order to use a custom dataproc VM image.
    // If a custom image is not being used, this is not necessary.
    val computeImageUserIO = dataprocConfig.customDataprocImage.flatMap(parseImageProject) match {
      case None => IO.unit
      case Some(imageProject) if imageProject == googleProject => IO.unit
      case Some(imageProject) =>
        IO.fromFuture(IO(dbRef.inTransaction { _.clusterQuery.countActiveByProject(googleProject) })).flatMap { count =>
          // Note: don't remove the role if there are existing active clusters in the same project,
          // because it could potentially break other clusters. We only check this for the 'remove' case,
          // it's ok to re-add the roles.
          if (count > 0 && create == false) {
            IO.unit
          } else {
            for {
              projectNumber <- IO.fromFuture(IO(googleComputeDAO.getProjectNumber(googleProject))).flatMap(_.fold(IO.raiseError[Long](ClusterIamSetupException(imageProject)))(IO.pure))
              roles = Set("roles/compute.imageUser")

              // The Dataproc SA is used to retrieve the image. However projects created prior to 2016
              // don't have a Dataproc SA so they fall back to the API service account. This is documented here:
              // https://cloud.google.com/dataproc/docs/concepts/iam/iam#service_accounts
              dataprocSA = WorkbenchEmail(s"service-$projectNumber@dataproc-accounts.iam.gserviceaccount.com")
              apiSA = WorkbenchEmail(s"$projectNumber@cloudservices.gserviceaccount.com")
              _ <- retryIam(imageProject, dataprocSA, roles).recoverWith {
                case e if when400(e) => retryIam(imageProject, apiSA, roles)
              }
            } yield ()
          }
        }
    }

    List(dataprocWorkerIO, computeImageUserIO).parSequence_
  }

  private def when400(throwable: Throwable): Boolean = throwable match {
    case t: HttpResponseException => t.getStatusCode == 400
    case _ => false
  }

  def generateServiceAccountKey(googleProject: GoogleProject, serviceAccountEmailOpt: Option[WorkbenchEmail]): IO[Option[ServiceAccountKey]] = {
    // TODO: implement google2 version of GoogleIamDAO
    serviceAccountEmailOpt.traverse { email =>
      IO.fromFuture(IO(googleIamDAO.createServiceAccountKey(googleProject, email)))
    }
  }

  def removeServiceAccountKey(googleProject: GoogleProject, serviceAccountEmailOpt: Option[WorkbenchEmail], serviceAccountKeyIdOpt: Option[ServiceAccountKeyId]): IO[Unit] = {
    // TODO: implement google2 version of GoogleIamDAO
    (serviceAccountEmailOpt, serviceAccountKeyIdOpt).mapN { case (email, keyId) =>
      IO.fromFuture(IO(googleIamDAO.removeServiceAccountKey(googleProject, email, keyId)))
    } getOrElse IO.unit
  }

  def resizeCluster(cluster: Cluster, numWorkers: Option[Int], numPreemptibles: Option[Int]): IO[Unit] = {
    for {
      // IAM roles should already exist for a non-deleted cluster; this method is a no-op if the roles already exist.
      _ <- createClusterIamRoles(cluster.googleProject, cluster.serviceAccountInfo)

      // Resize the cluster in Google
      // TODO: implement google2 version of GoogleComputeDAO
      _ <- IO.fromFuture(IO(gdDAO.resizeCluster(cluster.googleProject, cluster.clusterName, numWorkers, numPreemptibles)))
    } yield ()
  }

  def setMasterMachineType(cluster: Cluster, machineType: MachineType): IO[Unit] = {
    cluster.instances.toList.traverse { instance =>
      instance.dataprocRole match {
        case Some(Master) =>
          // TODO: implement google2 version of GoogleComputeDAO
          IO.fromFuture(IO(googleComputeDAO.setMachineType(instance.key, machineType)))
        case _ =>
          // Note: we don't support changing the machine type for worker instances. While this is possible
          // in GCP, Spark settings are auto-tuned to machine size. Dataproc recommends adding or removing nodes,
          // and rebuilding the cluster if new worker machine/disk sizes are needed.
          IO.unit
      }
    }.void
  }

  def updateMasterDiskSize(cluster: Cluster, diskSize: Int): IO[Unit] = {
    cluster.instances.toList.traverse { instance =>
      instance.dataprocRole match {
        case Some(Master) =>
          // TODO: implement google2 version of GoogleComputeDAO
          IO.fromFuture(IO(googleComputeDAO.resizeDisk(instance.key, diskSize)))
        case _ =>
          // Note: we don't support changing the machine type for worker instances. While this is possible
          // in GCP, Spark settings are auto-tuned to machine size. Dataproc recommends adding or removing nodes,
          // and rebuilding the cluster if new worker machine/disk sizes are needed.
          IO.unit
      }
    }.void
  }

  def deleteCluster(cluster: Cluster): IO[Unit] = {
    IO.fromFuture(IO(gdDAO.deleteCluster(cluster.googleProject, cluster.clusterName)))
  }

  def stopCluster(cluster: Cluster): IO[Unit] = {
    for {
      // First remove all its preemptible instances, if any
      _ <- if (cluster.machineConfig.numberOfPreemptibleWorkers.exists(_ > 0))
        IO.fromFuture(IO(gdDAO.resizeCluster(cluster.googleProject, cluster.clusterName, numPreemptibles = Some(0))))
      else IO.unit

      // Now stop each instance individually
      _ <- cluster.nonPreemptibleInstances.toList.traverse { instance =>
        IO.fromFuture(IO(googleComputeDAO.stopInstance(instance.key)))
      }
    } yield ()
  }

  def startCluster(cluster: Cluster, metadata: Map[String, String]): IO[Unit] = {
    for {
      // Add back the preemptible instances, if any
      _ <- if (cluster.machineConfig.numberOfPreemptibleWorkers.exists(_ > 0))
        IO.fromFuture(IO(gdDAO.resizeCluster(cluster.googleProject, cluster.clusterName, numPreemptibles = cluster.machineConfig.numberOfPreemptibleWorkers)))
      else IO.unit

      // Start each instance individually
      _ <- cluster.nonPreemptibleInstances.toList.traverse { instance =>
        // Install a startup script on the master node so Jupyter starts back up again once the instance is restarted
        IO.fromFuture(IO(instance.dataprocRole match {
          case Some(Master) =>
            googleComputeDAO.addInstanceMetadata(instance.key, metadata) >>
              googleComputeDAO.startInstance(instance.key)
          case _ =>
            googleComputeDAO.startInstance(instance.key)
        }))
      }

    } yield ()
  }

  def createGoogleCluster(cluster: Cluster): IO[(Cluster, GcsBucketName, Option[ServiceAccountKey])] = {
    val initBucketName = generateUniqueBucketName("leoinit-"+cluster.clusterName.value)
    val stagingBucketName = generateUniqueBucketName("leostaging-"+cluster.clusterName.value)

    // Generate a service account key for the notebook service account (if present) to localize on the cluster.
    // We don't need to do this for the cluster service account because its credentials are already
    // on the metadata server.
    generateServiceAccountKey(cluster.googleProject, cluster.serviceAccountInfo.notebookServiceAccount).flatMap { serviceAccountKeyOpt =>
      for {
        // Create the firewall rule in the google project if it doesn't already exist, so we can access the cluster
        _ <- IO.fromFuture(IO(googleComputeDAO.updateFirewallRule(cluster.googleProject, firewallRule)))

        // Set up IAM roles necessary to create a cluster.
        _ <- createClusterIamRoles(cluster.googleProject, cluster.serviceAccountInfo)

        // Create the bucket in the cluster's google project and populate with initialization files.
        // ACLs are granted so the cluster service account can access the files at initialization time.
        _ <- bucketHelper.createInitBucket(cluster.googleProject, initBucketName, cluster.serviceAccountInfo).compile.drain
        _ <- initializeBucketObjects(cluster, initBucketName, serviceAccountKeyOpt).compile.drain

        // Create the cluster staging bucket. ACLs are granted so the user/pet can access it.
        _ <- bucketHelper.createStagingBucket(cluster.auditInfo.creator, cluster.googleProject, stagingBucketName, cluster.serviceAccountInfo).compile.drain
      } yield ???

    }

    // memoized
    val serviceAccountKeyFuture = generateServiceAccountKey(cluster.googleProject, cluster.serviceAccountInfo.notebookServiceAccount)
    val serviceAccountKeyIO = IO.fromFuture(IO(serviceAccountKeyFuture))

    val ioResult: IO[(Cluster, GcsBucketName, Option[ServiceAccountKey])] = for {
      // Create the firewall rule in the google project if it doesn't already exist, so we can access the cluster
      _ <- IO.fromFuture(IO(googleComputeDAO.updateFirewallRule(cluster.googleProject, firewallRule)))

      // Generate a service account key for the notebook service account (if present) to localize on the cluster.
      // We don't need to do this for the cluster service account because its credentials are already
      // on the metadata server.
      serviceAccountKeyOpt <- serviceAccountKeyIO

      // Add Dataproc Worker role to the cluster service account, if present.
      // This is needed to be able to spin up Dataproc clusters.
      // If the Google Compute default service account is being used, this is not necessary.
      _ <- IO.fromFuture(IO(addDataprocWorkerRoleToServiceAccount(cluster.googleProject, cluster.serviceAccountInfo.clusterServiceAccount)))

      // Create the bucket in the cluster's google project and populate with initialization files.
      // ACLs are granted so the cluster service account can access the files at initialization time.
      _ <- bucketHelper.createInitBucket(cluster.googleProject, initBucketName, cluster.serviceAccountInfo).compile.drain
      _ <- initializeBucketObjects(cluster, initBucketName, serviceAccountKeyOpt).compile.drain

      // Create the cluster staging bucket. ACLs are granted so the user/pet can access it.
      _ <- bucketHelper.createStagingBucket(cluster.auditInfo.creator, cluster.googleProject, stagingBucketName, cluster.serviceAccountInfo)

      // build cluster configuration
      machineConfig = cluster.machineConfig
      initScript = GcsPath(initBucketName, GcsObjectName(clusterResourcesConfig.initActionsScript.value))
      //   autopauseThreshold = cluster.autopauseThreshold
      //  clusterScopes = if(clusterRequest.scopes.isEmpty) dataprocConfig.defaultScopes else clusterRequest.scopes
      credentialsFileName = cluster.serviceAccountInfo.notebookServiceAccount.map(_ => s"/etc/${ClusterInitValues.serviceAccountCredentialsFilename}")

      // decide whether to use VPC network
      lookupProjectLabels = dataprocConfig.projectVPCNetworkLabel.isDefined || dataprocConfig.projectVPCSubnetLabel.isDefined
      projectLabels <- if (lookupProjectLabels) IO.fromFuture(IO(googleProjectDAO.getLabels(cluster.googleProject.value))) else IO(Map.empty[String, String])
      clusterVPCSettings = getClusterVPCSettings(projectLabels)

      // Create the cluster
      createClusterConfig = CreateClusterConfig(machineConfig, initScript, cluster.serviceAccountInfo.clusterServiceAccount, credentialsFileName, stagingBucketName, cluster.scopes, clusterVPCSettings, cluster.properties)
      retryResult <- IO.fromFuture(IO(retryExponentially(whenGoogleZoneCapacityIssue, "Cluster creation failed because zone with adequate resources was not found") { () =>
        gdDAO.createCluster(cluster.googleProject, cluster.clusterName, createClusterConfig)
      }))


      operation <- retryResult match {
        case Right((errors, op)) if errors == List.empty => IO(op)
        case Right((errors, op)) =>
          Metrics.newRelic.incrementCounterIO("zoneCapacityClusterCreationFailure", errors.length)
            .as(op)
        case Left(errors) =>
          Metrics.newRelic.incrementCounterIO("zoneCapacityClusterCreationFailure", errors.filter(whenGoogleZoneCapacityIssue).length)
            .flatMap(_ => IO.raiseError(errors.head))
      }

      cluster = Cluster.createFinal(cluster, operation, stagingBucketName)
    } yield (cluster, initBucketName, serviceAccountKeyOpt)


    ioResult.handleErrorWith { throwable =>
      cleanUpGoogleResourcesOnError(cluster.googleProject,
        cluster.clusterName, initBucketName,
        cluster.serviceAccountInfo, serviceAccountKeyIO) >> IO.raiseError(throwable)
    }

  }

  private def firewallRule = FirewallRule(
    name = FirewallRuleName(dataprocConfig.firewallRuleName),
    protocol = FirewallRuleProtocol(proxyConfig.jupyterProtocol),
    ports = List(FirewallRulePort(proxyConfig.jupyterPort.toString)),
    network = dataprocConfig.vpcNetwork.map(VPCNetworkName),
    targetTags = List(NetworkTag(dataprocConfig.networkTag)))

  // See https://cloud.google.com/dataproc/docs/guides/dataproc-images#custom_image_uri
  private def parseImageProject(customDataprocImage: String): Option[GoogleProject] = {
    val regex = ".*projects/(.*)/global/images/(.*)".r
    customDataprocImage match {
      case regex(project, _) => Some(GoogleProject(project))
      case _ => None
    }
  }

  /* Process the templated cluster init script and put all initialization files in the init bucket */
  private def initializeBucketObjects(cluster: Cluster, initBucketName: GcsBucketName, serviceAccountKey: Option[ServiceAccountKey]): Stream[IO, Unit] = {
    // Build a mapping of (name, value) pairs with which to apply templating logic to resources
    val replacements = ClusterInitValues(cluster, initBucketName, serviceAccountKey, dataprocConfig, proxyConfig, clusterFilesConfig, clusterResourcesConfig, contentSecurityPolicy).toMap

    // Raw files to upload to the bucket, no additional processing needed.
    val rawFilesToUpload = Stream.emits(
      Seq(
        clusterFilesConfig.jupyterServerCrt,
        clusterFilesConfig.jupyterServerKey,
        clusterFilesConfig.jupyterRootCaPem
      )
    ).evalMap { f =>
      TemplateHelper.readFile[IO](f, executionContext)
    }

    val rawResourcesToUpload = Stream.emits(
      Seq(
        clusterResourcesConfig.jupyterDockerCompose,
        clusterResourcesConfig.rstudioDockerCompose,
        clusterResourcesConfig.proxyDockerCompose,
        clusterResourcesConfig.proxySiteConf,
        clusterResourcesConfig.welderDockerCompose,
        clusterResourcesConfig.initVmScript
      )
    ).evalMap { resource =>
      TemplateHelper.readResource(resource, executionContext)
    }

    val templatedResourcesToUpload = Stream.emits(
      Seq(
        clusterResourcesConfig.initActionsScript,
        clusterResourcesConfig.jupyterNotebookConfigUri,
        clusterResourcesConfig.jupyterNotebookFrontendConfigUri
      )
    ).evalMap { resource =>
      TemplateHelper.templateResource(replacements, resource, executionContext)
    }

    val privateKey = Stream.emit(
      for {
        k <- serviceAccountKey
        d <- k.privateKeyData.decode
      } yield GcsResource(GcsBlobName(ClusterInitValues.serviceAccountCredentialsFilename), d.getBytes(Charset.defaultCharset))
    ).unNone

    (rawFilesToUpload ++ rawResourcesToUpload ++ templatedResourcesToUpload ++ privateKey).evalMap[IO, Unit] { resource =>
      bucketHelper.storeObject(initBucketName, resource.gcsBlobName, resource.content, "text/plain")
    }
  }

}
