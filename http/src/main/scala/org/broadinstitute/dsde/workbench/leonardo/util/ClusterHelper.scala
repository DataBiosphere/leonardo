package org.broadinstitute.dsde.workbench.leonardo
package util

import java.nio.charset.StandardCharsets

import _root_.io.chrisdavenport.log4cats.Logger
import akka.actor.ActorSystem
import cats.effect._
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.typesafe.scalalogging.LazyLogging
import fs2._
import org.broadinstitute.dsde.workbench.google.GoogleUtilities.RetryPredicates._
import org.broadinstitute.dsde.workbench.google._
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.leonardo.config.{
  ClusterFilesConfig,
  ClusterResourcesConfig,
  DataprocConfig,
  ProxyConfig
}
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{GoogleComputeDAO, GoogleDataprocDAO, _}
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.WelderAction.{DeployWelder, DisableDelocalization, UpdateWelder}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google.DataprocRole.Master
import org.broadinstitute.dsde.workbench.leonardo.model.google.VPCConfig._
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.newrelic.NewRelicMetrics
import org.broadinstitute.dsde.workbench.util.Retry

import scala.concurrent.ExecutionContext

case class ClusterIamSetupException(googleProject: GoogleProject)
    extends LeoException(s"Error occurred setting up IAM roles in project ${googleProject.value}")

class ClusterHelper(
  dbRef: DbReference,
  dataprocConfig: DataprocConfig,
  proxyConfig: ProxyConfig,
  clusterResourcesConfig: ClusterResourcesConfig,
  clusterFilesConfig: ClusterFilesConfig,
  bucketHelper: BucketHelper,
  gdDAO: GoogleDataprocDAO,
  googleComputeDAO: GoogleComputeDAO,
  googleIamDAO: GoogleIamDAO,
  googleProjectDAO: GoogleProjectDAO,
  contentSecurityPolicy: String,
  blocker: Blocker
)(implicit val executionContext: ExecutionContext,
  val system: ActorSystem,
  contextShift: ContextShift[IO],
  log: Logger[IO],
  metrics: NewRelicMetrics[IO])
    extends LazyLogging
    with Retry {

  def createCluster(
    cluster: Cluster
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[(Cluster, GcsBucketName, Option[ServiceAccountKey])] = {
    val initBucketName = generateUniqueBucketName("leoinit-" + cluster.clusterName.value)
    val stagingBucketName = generateUniqueBucketName("leostaging-" + cluster.clusterName.value)

    // Generate a service account key for the notebook service account (if present) to localize on the cluster.
    // We don't need to do this for the cluster service account because its credentials are already
    // on the metadata server.
    generateServiceAccountKey(cluster.googleProject, cluster.serviceAccountInfo.notebookServiceAccount).flatMap {
      serviceAccountKeyOpt =>
        val ioResult = for {
          // get VPC settings
          projectLabels <- if (dataprocConfig.projectVPCNetworkLabel.isDefined || dataprocConfig.projectVPCSubnetLabel.isDefined) {
            IO.fromFuture(IO(googleProjectDAO.getLabels(cluster.googleProject.value)))
          } else IO.pure(Map.empty[String, String])
          clusterVPCSettings = getClusterVPCSettings(projectLabels)

          // Create the firewall rule in the google project if it doesn't already exist, so we can access the cluster
          _ <- IO.fromFuture(
            IO(googleComputeDAO.updateFirewallRule(cluster.googleProject, getFirewallRule(clusterVPCSettings)))
          )

          // Set up IAM roles necessary to create a cluster.
          _ <- createClusterIamRoles(cluster.googleProject, cluster.serviceAccountInfo)

          // Create the bucket in the cluster's google project and populate with initialization files.
          // ACLs are granted so the cluster service account can access the files at initialization time.
          _ <- bucketHelper
            .createInitBucket(cluster.googleProject, initBucketName, cluster.serviceAccountInfo)
            .compile
            .drain

          // Create the cluster staging bucket. ACLs are granted so the user/pet can access it.
          _ <- bucketHelper
            .createStagingBucket(cluster.auditInfo.creator,
                                 cluster.googleProject,
                                 stagingBucketName,
                                 cluster.serviceAccountInfo)
            .compile
            .drain

          _ <- initializeBucketObjects(cluster, initBucketName, stagingBucketName, serviceAccountKeyOpt, Map.empty).compile.drain

          // build cluster configuration
          machineConfig = cluster.machineConfig
          initScriptResources = if (dataprocConfig.customDataprocImage.isEmpty)
            List(clusterResourcesConfig.initVmScript, clusterResourcesConfig.initActionsScript)
          else List(clusterResourcesConfig.initActionsScript)
          initScripts = initScriptResources.map(resource => GcsPath(initBucketName, GcsObjectName(resource.value)))
          credentialsFileName = cluster.serviceAccountInfo.notebookServiceAccount
            .map(_ => s"/etc/${ClusterInitValues.serviceAccountCredentialsFilename}")

          // Create the cluster
          createClusterConfig = CreateClusterConfig(
            machineConfig,
            initScripts,
            cluster.serviceAccountInfo.clusterServiceAccount,
            credentialsFileName,
            stagingBucketName,
            cluster.scopes,
            clusterVPCSettings,
            cluster.properties,
            dataprocConfig.customDataprocImage
          )
          retryResult <- IO.fromFuture(
            IO(
              retryExponentially(whenGoogleZoneCapacityIssue,
                                 "Cluster creation failed because zone with adequate resources was not found") { () =>
                gdDAO.createCluster(cluster.googleProject, cluster.clusterName, createClusterConfig)
              }
            )
          )

          operation <- retryResult match {
            case Right((errors, op)) if errors == List.empty => IO(op)
            case Right((errors, op)) =>
              metrics
                .incrementCounter("zoneCapacityClusterCreationFailure", errors.length)
                .as(op)
            case Left(errors) =>
              metrics
                .incrementCounter("zoneCapacityClusterCreationFailure",
                                  errors.filter(whenGoogleZoneCapacityIssue).length)
                .flatMap(_ => IO.raiseError(errors.head))
          }

          finalCluster = Cluster.addDataprocFields(cluster, operation, stagingBucketName)

        } yield (finalCluster, initBucketName, serviceAccountKeyOpt)

        ioResult.handleErrorWith { throwable =>
          cleanUpGoogleResourcesOnError(cluster.googleProject,
                                        cluster.clusterName,
                                        initBucketName,
                                        cluster.serviceAccountInfo,
                                        serviceAccountKeyOpt) >> IO.raiseError(throwable)
        }
    }
  }

  def deleteCluster(cluster: Cluster): IO[Unit] =
    IO.fromFuture(IO(gdDAO.deleteCluster(cluster.googleProject, cluster.clusterName)))

  def stopCluster(cluster: Cluster): IO[Unit] =
    for {
      // First remove all its preemptible instances, if any
      _ <- if (cluster.machineConfig.numberOfPreemptibleWorkers.exists(_ > 0))
        IO.fromFuture(IO(gdDAO.resizeCluster(cluster.googleProject, cluster.clusterName, numPreemptibles = Some(0))))
      else IO.unit

      // Now stop each instance individually
      _ <- cluster.nonPreemptibleInstances.toList.parTraverse { instance =>
        IO.fromFuture(IO(googleComputeDAO.stopInstance(instance.key)))
      }
    } yield ()

  def startCluster(cluster: Cluster, welderAction: WelderAction): IO[Unit] =
    for {
      metadata <- getMasterInstanceStartupScript(cluster, welderAction)

      // Add back the preemptible instances, if any
      _ <- if (cluster.machineConfig.numberOfPreemptibleWorkers.exists(_ > 0))
        IO.fromFuture(
          IO(
            gdDAO.resizeCluster(cluster.googleProject,
                                cluster.clusterName,
                                numPreemptibles = cluster.machineConfig.numberOfPreemptibleWorkers)
          )
        )
      else IO.unit

      // Start each instance individually
      _ <- cluster.nonPreemptibleInstances.toList.parTraverse { instance =>
        // Install a startup script on the master node so Jupyter starts back up again once the instance is restarted
        IO.fromFuture(IO(instance.dataprocRole.traverse {
          case Master =>
            googleComputeDAO.addInstanceMetadata(instance.key, metadata) >>
              googleComputeDAO.startInstance(instance.key)
          case _ =>
            googleComputeDAO.startInstance(instance.key)
        }))
      }

    } yield ()

  def resizeCluster(cluster: Cluster, numWorkers: Option[Int], numPreemptibles: Option[Int]): IO[Unit] =
    for {
      // IAM roles should already exist for a non-deleted cluster; this method is a no-op if the roles already exist.
      _ <- createClusterIamRoles(cluster.googleProject, cluster.serviceAccountInfo)

      // Resize the cluster in Google
      _ <- IO.fromFuture(
        IO(gdDAO.resizeCluster(cluster.googleProject, cluster.clusterName, numWorkers, numPreemptibles))
      )
    } yield ()

  def setMasterMachineType(cluster: Cluster, machineType: MachineType): IO[Unit] =
    cluster.instances.toList.traverse_ { instance =>
      // Note: we don't support changing the machine type for worker instances. While this is possible
      // in GCP, Spark settings are auto-tuned to machine size. Dataproc recommends adding or removing nodes,
      // and rebuilding the cluster if new worker machine/disk sizes are needed.
      instance.dataprocRole.traverse_ {
        case Master => IO.fromFuture(IO(googleComputeDAO.setMachineType(instance.key, machineType)))
        case _      => IO.unit
      }
    }

  def updateMasterDiskSize(cluster: Cluster, diskSize: Int): IO[Unit] =
    cluster.instances.toList.traverse_ { instance =>
      // Note: we don't support changing the machine type for worker instances. While this is possible
      // in GCP, Spark settings are auto-tuned to machine size. Dataproc recommends adding or removing nodes,
      // and rebuilding the cluster if new worker machine/disk sizes are needed.
      instance.dataprocRole.traverse_ {
        case Master => IO.fromFuture(IO(googleComputeDAO.resizeDisk(instance.key, diskSize)))
        case _      => IO.unit
      }
    }

  def createClusterIamRoles(googleProject: GoogleProject, serviceAccountInfo: ServiceAccountInfo): IO[Unit] =
    updateClusterIamRoles(googleProject, serviceAccountInfo, true)

  def removeClusterIamRoles(googleProject: GoogleProject, serviceAccountInfo: ServiceAccountInfo): IO[Unit] =
    updateClusterIamRoles(googleProject, serviceAccountInfo, false)

  def generateServiceAccountKey(googleProject: GoogleProject,
                                serviceAccountEmailOpt: Option[WorkbenchEmail]): IO[Option[ServiceAccountKey]] =
    serviceAccountEmailOpt.traverse { email =>
      IO.fromFuture(IO(googleIamDAO.createServiceAccountKey(googleProject, email)))
    }

  def removeServiceAccountKey(googleProject: GoogleProject,
                              serviceAccountEmailOpt: Option[WorkbenchEmail],
                              serviceAccountKeyIdOpt: Option[ServiceAccountKeyId]): IO[Unit] =
    (serviceAccountEmailOpt, serviceAccountKeyIdOpt).mapN {
      case (email, keyId) =>
        IO.fromFuture(IO(googleIamDAO.removeServiceAccountKey(googleProject, email, keyId)))
    } getOrElse IO.unit

  private def cleanUpGoogleResourcesOnError(googleProject: GoogleProject,
                                            clusterName: ClusterName,
                                            initBucketName: GcsBucketName,
                                            serviceAccountInfo: ServiceAccountInfo,
                                            serviceAccountKeyOpt: Option[ServiceAccountKey]): IO[Unit] = {
    // Clean up resources in Google
    val deleteBucket = bucketHelper.deleteInitBucket(initBucketName).attempt.flatMap {
      case Left(e) =>
        log.error(e)(
          s"Failed to delete init bucket ${initBucketName.value} for ${googleProject.value} / ${clusterName.value}"
        )
      case _ =>
        log.info(
          s"Successfully deleted init bucket ${initBucketName.value} for ${googleProject.value} / ${clusterName.value}"
        )
    }

    // Don't delete the staging bucket so the user can see error logs.

    val deleteCluster = IO.fromFuture(IO(gdDAO.deleteCluster(googleProject, clusterName))).attempt.flatMap {
      case Left(e) => log.error(e)(s"Failed to delete cluster ${googleProject.value} / ${clusterName.value}")
      case _       => log.info(s"Successfully deleted cluster ${googleProject.value} / ${clusterName.value}")
    }

    val deleteServiceAccountKey = removeServiceAccountKey(googleProject,
                                                          serviceAccountInfo.notebookServiceAccount,
                                                          serviceAccountKeyOpt.map(_.id)).attempt.flatMap {
      case Left(e) =>
        log.error(e)(s"Failed to delete service account key for ${serviceAccountInfo.notebookServiceAccount}")
      case _ => log.info(s"Successfully deleted service account key for ${serviceAccountInfo.notebookServiceAccount}")
    }

    val removeIamRoles = removeClusterIamRoles(googleProject, serviceAccountInfo).attempt.flatMap {
      case Left(e) => log.error(e)(s"Failed to remove IAM roles for ${googleProject.value} / ${clusterName.value}")
      case _       => log.info(s"Successfully removed IAM roles for ${googleProject.value} / ${clusterName.value}")
    }

    List(deleteBucket, deleteCluster, deleteServiceAccountKey, removeIamRoles).parSequence_
  }

  private[leonardo] def getClusterVPCSettings(projectLabels: Map[String, String]): Option[VPCConfig] = {
    //Dataproc only allows you to specify a subnet OR a network. Subnets will be preferred if present.
    //High-security networks specified inside of the project will always take precedence over anything
    //else. Thus, VPC configuration takes the following precedence:
    // 1) High-security subnet in the project (if present)
    // 2) High-security network in the project (if present)
    // 3) Subnet specified in leonardo.conf (if present)
    // 4) Network specified in leonardo.conf (if present)
    // 5) The default network in the project
    val projectSubnet = dataprocConfig.projectVPCSubnetLabel.flatMap(subnetLabel => projectLabels.get(subnetLabel))
    val projectNetwork = dataprocConfig.projectVPCNetworkLabel.flatMap(networkLabel => projectLabels.get(networkLabel))
    val configSubnet = dataprocConfig.vpcSubnet
    val configNetwork = dataprocConfig.vpcNetwork

    (projectSubnet, projectNetwork, configSubnet, configNetwork) match {
      case (Some(subnet), _, _, _)  => Some(VPCSubnet(subnet))
      case (_, Some(network), _, _) => Some(VPCNetwork(network))
      case (_, _, Some(subnet), _)  => Some(VPCSubnet(subnet))
      case (_, _, _, Some(network)) => Some(VPCNetwork(network))
      case (_, _, _, _)             => None
    }
  }

  /* Process the templated cluster init script and put all initialization files in the init bucket */
  private[leonardo] def initializeBucketObjects(
    cluster: Cluster,
    initBucketName: GcsBucketName,
    stagingBucketName: GcsBucketName,
    serviceAccountKey: Option[ServiceAccountKey],
    customClusterEnvironmentVariables: Map[String, String]
  ): Stream[IO, Unit] = {
    // Build a mapping of (name, value) pairs with which to apply templating logic to resources
    val replacements = ClusterInitValues(cluster,
                                         initBucketName,
                                         stagingBucketName,
                                         serviceAccountKey,
                                         dataprocConfig,
                                         proxyConfig,
                                         clusterFilesConfig,
                                         clusterResourcesConfig,
                                         contentSecurityPolicy).toMap

    // Jupyter allows setting of arbitrary environment variables on cluster creation if they are passed in to
    // docker-compose as a file of format:
    //     var1=value1
    //     var2=value2
    // etc. We're building a string of that format here.
    val customEnvVars = customClusterEnvironmentVariables.foldLeft("")({
      case (memo, (key, value)) => memo + s"$key=$value\n"
    })

    val uploadRawFiles = for {
      f <- Stream.emits(
        Seq(
          clusterFilesConfig.jupyterServerCrt,
          clusterFilesConfig.jupyterServerKey,
          clusterFilesConfig.jupyterRootCaPem
        )
      )
      bytes <- Stream.eval(TemplateHelper.fileStream(f, blocker).compile.to[Array])
      _ <- bucketHelper.storeObject(initBucketName, GcsBlobName(f.getName), bytes, "text/plain")
    } yield ()

    val uploadRawResources = for {
      r <- Stream.emits(
        Seq(
          clusterResourcesConfig.jupyterDockerCompose,
          clusterResourcesConfig.rstudioDockerCompose,
          clusterResourcesConfig.proxyDockerCompose,
          clusterResourcesConfig.proxySiteConf,
          clusterResourcesConfig.welderDockerCompose,
          clusterResourcesConfig.initVmScript
        )
      )
      bytes <- Stream.eval(TemplateHelper.resourceStream(r, blocker).compile.to[Array])
      _ <- bucketHelper.storeObject(initBucketName, GcsBlobName(r.value), bytes, "text/plain")
    } yield ()

    val uploadTemplatedResources = for {
      r <- Stream.emits(
        Seq(
          clusterResourcesConfig.initActionsScript,
          clusterResourcesConfig.jupyterNotebookConfigUri,
          clusterResourcesConfig.jupyterNotebookFrontendConfigUri
        )
      )
      bytes <- Stream.eval(TemplateHelper.templateResource(replacements, r, blocker).compile.to[Array])
      _ <- bucketHelper.storeObject(initBucketName, GcsBlobName(r.value), bytes, "text/plain")
    } yield ()

    val uploadPrivateKey = for {
      k <- Stream(serviceAccountKey).unNone
      data <- Stream(k.privateKeyData.decode).unNone
      _ <- bucketHelper.storeObject(initBucketName,
                                    GcsBlobName(ClusterInitValues.serviceAccountCredentialsFilename),
                                    data.getBytes(StandardCharsets.UTF_8),
                                    "text/plain")
    } yield ()

    val uploadCustomEnvVars = bucketHelper.storeObject(initBucketName,
                                                       GcsBlobName(clusterResourcesConfig.customEnvVarsConfigUri.value),
                                                       customEnvVars.getBytes(StandardCharsets.UTF_8),
                                                       "text/plain")

    Stream(uploadRawFiles, uploadRawResources, uploadTemplatedResources, uploadPrivateKey, uploadCustomEnvVars).parJoin(
      5
    )
  }

  private def getFirewallRule(vpcConfig: Option[VPCConfig]) =
    FirewallRule(
      name = FirewallRuleName(dataprocConfig.firewallRuleName),
      protocol = FirewallRuleProtocol(proxyConfig.jupyterProtocol),
      ports = List(FirewallRulePort(proxyConfig.jupyterPort.toString)),
      network = vpcConfig,
      targetTags = List(NetworkTag(dataprocConfig.networkTag))
    )

  private def updateClusterIamRoles(googleProject: GoogleProject,
                                    serviceAccountInfo: ServiceAccountInfo,
                                    create: Boolean): IO[Unit] = {
    val retryIam: (GoogleProject, WorkbenchEmail, Set[String]) => IO[Boolean] = (project, email, roles) =>
      IO.fromFuture[Boolean](IO(retryExponentially(when409, s"IAM policy change failed for Google project '$project'") {
        () =>
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
      IO.fromFuture(IO(dbRef.inTransaction { _.clusterQuery.countActiveByClusterServiceAccount(email) })).flatMap {
        count =>
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
      case None                                                => IO.unit
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
              projectNumber <- IO
                .fromFuture(IO(googleComputeDAO.getProjectNumber(googleProject)))
                .flatMap(_.fold(IO.raiseError[Long](ClusterIamSetupException(imageProject)))(IO.pure))
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

  // See https://cloud.google.com/dataproc/docs/guides/dataproc-images#custom_image_uri
  private def parseImageProject(customDataprocImage: String): Option[GoogleProject] = {
    val regex = ".*projects/(.*)/global/images/(.*)".r
    customDataprocImage match {
      case regex(project, _) => Some(GoogleProject(project))
      case _                 => None
    }
  }

  // Startup script to install on the cluster master node. This allows Jupyter to start back up after
  // a cluster is resumed.
  private def getMasterInstanceStartupScript(cluster: Cluster, welderAction: WelderAction): IO[Map[String, String]] = {
    val googleKey = "startup-script" // required; see https://cloud.google.com/compute/docs/startupscript

    // These things need to be provided to ClusterInitValues, but aren't actually needed for the startup script
    val dummyInitBucket = GcsBucketName("dummy-init-bucket")

    val clusterInit = ClusterInitValues(
      cluster,
      dummyInitBucket,
      None,
      dataprocConfig,
      proxyConfig,
      clusterFilesConfig,
      clusterResourcesConfig,
      contentSecurityPolicy
    )
    val replacements: Map[String, String] = clusterInit.toMap ++
      Map(
        "deployWelder" -> (welderAction == DeployWelder).toString,
        "updateWelder" -> (welderAction == UpdateWelder).toString,
        "disableDelocalization" -> (welderAction == DisableDelocalization).toString
      )

    TemplateHelper.templateResource(replacements, clusterResourcesConfig.startupScript, blocker).compile.to[Array].map {
      bytes =>
        Map(googleKey -> new String(bytes, StandardCharsets.UTF_8))
    }
  }

}
