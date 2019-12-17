package org.broadinstitute.dsde.workbench.leonardo
package util

import java.nio.charset.StandardCharsets

import _root_.io.chrisdavenport.log4cats.Logger
import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import cats.effect._
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.google.api.services.admin.directory.model.Group
import com.typesafe.scalalogging.LazyLogging
import fs2._
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO.MemberType
import org.broadinstitute.dsde.workbench.google.GoogleUtilities.RetryPredicates._
import org.broadinstitute.dsde.workbench.google._
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.leonardo.config._
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

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

final case class ClusterIamSetupException(googleProject: GoogleProject)
    extends LeoException(s"Error occurred setting up IAM roles in project ${googleProject.value}")

final case class GoogleGroupCreationException(googleGroup: WorkbenchEmail, msg: String)
    extends LeoException(s"Failed to create the Google group '${googleGroup}': $msg", StatusCodes.InternalServerError)

final case object ImageProjectNotFoundException
    extends LeoException("Custom Dataproc image project not found", StatusCodes.NotFound)

class ClusterHelper(
  dbRef: DbReference,
  dataprocConfig: DataprocConfig,
  googleGroupsConfig: GoogleGroupsConfig,
  proxyConfig: ProxyConfig,
  clusterResourcesConfig: ClusterResourcesConfig,
  clusterFilesConfig: ClusterFilesConfig,
  monitorConfig: MonitorConfig,
  bucketHelper: BucketHelper,
  gdDAO: GoogleDataprocDAO,
  googleComputeDAO: GoogleComputeDAO,
  googleDirectoryDAO: GoogleDirectoryDAO,
  googleIamDAO: GoogleIamDAO,
  googleProjectDAO: GoogleProjectDAO,
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
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[CreateClusterResponse] = {
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

          // Add member to the Google Group that has the IAM role to pull the Dataproc image
          _ <- updateDataprocImageGroupMembership(cluster.googleProject, createCluster = true)

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

          _ <- initializeBucketObjects(cluster, initBucketName, stagingBucketName, serviceAccountKeyOpt).compile.drain

          // build cluster configuration
          machineConfig = cluster.machineConfig
          initScriptResources = List(clusterResourcesConfig.initActionsScript)
          initScripts = initScriptResources.map(resource => GcsPath(initBucketName, GcsObjectName(resource.value)))
          credentialsFileName = cluster.serviceAccountInfo.notebookServiceAccount
            .map(_ => s"/etc/${ClusterTemplateValues.serviceAccountCredentialsFilename}")

          // If user is using https://github.com/DataBiosphere/terra-docker/tree/master#terra-base-images for jupyter image, then
          // we will use the new custom dataproc image
          dataprocImage = if (cluster.clusterImages.exists(_.imageUrl == dataprocConfig.jupyterImage))
            dataprocConfig.legacyCustomDataprocImage
          else dataprocConfig.customDataprocImage

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
            dataprocImage,
            monitorConfig.monitorStatusTimeouts.getOrElse(ClusterStatus.Creating, 1 hour)
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

        } yield CreateClusterResponse(finalCluster, initBucketName, serviceAccountKeyOpt, dataprocImage)

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
      metadata <- getMasterInstanceShutdownScript(cluster)

      // First remove all its preemptible instances, if any
      _ <- if (cluster.machineConfig.numberOfPreemptibleWorkers.exists(_ > 0))
        IO.fromFuture(IO(gdDAO.resizeCluster(cluster.googleProject, cluster.clusterName, numPreemptibles = Some(0))))
      else IO.unit

      // Now stop each instance individually
      _ <- cluster.nonPreemptibleInstances.toList.parTraverse { instance =>
        IO.fromFuture(IO(instance.dataprocRole.traverse {
          case Master =>
            googleComputeDAO.addInstanceMetadata(instance.key, metadata) >>
              googleComputeDAO.stopInstance(instance.key)
          case _ =>
            googleComputeDAO.stopInstance(instance.key)
        }))
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

      _ <- updateDataprocImageGroupMembership(cluster.googleProject, createCluster = true)

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
    updateClusterIamRoles(googleProject, serviceAccountInfo, createCluster = true)

  def removeClusterIamRoles(googleProject: GoogleProject, serviceAccountInfo: ServiceAccountInfo): IO[Unit] =
    updateClusterIamRoles(googleProject, serviceAccountInfo, createCluster = false)

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

  def setupDataprocImageGoogleGroup(): IO[Unit] =
    createDataprocImageUserGoogleGroupIfItDoesntExist() >>
      addIamRoleToDataprocImageGroup(dataprocConfig.customDataprocImage)

  /**
   * Add the user's service account to the Google group.
   * This group has compute.imageUser role on the custom Dataproc image project,
   * which allows the user's cluster to pull the image.
   */
  def updateDataprocImageGroupMembership(googleProject: GoogleProject, createCluster: Boolean): IO[Unit] =
    parseImageProject(dataprocConfig.customDataprocImage).traverse_ { imageProject =>
      for {
        count <- dbRef.inTransactionIO { _.clusterQuery.countActiveByProject(googleProject) }
        // Note: Don't remove the account if there are existing active clusters in the same project,
        // because it could potentially break other clusters. We only check this for the 'remove' case.
        _ <- if (count > 0 && !createCluster) {
          IO.unit
        } else {
          for {
            projectNumberOptIO <- IO.fromFuture(IO(googleComputeDAO.getProjectNumber(googleProject)))
            projectNumber <- IO.fromEither(projectNumberOptIO.toRight(ClusterIamSetupException(imageProject)))
            // Note that the Dataproc service account is used to retrieve the image, and not the user's
            // pet service account. There is one Dataproc service account per Google project. For more details:
            // https://cloud.google.com/dataproc/docs/concepts/iam/iam#service_accounts
            dataprocServiceAccountEmail = WorkbenchEmail(
              s"service-${projectNumber}@dataproc-accounts.iam.gserviceaccount.com"
            )
            _ <- updateGroupMembership(googleGroupsConfig.dataprocImageProjectGroupEmail,
                                       dataprocServiceAccountEmail,
                                       createCluster)
          } yield ()
        }
      } yield ()
    }

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
    serviceAccountKey: Option[ServiceAccountKey]
  ): Stream[IO, Unit] = {
    // Build a mapping of (name, value) pairs with which to apply templating logic to resources
    val replacements = ClusterTemplateValues(
      cluster,
      Some(initBucketName),
      Some(stagingBucketName),
      serviceAccountKey,
      dataprocConfig,
      proxyConfig,
      clusterFilesConfig,
      clusterResourcesConfig
    ).toMap

    // Jupyter allows setting of arbitrary environment variables on cluster creation if they are passed in to
    // docker-compose as a file of format:
    //     var1=value1
    //     var2=value2
    // etc. We're building a string of that format here.
    val customEnvVars = cluster.customClusterEnvironmentVariables.foldLeft("")({
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
          clusterResourcesConfig.jupyterNotebookConfigUri
        )
      )
      bytes <- Stream.eval(TemplateHelper.resourceStream(r, blocker).compile.to[Array])
      _ <- bucketHelper.storeObject(initBucketName, GcsBlobName(r.value), bytes, "text/plain")
    } yield ()

    val uploadTemplatedResources = for {
      r <- Stream.emits(
        Seq(
          clusterResourcesConfig.initActionsScript,
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
                                    GcsBlobName(ClusterTemplateValues.serviceAccountCredentialsFilename),
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

  /**
   * Add the Dataproc Worker role in the user's project to the cluster service account, if present.
   * This is needed to be able to spin up Dataproc clusters using a custom service account.
   * If the Google Compute default service account is being used, this is not necessary.
   */
  private def updateClusterIamRoles(googleProject: GoogleProject,
                                    serviceAccountInfo: ServiceAccountInfo,
                                    createCluster: Boolean): IO[Unit] = {
    val retryIam: (GoogleProject, WorkbenchEmail, Set[String]) => IO[Unit] = (project, email, roles) =>
      IO.fromFuture[Unit](IO(retryExponentially(when409, s"IAM policy change failed for Google project '$project'") {
        () =>
          if (createCluster) {
            googleIamDAO.addIamRoles(project, email, MemberType.ServiceAccount, roles).void
          } else {
            googleIamDAO.removeIamRoles(project, email, MemberType.ServiceAccount, roles).void
          }
      }))

    serviceAccountInfo.clusterServiceAccount.traverse_ { email =>
      // Note: don't remove the role if there are existing active clusters owned by the same user,
      // because it could potentially break other clusters. We only check this for the 'remove' case,
      // it's ok to re-add the roles.
      IO.fromFuture(IO(dbRef.inTransaction { _.clusterQuery.countActiveByClusterServiceAccount(email) })).flatMap {
        count =>
          if (count > 0 && !createCluster) {
            IO.unit
          } else {
            retryIam(googleProject, email, Set("roles/dataproc.worker"))
          }
      }
    }
  }

  private def createDataprocImageUserGoogleGroupIfItDoesntExist(): IO[Unit] =
    for {
      _ <- log.debug(
        s"Checking if Dataproc image user Google group '${googleGroupsConfig.dataprocImageProjectGroupEmail}' already exists..."
      )

      groupOpt <- IO.fromFuture[Option[Group]](
        IO(googleDirectoryDAO.getGoogleGroup(googleGroupsConfig.dataprocImageProjectGroupEmail))
      )
      _ <- groupOpt.fold(
        log.debug(
          s"Dataproc image user Google group '${googleGroupsConfig.dataprocImageProjectGroupEmail}' does not exist. Attempting to create it..."
        ) >> createDataprocImageUserGoogleGroup()
      )(
        group =>
          log.debug(
            s"Dataproc image user Google group '${googleGroupsConfig.dataprocImageProjectGroupEmail}' already exists: $group \n Won't attempt to create it."
          )
      )
    } yield ()

  private def createDataprocImageUserGoogleGroup(): IO[Unit] =
    IO.fromFuture(
        IO(
          googleDirectoryDAO
            .createGroup(googleGroupsConfig.dataprocImageProjectGroupName,
                         googleGroupsConfig.dataprocImageProjectGroupEmail,
                         Option(googleDirectoryDAO.lockedDownGroupSettings))
        )
      )
      .handleErrorWith {
        case t if when409(t) => IO.unit
        case t =>
          IO.raiseError(GoogleGroupCreationException(googleGroupsConfig.dataprocImageProjectGroupEmail, t.getMessage))
      }

  private def addIamRoleToDataprocImageGroup(customDataprocImage: CustomDataprocImage): IO[Unit] = {
    val computeImageUserRole = Set("roles/compute.imageUser")
    parseImageProject(dataprocConfig.customDataprocImage).fold(
      IO.raiseError[Unit](ImageProjectNotFoundException)
    ) { imageProject =>
      for {
        _ <- log.debug(
          s"Attempting to grant 'compute.imageUser' permissions to '${googleGroupsConfig.dataprocImageProjectGroupEmail}' on project '$imageProject' ..."
        )
        _ <- IO.fromFuture[Boolean](
          IO(
            retryExponentially(
              when409,
              s"IAM policy change failed for '${googleGroupsConfig.dataprocImageProjectGroupEmail}' on Google project '$imageProject'."
            ) { () =>
              googleIamDAO.addIamRoles(imageProject,
                                       googleGroupsConfig.dataprocImageProjectGroupEmail,
                                       MemberType.Group,
                                       computeImageUserRole)
            }
          )
        )
      } yield ()
    }
  }

  private def updateGroupMembership(groupEmail: WorkbenchEmail,
                                    memberEmail: WorkbenchEmail,
                                    addToGroup: Boolean): IO[Unit] =
    IO.fromFuture[Unit] {
      IO {
        retryExponentially(when409, s"Could not update group '$groupEmail' for member '$memberEmail'") { () =>
          logger.debug(s"Checking if '$memberEmail' is part of group '$groupEmail'...")
          googleDirectoryDAO.isGroupMember(groupEmail, memberEmail).flatMap {
            case false if (addToGroup) =>
              logger.debug(s"Adding '$memberEmail' to group '$groupEmail'...")
              googleDirectoryDAO.addMemberToGroup(groupEmail, memberEmail)
            case true if (!addToGroup) =>
              logger.debug(s"Removing '$memberEmail' from group '$groupEmail'...")
              googleDirectoryDAO.removeMemberFromGroup(groupEmail, memberEmail)
            case _ => Future.unit
          }
        }
      }
    }

  // See https://cloud.google.com/dataproc/docs/guides/dataproc-images#custom_image_uri
  private def parseImageProject(customDataprocImage: CustomDataprocImage): Option[GoogleProject] = {
    val regex = ".*projects/(.*)/global/images/(.*)".r
    customDataprocImage.asString match {
      case regex(project, _) => Some(GoogleProject(project))
      case _                 => None
    }
  }

  // Startup script to install on the cluster master node. This allows Jupyter to start back up after
  // a cluster is resumed.
  private def getMasterInstanceStartupScript(cluster: Cluster, welderAction: WelderAction): IO[Map[String, String]] = {
    val googleKey = "startup-script" // required; see https://cloud.google.com/compute/docs/startupscript

    val clusterInit = ClusterTemplateValues(
      cluster,
      None,
      cluster.dataprocInfo.map(_.stagingBucket),
      None,
      dataprocConfig,
      proxyConfig,
      clusterFilesConfig,
      clusterResourcesConfig
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

  private def getMasterInstanceShutdownScript(cluster: Cluster): IO[Map[String, String]] = {
    val googleKey = "shutdown-script" // required; see https://cloud.google.com/compute/docs/shutdownscript

    val replacements = ClusterTemplateValues(
      cluster,
      None,
      cluster.dataprocInfo.map(_.stagingBucket),
      None,
      dataprocConfig,
      proxyConfig,
      clusterFilesConfig,
      clusterResourcesConfig
    ).toMap

    TemplateHelper
      .templateResource(replacements, clusterResourcesConfig.shutdownScript, blocker)
      .compile
      .to[Array]
      .map { bytes =>
        Map(googleKey -> new String(bytes, StandardCharsets.UTF_8))
      }
  }

}

final case class CreateClusterResponse(cluster: Cluster,
                                       initBucket: GcsBucketName,
                                       serviceAccountKey: Option[ServiceAccountKey],
                                       customDataprocImage: CustomDataprocImage)
