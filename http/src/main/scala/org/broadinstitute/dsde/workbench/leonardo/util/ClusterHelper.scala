package org.broadinstitute.dsde.workbench.leonardo
package util

import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat
import java.time.Instant

import _root_.io.chrisdavenport.log4cats.Logger
import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import cats.data.OptionT
import cats.effect._
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.google.api.services.admin.directory.model.Group
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO.MemberType
import org.broadinstitute.dsde.workbench.google.GoogleUtilities.RetryPredicates._
import org.broadinstitute.dsde.workbench.google._
import org.broadinstitute.dsde.workbench.google2.{DiskName, GoogleComputeService, MachineTypeName, RegionName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.DataprocRole.Master
import org.broadinstitute.dsde.workbench.leonardo.RuntimeImageType.Welder
import org.broadinstitute.dsde.workbench.leonardo.WelderAction._
import org.broadinstitute.dsde.workbench.leonardo.config._
import org.broadinstitute.dsde.workbench.leonardo.dao.WelderDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.google._
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.service.{
  RuntimeCannotBeStartedException,
  RuntimeCannotBeStoppedException,
  RuntimeOutOfDateException
}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.CreateCluster
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.newrelic.NewRelicMetrics
import org.broadinstitute.dsde.workbench.util.Retry
import sun.reflect.generics.reflectiveObjects.NotImplementedException

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

final case class ClusterIamSetupException(googleProject: GoogleProject)
    extends LeoException(s"Error occurred setting up IAM roles in project ${googleProject.value}")

final case class GoogleGroupCreationException(googleGroup: WorkbenchEmail, msg: String)
    extends LeoException(s"Failed to create the Google group '${googleGroup}': $msg", StatusCodes.InternalServerError)

final case object ImageProjectNotFoundException
    extends LeoException("Custom Dataproc image project not found", StatusCodes.NotFound)

final case class ClusterResourceConstaintsException(clusterProjectAndName: RuntimeProjectAndName,
                                                    machineType: MachineTypeName)
    extends LeoException(
      s"Unable to calculate memory constraints for cluster ${clusterProjectAndName.googleProject}/${clusterProjectAndName.runtimeName} with master machine type ${machineType}"
    )

class ClusterHelper(
  dataprocConfig: DataprocConfig,
  imageConfig: ImageConfig,
  googleGroupsConfig: GoogleGroupsConfig,
  proxyConfig: ProxyConfig,
  clusterResourcesConfig: ClusterResourcesConfig,
  clusterFilesConfig: ClusterFilesConfig,
  monitorConfig: MonitorConfig,
  welderConfig: WelderConfig,
  bucketHelper: BucketHelper,
  vpcHelper: VPCHelper,
  gdDAO: GoogleDataprocDAO,
  googleComputeService: GoogleComputeService[IO],
  googleDirectoryDAO: GoogleDirectoryDAO,
  googleIamDAO: GoogleIamDAO,
  googleProjectDAO: GoogleProjectDAO,
  welderDao: WelderDAO[IO],
  blocker: Blocker
)(implicit val executionContext: ExecutionContext,
  val system: ActorSystem,
  contextShift: ContextShift[IO],
  log: Logger[IO],
  metrics: NewRelicMetrics[IO],
  dbRef: DbReference[IO])
    extends LazyLogging
    with Retry {

  import dbRef._

  def createCluster(
    params: CreateCluster
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[CreateClusterResponse] = {
    val initBucketName = generateUniqueBucketName("leoinit-" + params.clusterProjectAndName.runtimeName.asString)
    val stagingBucketName = generateUniqueBucketName("leostaging-" + params.clusterProjectAndName.runtimeName.asString)

    // Generate a service account key for the notebook service account (if present) to localize on the cluster.
    // We don't need to do this for the cluster service account because its credentials are already
    // on the metadata server.
    generateServiceAccountKey(params.clusterProjectAndName.googleProject,
                              params.serviceAccountInfo.notebookServiceAccount).flatMap { serviceAccountKeyOpt =>
      val ioResult = for {
        // Set up VPC network and firewall
        vpcSettings <- vpcHelper.getOrCreateVPCSettings(params.clusterProjectAndName.googleProject)
        firewallRule <- vpcHelper.getOrCreateFirewallRule(params.clusterProjectAndName.googleProject, vpcSettings)

        resourceConstraints <- getClusterResourceContraints(params.clusterProjectAndName,
                                                            params.runtimeConfig.machineType)

        // Set up IAM roles necessary to create a cluster.
        _ <- createClusterIamRoles(params.clusterProjectAndName.googleProject, params.serviceAccountInfo)

        // Add member to the Google Group that has the IAM role to pull the Dataproc image
        _ <- updateDataprocImageGroupMembership(params.clusterProjectAndName.googleProject, createCluster = true)

        // Create the bucket in the cluster's google project and populate with initialization files.
        // ACLs are granted so the cluster service account can access the files at initialization time.
        _ <- bucketHelper
          .createInitBucket(params.clusterProjectAndName.googleProject, initBucketName, params.serviceAccountInfo)
          .compile
          .drain

        // Create the cluster staging bucket. ACLs are granted so the user/pet can access it.
        _ <- bucketHelper
          .createStagingBucket(params.auditInfo.creator,
                               params.clusterProjectAndName.googleProject,
                               stagingBucketName,
                               params.serviceAccountInfo)
          .compile
          .drain

        templateParams = RuntimeTemplateValuesConfig.fromCreateCluster(
          params,
          Some(initBucketName),
          Some(stagingBucketName),
          serviceAccountKeyOpt,
          dataprocConfig,
          imageConfig,
          welderConfig,
          proxyConfig,
          clusterFilesConfig,
          clusterResourcesConfig,
          Some(resourceConstraints)
        )
        _ <- bucketHelper
          .initializeBucketObjects(initBucketName, templateParams, params.customClusterEnvironmentVariables)
          .compile
          .drain

        // build cluster configuration
        initScriptResources = List(clusterResourcesConfig.initActionsScript)
        initScripts = initScriptResources.map(resource => GcsPath(initBucketName, GcsObjectName(resource.asString)))
        credentialsFileName = params.serviceAccountInfo.notebookServiceAccount
          .map(_ => s"/etc/${RuntimeTemplateValues.serviceAccountCredentialsFilename}")

        // If user is using https://github.com/DataBiosphere/terra-docker/tree/master#terra-base-images for jupyter image, then
        // we will use the new custom dataproc image
        dataprocImage = if (params.runtimeImages.exists(_.imageUrl == imageConfig.legacyJupyterImage))
          dataprocConfig.legacyCustomDataprocImage
        else dataprocConfig.customDataprocImage

        res <- params.runtimeConfig match {
          case _: RuntimeConfig.GceConfig => IO.raiseError(new NotImplementedException)
          case x: RuntimeConfig.DataprocConfig =>
            val createClusterConfig = CreateClusterConfig(
              x,
              initScripts,
              params.serviceAccountInfo.clusterServiceAccount,
              credentialsFileName,
              stagingBucketName,
              params.scopes,
              Some(vpcSettings),
              params.properties,
              dataprocImage,
              monitorConfig.monitorStatusTimeouts.getOrElse(RuntimeStatus.Creating, 1 hour)
            )
            for { // Create the cluster
              retryResult <- IO.fromFuture(
                IO(
                  retryExponentially(whenGoogleZoneCapacityIssue,
                                     "Cluster creation failed because zone with adequate resources was not found") {
                    () =>
                      gdDAO.createCluster(params.clusterProjectAndName.googleProject,
                                          params.clusterProjectAndName.runtimeName,
                                          createClusterConfig)
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

              asyncRuntimeFields = AsyncRuntimeFields(operation.uuid, operation.name, stagingBucketName, None)
            } yield CreateClusterResponse(asyncRuntimeFields, initBucketName, serviceAccountKeyOpt, dataprocImage)
        }
      } yield res

      ioResult.handleErrorWith { throwable =>
        cleanUpGoogleResourcesOnError(params.clusterProjectAndName.googleProject,
                                      params.clusterProjectAndName.runtimeName,
                                      initBucketName,
                                      params.serviceAccountInfo,
                                      serviceAccountKeyOpt) >> IO.raiseError(throwable)
      }
    }
  }

  def deleteCluster(cluster: Cluster): IO[Unit] =
    IO.fromFuture(IO(gdDAO.deleteCluster(cluster.googleProject, cluster.runtimeName)))

  def stopCluster(cluster: Cluster, runtimeConfig: RuntimeConfig)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    if (cluster.status.isStoppable) {
      for {
        // Flush the welder cache to disk
        _ <- if (cluster.welderEnabled) {
          welderDao
            .flushCache(cluster.googleProject, cluster.runtimeName)
            .handleErrorWith(e => log.error(e)(s"Failed to flush welder cache for ${cluster}"))
        } else IO.unit

        // Stop the cluster in Google
        _ <- stopGoogleCluster(cluster, runtimeConfig)

        // Update the cluster status to Stopping
        now <- IO(Instant.now)
        _ <- dbRef.inTransaction { clusterQuery.setToStopping(cluster.id, now) }
      } yield ()

    } else IO.raiseError(RuntimeCannotBeStoppedException(cluster.googleProject, cluster.runtimeName, cluster.status))

  private def stopGoogleCluster(cluster: Cluster,
                                runtimeConfig: RuntimeConfig)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    for {
      metadata <- getMasterInstanceShutdownScript(cluster)

      // First remove all its preemptible instances, if any
      _ <- runtimeConfig match {
        case x: RuntimeConfig.DataprocConfig if x.numberOfPreemptibleWorkers.exists(_ > 0) =>
          IO.fromFuture(IO(gdDAO.resizeCluster(cluster.googleProject, cluster.runtimeName, numPreemptibles = Some(0))))
        case _ => IO.unit
      }

      // Now stop each instance individually
      _ <- cluster.nonPreemptibleInstances.toList.parTraverse { instance =>
        instance.dataprocRole match {
          case Master =>
            googleComputeService.addInstanceMetadata(
              instance.key.project,
              instance.key.zone,
              instance.key.name,
              metadata
            ) >> googleComputeService.stopInstance(instance.key.project, instance.key.zone, instance.key.name)
          case _ =>
            googleComputeService.stopInstance(instance.key.project, instance.key.zone, instance.key.name)
        }
      }
    } yield ()

  private def startGoogleCluster(cluster: Cluster, welderAction: WelderAction, runtimeConfig: RuntimeConfig)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Unit] =
    for {
      metadata <- getMasterInstanceStartupScript(cluster, welderAction)

      // Add back the preemptible instances, if any
      _ <- runtimeConfig match {
        case x: RuntimeConfig.DataprocConfig if (x.numberOfPreemptibleWorkers.exists(_ > 0)) =>
          IO.fromFuture(
            IO(
              gdDAO.resizeCluster(cluster.googleProject,
                                  cluster.runtimeName,
                                  numPreemptibles = x.numberOfPreemptibleWorkers)
            )
          )
        case _ => IO.unit
      }

      // Start each instance individually
      _ <- cluster.nonPreemptibleInstances.toList.parTraverse { instance =>
        // Install a startup script on the master node so Jupyter starts back up again once the instance is restarted
        instance.dataprocRole match {
          case Master =>
            googleComputeService.addInstanceMetadata(
              instance.key.project,
              instance.key.zone,
              instance.key.name,
              metadata
            ) >> googleComputeService.startInstance(instance.key.project, instance.key.zone, instance.key.name)
          case _ =>
            googleComputeService.startInstance(instance.key.project, instance.key.zone, instance.key.name)
        }
      }

    } yield ()

  def startCluster(cluster: Cluster, now: Instant)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    if (cluster.status.isStartable) {
      val welderAction = getWelderAction(cluster)
      for {
        // Check if welder should be deployed or updated
        now <- IO(Instant.now)
        updatedCluster <- welderAction match {
          case DeployWelder | UpdateWelder      => updateWelder(cluster, now)
          case NoAction | DisableDelocalization => IO.pure(cluster)
          case RuntimeOutOfDate                 => IO.raiseError(RuntimeOutOfDateException())
        }
        _ <- if (welderAction == DisableDelocalization && !cluster.labels.contains("welderInstallFailed"))
          dbRef.inTransaction { labelQuery.save(cluster.id, "welderInstallFailed", "true") }.void
        else IO.unit

        runtimeConfig <- dbRef.inTransaction(
          RuntimeConfigQueries.getRuntimeConfig(cluster.runtimeConfigId)
        )
        // Start the cluster in Google
        _ <- startGoogleCluster(updatedCluster, welderAction, runtimeConfig)

        // Update the cluster status to Starting
        now <- IO(Instant.now)
        _ <- dbRef.inTransaction { clusterQuery.updateClusterStatus(updatedCluster.id, RuntimeStatus.Starting, now) }
      } yield ()
    } else IO.raiseError(RuntimeCannotBeStartedException(cluster.googleProject, cluster.runtimeName, cluster.status))

  private def getWelderAction(cluster: Cluster): WelderAction =
    if (cluster.welderEnabled) {
      // Welder is already enabled; do we need to update it?
      val labelFound = welderConfig.updateWelderLabel.exists(cluster.labels.contains)

      val imageChanged = cluster.runtimeImages.find(_.imageType == Welder) match {
        case Some(welderImage) if welderImage.imageUrl != imageConfig.welderImage => true
        case _                                                                    => false
      }

      if (labelFound && imageChanged) UpdateWelder
      else NoAction
    } else {
      // Welder is not enabled; do we need to deploy it?
      val labelFound = welderConfig.deployWelderLabel.exists(cluster.labels.contains)
      if (labelFound) {
        if (isClusterBeforeCutoffDate(cluster)) DisableDelocalization
        else DeployWelder
      } else NoAction
    }

  private def isClusterBeforeCutoffDate(cluster: Cluster): Boolean =
    (for {
      dateStr <- welderConfig.deployWelderCutoffDate
      date <- Try(new SimpleDateFormat("yyyy-MM-dd").parse(dateStr)).toOption
      isClusterBeforeCutoffDate = cluster.auditInfo.createdDate.isBefore(date.toInstant)
    } yield isClusterBeforeCutoffDate) getOrElse false

  private def updateWelder(cluster: Cluster, now: Instant): IO[Cluster] =
    for {
      _ <- log.info(s"Will deploy welder to cluster ${cluster.projectNameString}")
      _ <- metrics.incrementCounter("welder/deploy")
      now <- IO(Instant.now)
      welderImage = RuntimeImage(Welder, imageConfig.welderImage, now)

      _ <- dbRef.inTransaction {
        clusterQuery.updateWelder(cluster.id, RuntimeImage(Welder, imageConfig.welderImage, now), now)
      }

      newCluster = cluster.copy(welderEnabled = true,
                                runtimeImages = cluster.runtimeImages.filterNot(_.imageType == Welder) + welderImage)
    } yield newCluster

  def resizeCluster(cluster: Cluster, numWorkers: Option[Int], numPreemptibles: Option[Int]): IO[Unit] =
    for {
      // IAM roles should already exist for a non-deleted cluster; this method is a no-op if the roles already exist.
      _ <- createClusterIamRoles(cluster.googleProject, cluster.serviceAccountInfo)

      _ <- updateDataprocImageGroupMembership(cluster.googleProject, createCluster = true)

      // Resize the cluster in Google
      _ <- IO.fromFuture(
        IO(gdDAO.resizeCluster(cluster.googleProject, cluster.runtimeName, numWorkers, numPreemptibles))
      )
    } yield ()

  def updateMasterMachineType(existingCluster: Cluster,
                              machineType: MachineTypeName)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    for {
      _ <- log.info(
        s"New machine config present. Changing machine type to ${machineType} for cluster ${existingCluster.projectNameString}..."
      )
      // Update the machine type in Google
      _ <- setMasterMachineTypeInGoogle(existingCluster, machineType)
      // Update the DB
      now <- IO(Instant.now)
      _ <- dbRef.inTransaction {
        RuntimeConfigQueries.updateMachineType(existingCluster.runtimeConfigId, machineType, now)
      }
    } yield ()

  //updates machine type in gdDAO
  private def setMasterMachineTypeInGoogle(cluster: Cluster, machineType: MachineTypeName)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Unit] =
    cluster.dataprocInstances.toList.traverse_ { instance =>
      // Note: we don't support changing the machine type for worker instances. While this is possible
      // in GCP, Spark settings are auto-tuned to machine size. Dataproc recommends adding or removing nodes,
      // and rebuilding the cluster if new worker machine/disk sizes are needed.
      instance.dataprocRole match {
        case Master =>
          googleComputeService.setMachineType(instance.key.project, instance.key.zone, instance.key.name, machineType)
        case _ => IO.unit
      }
    }

  def updateMasterDiskSize(cluster: Cluster, diskSize: Int)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    cluster.dataprocInstances.toList.traverse_ { instance =>
      // Note: we don't support changing the machine type for worker instances. While this is possible
      // in GCP, Spark settings are auto-tuned to machine size. Dataproc recommends adding or removing nodes,
      // and rebuilding the cluster if new worker machine/disk sizes are needed.
      instance.dataprocRole match {
        case Master =>
          // Note for Dataproc the disk name is the same as the instance name
          googleComputeService.resizeDisk(instance.key.project,
                                          instance.key.zone,
                                          DiskName(instance.key.name.value),
                                          diskSize)
        case _ => IO.unit
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
        count <- inTransaction { clusterQuery.countActiveByProject(googleProject) }
        // Note: Don't remove the account if there are existing active clusters in the same project,
        // because it could potentially break other clusters. We only check this for the 'remove' case.
        _ <- if (count > 0 && !createCluster) {
          IO.unit
        } else {
          for {
            projectNumberOptIO <- IO.fromFuture(IO(googleProjectDAO.getProjectNumber(googleProject.value)))
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
          s"Failed to delete init bucket ${initBucketName.value} for ${googleProject.value} / ${clusterName.asString}"
        )
      case _ =>
        log.info(
          s"Successfully deleted init bucket ${initBucketName.value} for ${googleProject.value} / ${clusterName.asString}"
        )
    }

    // Don't delete the staging bucket so the user can see error logs.

    val deleteCluster = IO.fromFuture(IO(gdDAO.deleteCluster(googleProject, clusterName))).attempt.flatMap {
      case Left(e) => log.error(e)(s"Failed to delete cluster ${googleProject.value} / ${clusterName.asString}")
      case _       => log.info(s"Successfully deleted cluster ${googleProject.value} / ${clusterName.asString}")
    }

    val deleteServiceAccountKey = removeServiceAccountKey(googleProject,
                                                          serviceAccountInfo.notebookServiceAccount,
                                                          serviceAccountKeyOpt.map(_.id)).attempt.flatMap {
      case Left(e) =>
        log.error(e)(s"Failed to delete service account key for ${serviceAccountInfo.notebookServiceAccount}")
      case _ => log.info(s"Successfully deleted service account key for ${serviceAccountInfo.notebookServiceAccount}")
    }

    val removeIamRoles = removeClusterIamRoles(googleProject, serviceAccountInfo).attempt.flatMap {
      case Left(e) => log.error(e)(s"Failed to remove IAM roles for ${googleProject.value} / ${clusterName.asString}")
      case _       => log.info(s"Successfully removed IAM roles for ${googleProject.value} / ${clusterName.asString}")
    }

    List(deleteBucket, deleteCluster, deleteServiceAccountKey, removeIamRoles).parSequence_
  }

  private[leonardo] def getClusterResourceContraints(runtimeProjectAndName: RuntimeProjectAndName,
                                                     machineType: MachineTypeName)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[RuntimeResourceConstraints] = {
    val totalMemory = for {
      // Find a zone in which to query the machine type: either the configured zone or
      // an arbitrary zone in the configured region.
      zoneUri <- {
        val configuredZone = OptionT.fromOption[IO](dataprocConfig.dataprocZone.map(ZoneName))
        val zoneList = for {
          zones <- googleComputeService.getZones(runtimeProjectAndName.googleProject,
                                                 RegionName(dataprocConfig.dataprocDefaultRegion))
          _ <- log.debug(s"List of zones in project ${runtimeProjectAndName.googleProject}: ${zones}")
          zoneNames = zones.map(z => ZoneName(z.getName))
        } yield zoneNames

        configuredZone orElse OptionT(zoneList.map(_.headOption))
      }
      _ <- OptionT.liftF(log.debug(s"Using zone ${zoneUri} to resolve machine type"))

      // Resolve the master machine type in Google to get the total memory.
      machineType <- OptionT.pure[IO](machineType)
      resolvedMachineType <- OptionT(
        googleComputeService.getMachineType(runtimeProjectAndName.googleProject, zoneUri, machineType)
      )
      _ <- OptionT.liftF(log.debug(s"Resolved machine type: ${resolvedMachineType.toString}"))
    } yield MemorySize.fromMb(resolvedMachineType.getMemoryMb.toDouble)

    totalMemory.value.flatMap {
      case None        => IO.raiseError(ClusterResourceConstaintsException(runtimeProjectAndName, machineType))
      case Some(total) =>
        // total - dataproc allocated - welder allocated
        val dataprocAllocated = dataprocConfig.dataprocReservedMemory.map(_.bytes).getOrElse(0L)
        val welderAllocated = welderConfig.welderReservedMemory.map(_.bytes).getOrElse(0L)
        val result = MemorySize(total.bytes - dataprocAllocated - welderAllocated)
        IO.pure(RuntimeResourceConstraints(result))
    }
  }

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
      dbRef.inTransaction { clusterQuery.countActiveByClusterServiceAccount(email) }.flatMap { count =>
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
  private def getMasterInstanceStartupScript(runtime: Runtime, welderAction: WelderAction): IO[Map[String, String]] = {
    val googleKey = "startup-script" // required; see https://cloud.google.com/compute/docs/startupscript

    val templateConfig = RuntimeTemplateValuesConfig.fromRuntime(runtime,
                                                                 None,
                                                                 None,
                                                                 dataprocConfig,
                                                                 imageConfig,
                                                                 welderConfig,
                                                                 proxyConfig,
                                                                 clusterFilesConfig,
                                                                 clusterResourcesConfig,
                                                                 None)
    val clusterInit = RuntimeTemplateValues(templateConfig)
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

  private def getMasterInstanceShutdownScript(runtime: Runtime): IO[Map[String, String]] = {
    val googleKey = "shutdown-script" // required; see https://cloud.google.com/compute/docs/shutdownscript

    val templateConfig = RuntimeTemplateValuesConfig.fromRuntime(runtime,
                                                                 None,
                                                                 None,
                                                                 dataprocConfig,
                                                                 imageConfig,
                                                                 welderConfig,
                                                                 proxyConfig,
                                                                 clusterFilesConfig,
                                                                 clusterResourcesConfig,
                                                                 None)
    val replacements = RuntimeTemplateValues(templateConfig).toMap

    TemplateHelper
      .templateResource(replacements, clusterResourcesConfig.shutdownScript, blocker)
      .compile
      .to[Array]
      .map { bytes =>
        Map(googleKey -> new String(bytes, StandardCharsets.UTF_8))
      }
  }

}

final case class CreateClusterResponse(asyncRuntimeFields: AsyncRuntimeFields,
                                       initBucket: GcsBucketName,
                                       serviceAccountKey: Option[ServiceAccountKey],
                                       customDataprocImage: CustomDataprocImage)
