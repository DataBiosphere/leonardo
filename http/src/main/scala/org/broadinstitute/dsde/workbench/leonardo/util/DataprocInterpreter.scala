package org.broadinstitute.dsde.workbench.leonardo
package util

import _root_.io.chrisdavenport.log4cats.StructuredLogger
import akka.http.scaladsl.model.StatusCodes
import cats.Parallel
import cats.effect.{Async, _}
import cats.mtl.Ask
import cats.syntax.all._
import com.google.api.gax.rpc.ApiException
import com.google.api.services.admin.directory.model.Group
import com.google.cloud.dataproc.v1._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO.MemberType
import org.broadinstitute.dsde.workbench.google.GoogleUtilities.RetryPredicates._
import org.broadinstitute.dsde.workbench.google._
import org.broadinstitute.dsde.workbench.google2.DataprocRole.Master
import org.broadinstitute.dsde.workbench.google2.{
  CreateClusterConfig,
  DataprocClusterName,
  DiskName,
  GoogleComputeService,
  GoogleDataprocService,
  GoogleDiskService,
  GoogleResourceService,
  MachineTypeName,
  RegionName,
  ZoneName
}
import org.broadinstitute.dsde.workbench.leonardo.CustomImage.DataprocCustomImage
import org.broadinstitute.dsde.workbench.leonardo.dao.WelderDAO
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.dataprocInCreateRuntimeMsgToDataprocRuntime
import org.broadinstitute.dsde.workbench.leonardo.model.{InvalidDataprocMachineConfigException, _}
import org.broadinstitute.dsde.workbench.leonardo.monitor.RuntimeConfigInCreateRuntimeMessage
import org.broadinstitute.dsde.workbench.leonardo.util.RuntimeInterpreterConfig.DataprocInterpreterConfig
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

final case class ClusterIamSetupException(googleProject: GoogleProject)
    extends LeoException(s"Error occurred setting up IAM roles in project ${googleProject.value}", traceId = None)

final case class GoogleGroupCreationException(googleGroup: WorkbenchEmail, msg: String)
    extends LeoException(s"Failed to create the Google group '${googleGroup}': $msg",
                         StatusCodes.InternalServerError,
                         traceId = None)

final case class GoogleProjectNotFoundException(project: GoogleProject)
    extends LeoException(s"Google did not have any record of project: $project.", StatusCodes.NotFound, traceId = None)

final case object ImageProjectNotFoundException
    extends LeoException("Custom Dataproc image project not found", StatusCodes.NotFound, traceId = None)

final case class ClusterResourceConstraintsException(clusterProjectAndName: RuntimeProjectAndName,
                                                     machineType: MachineTypeName,
                                                     region: RegionName)
    extends LeoException(
      s"Unable to calculate memory constraints for cluster ${clusterProjectAndName.googleProject}/${clusterProjectAndName.runtimeName} with master machine type ${machineType} in region ${region}",
      traceId = None
    )

final case class RegionNotSupportedException(region: RegionName, traceId: TraceId)
    extends LeoException(s"Region ${region.value} not supported for Dataproc cluster creation",
                         StatusCodes.Conflict,
                         traceId = Some(traceId))

class DataprocInterpreter[F[_]: Timer: Parallel: ContextShift](
  config: DataprocInterpreterConfig,
  bucketHelper: BucketHelper[F],
  vpcAlg: VPCAlgebra[F],
  googleDataprocService: Map[RegionName, GoogleDataprocService[F]],
  googleComputeService: GoogleComputeService[F],
  googleDiskService: GoogleDiskService[F],
  googleDirectoryDAO: GoogleDirectoryDAO,
  googleIamDAO: GoogleIamDAO,
  googleResourceService: GoogleResourceService[F],
  welderDao: WelderDAO[F],
  blocker: Blocker
)(implicit val F: Async[F],
  executionContext: ExecutionContext,
  contextShift: ContextShift[IO], // needed for IO.fromFuture(...)
  metrics: OpenTelemetryMetrics[F],
  logger: StructuredLogger[F],
  dbRef: DbReference[F])
    extends BaseRuntimeInterpreter[F](config, welderDao)
    with RuntimeAlgebra[F]
    with LazyLogging {

  import dbRef._

  override def createRuntime(
    params: CreateRuntimeParams
  )(implicit ev: Ask[F, AppContext]): F[CreateGoogleRuntimeResponse] = {
    for {
      ctx <- ev.ask
      machineConfig <- params.runtimeConfig match {
        case x: RuntimeConfigInCreateRuntimeMessage.DataprocConfig =>
          F.pure(dataprocInCreateRuntimeMsgToDataprocRuntime(x))
        case _ =>
          F.raiseError[RuntimeConfig.DataprocConfig](
            new RuntimeException("DataprocInterpreter shouldn't get a GCE request")
          )
      }
      initBucketName = generateUniqueBucketName("leoinit-" + params.runtimeProjectAndName.runtimeName.asString)
      stagingBucketName = generateUniqueBucketName("leostaging-" + params.runtimeProjectAndName.runtimeName.asString)

      createOp = for {
        // Set up VPC network and firewall
        (network, subnetwork) <- vpcAlg.setUpProjectNetwork(
          SetUpProjectNetworkParams(params.runtimeProjectAndName.googleProject, machineConfig.region)
        )
        _ <- vpcAlg.setUpProjectFirewalls(
          SetUpProjectFirewallsParams(params.runtimeProjectAndName.googleProject, network)
        )
        resourceConstraints <- getClusterResourceContraints(params.runtimeProjectAndName,
                                                            machineConfig.masterMachineType,
                                                            machineConfig.region)

        // Set up IAM roles necessary to create a cluster.
        _ <- createClusterIamRoles(params.runtimeProjectAndName.googleProject, params.serviceAccountInfo)

        // Add member to the Google Group that has the IAM role to pull the Dataproc image
        _ <- updateDataprocImageGroupMembership(params.runtimeProjectAndName.googleProject, createCluster = true)

        // Create the bucket in the cluster's google project and populate with initialization files.
        // ACLs are granted so the cluster service account can access the files at initialization time.
        _ <- bucketHelper
          .createInitBucket(params.runtimeProjectAndName.googleProject, initBucketName, params.serviceAccountInfo)
          .compile
          .drain

        // Create the cluster staging bucket. ACLs are granted so the user/pet can access it.
        _ <- bucketHelper
          .createStagingBucket(params.auditInfo.creator,
                               params.runtimeProjectAndName.googleProject,
                               stagingBucketName,
                               params.serviceAccountInfo)
          .compile
          .drain

        templateParams = RuntimeTemplateValuesConfig.fromCreateRuntimeParams(
          params,
          Some(initBucketName),
          Some(stagingBucketName),
          None,
          config.imageConfig,
          config.welderConfig,
          config.proxyConfig,
          config.clusterFilesConfig,
          config.clusterResourcesConfig,
          Some(resourceConstraints),
          false
        )
        templateValues = RuntimeTemplateValues(templateParams, Some(ctx.now))
        _ <- bucketHelper
          .initializeBucketObjects(initBucketName,
                                   templateParams.serviceAccountKey,
                                   templateValues,
                                   params.customEnvironmentVariables)
          .compile
          .drain

        // build cluster configuration
        initScriptResources = List(config.clusterResourcesConfig.initActionsScript)
        initScripts = initScriptResources.map(resource => GcsPath(initBucketName, GcsObjectName(resource.asString)))

        // If user is using https://github.com/DataBiosphere/terra-docker/tree/master#terra-base-images for jupyter image, then
        // we will use the new custom dataproc image
        dataprocImage = if (params.runtimeImages.exists(_.imageUrl == config.imageConfig.legacyJupyterImage.imageUrl))
          config.dataprocConfig.legacyCustomDataprocImage
        else config.dataprocConfig.customDataprocImage

        gceClusterConfig = {
          val bldr = GceClusterConfig
            .newBuilder()
            .addTags(config.vpcConfig.networkTag.value)
            .setSubnetworkUri(subnetwork.value)
            .setServiceAccount(params.serviceAccountInfo.value)
            .addAllServiceAccountScopes(params.scopes.asJava)
          bldr.build()
        }

        nodeInitializationActions = initScripts.map { script =>
          NodeInitializationAction
            .newBuilder()
            .setExecutableFile(script.toUri)
            .setExecutionTimeout(
              com.google.protobuf.Duration.newBuilder().setSeconds(config.runtimeCreationTimeout.toSeconds)
            )
            .build()
        }

        masterConfig = InstanceGroupConfig
          .newBuilder()
          .setMachineTypeUri(machineConfig.masterMachineType.value)
          .setDiskConfig(
            DiskConfig
              .newBuilder()
              .setBootDiskSizeGb(machineConfig.masterDiskSize.gb)
          )
          .setImageUri(dataprocImage.asString)
          .build()

        (workerConfig, secondaryWorkerConfig) = if (machineConfig.numberOfWorkers > 0) {
          (machineConfig.workerMachineType,
           machineConfig.workerDiskSize,
           machineConfig.numberOfWorkerLocalSSDs,
           machineConfig.numberOfPreemptibleWorkers)
            .mapN {
              case (machineType, diskSize, numLocalSSDs, numPreemptibles) =>
                val workerConfig = InstanceGroupConfig
                  .newBuilder()
                  .setNumInstances(machineConfig.numberOfWorkers)
                  .setMachineTypeUri(machineType.value)
                  .setDiskConfig(DiskConfig.newBuilder().setBootDiskSizeGb(diskSize.gb).setNumLocalSsds(numLocalSSDs))
                  .setImageUri(dataprocImage.asString)
                  .build()

                val secondaryWorkerConfig =
                  if (numPreemptibles > 0)
                    Some(
                      InstanceGroupConfig
                        .newBuilder()
                        .setIsPreemptible(true)
                        .setNumInstances(numPreemptibles)
                        .setMachineTypeUri(machineType.value)
                        .setDiskConfig(DiskConfig.newBuilder().setBootDiskSizeGb(diskSize.gb))
                        .setImageUri(dataprocImage.asString)
                        .build()
                    )
                  else None

                (Some(workerConfig), secondaryWorkerConfig)
            }
            .getOrElse((None, None))
        } else (None, None)

        softwareConfig = getSoftwareConfig(params.runtimeProjectAndName.googleProject, machineConfig)

        createClusterConfig = CreateClusterConfig(
          gceClusterConfig,
          nodeInitializationActions,
          masterConfig,
          workerConfig,
          secondaryWorkerConfig,
          stagingBucketName,
          softwareConfig
        )

        dataproc <- F.fromOption(googleDataprocService.get(machineConfig.region),
                                 RegionNotSupportedException(machineConfig.region, ctx.traceId))
        op <- dataproc.createCluster(
          params.runtimeProjectAndName.googleProject,
          machineConfig.region,
          DataprocClusterName(params.runtimeProjectAndName.runtimeName.asString),
          Some(createClusterConfig)
        )

        asyncRuntimeFields = AsyncRuntimeFields(
          GoogleId(op.metadata.getClusterUuid),
          op.name,
          stagingBucketName,
          None
        )
      } yield CreateGoogleRuntimeResponse(asyncRuntimeFields, initBucketName, None, dataprocImage)

      res <- createOp.handleErrorWith { throwable =>
        cleanUpGoogleResourcesOnError(
          params.runtimeProjectAndName.googleProject,
          params.runtimeProjectAndName.runtimeName,
          initBucketName,
          params.serviceAccountInfo,
          machineConfig.region
        ) >> F.raiseError[CreateGoogleRuntimeResponse](throwable)
      }
    } yield res
  }

  override def deleteRuntime(
    params: DeleteRuntimeParams
  )(implicit ev: Ask[F, AppContext]): F[Option[com.google.cloud.compute.v1.Operation]] =
    if (params.runtimeAndRuntimeConfig.runtime.asyncRuntimeFields.isDefined) { //check if runtime has been created
      for {
        ctx <- ev.ask
        region <- F.fromOption(
          LeoLenses.dataprocRegion.getOption(params.runtimeAndRuntimeConfig.runtimeConfig),
          new RuntimeException("DataprocInterpreter shouldn't get a GCE request")
        )
        metadata <- getShutdownScript(params.runtimeAndRuntimeConfig.runtime, blocker)
        _ <- params.runtimeAndRuntimeConfig.runtime.dataprocInstances.find(_.dataprocRole == Master).traverse {
          instance =>
            googleComputeService
              .addInstanceMetadata(instance.key.project, instance.key.zone, instance.key.name, metadata)
        }

        dataproc <- F.fromOption(googleDataprocService.get(region), RegionNotSupportedException(region, ctx.traceId))
        _ <- dataproc.deleteCluster(
          params.runtimeAndRuntimeConfig.runtime.googleProject,
          region,
          DataprocClusterName(params.runtimeAndRuntimeConfig.runtime.runtimeName.asString)
        )
      } yield None
    } else F.pure(None)

  override def finalizeDelete(params: FinalizeDeleteParams)(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      _ <- removeClusterIamRoles(params.runtime.googleProject, params.runtime.serviceAccount)
      _ <- updateDataprocImageGroupMembership(params.runtime.googleProject, createCluster = false)
    } yield ()

  override protected def stopGoogleRuntime(params: StopGoogleRuntime)(
    implicit ev: Ask[F, AppContext]
  ): F[Option[com.google.cloud.compute.v1.Operation]] =
    for {
      ctx <- ev.ask
      region <- F.fromOption(
        LeoLenses.dataprocRegion.getOption(params.runtimeAndRuntimeConfig.runtimeConfig),
        new RuntimeException("DataprocInterpreter shouldn't get a GCE request")
      )
      metadata <- getShutdownScript(params.runtimeAndRuntimeConfig.runtime, blocker)
      dataproc <- F.fromOption(googleDataprocService.get(region), RegionNotSupportedException(region, ctx.traceId))
      _ <- dataproc stopCluster (
        params.runtimeAndRuntimeConfig.runtime.googleProject,
        region,
        DataprocClusterName(params.runtimeAndRuntimeConfig.runtime.runtimeName.asString),
        Some(metadata)
      )
    } yield None

  override protected def startGoogleRuntime(params: StartGoogleRuntime)(
    implicit ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      dataprocConfig <- F.fromOption(
        LeoLenses.dataprocPrism.getOption(params.runtimeAndRuntimeConfig.runtimeConfig),
        new RuntimeException("DataprocInterpreter shouldn't get a GCE request")
      )
      resourceConstraints <- getClusterResourceContraints(
        RuntimeProjectAndName(params.runtimeAndRuntimeConfig.runtime.googleProject,
                              params.runtimeAndRuntimeConfig.runtime.runtimeName),
        params.runtimeAndRuntimeConfig.runtimeConfig.machineType,
        dataprocConfig.region
      )
      metadata <- getStartupScript(params.runtimeAndRuntimeConfig.runtime,
                                   params.welderAction,
                                   params.initBucket,
                                   blocker,
                                   resourceConstraints,
                                   false)

      dataproc <- F.fromOption(googleDataprocService.get(dataprocConfig.region),
                               RegionNotSupportedException(dataprocConfig.region, ctx.traceId))
      _ <- dataproc.startCluster(
        params.runtimeAndRuntimeConfig.runtime.googleProject,
        dataprocConfig.region,
        DataprocClusterName(params.runtimeAndRuntimeConfig.runtime.runtimeName.asString),
        dataprocConfig.numberOfPreemptibleWorkers,
        Some(metadata)
      )
    } yield ()

  override def resizeCluster(params: ResizeClusterParams)(implicit ev: Ask[F, AppContext]): F[Unit] =
    (for {
      ctx <- ev.ask
      region <- F.fromOption(
        LeoLenses.dataprocRegion.getOption(params.runtimeAndRuntimeConfig.runtimeConfig),
        new RuntimeException("DataprocInterpreter shouldn't get a GCE request")
      )
      // IAM roles should already exist for a non-deleted cluster; this method is a no-op if the roles already exist.
      _ <- createClusterIamRoles(params.runtimeAndRuntimeConfig.runtime.googleProject,
                                 params.runtimeAndRuntimeConfig.runtime.serviceAccount)

      _ <- updateDataprocImageGroupMembership(params.runtimeAndRuntimeConfig.runtime.googleProject,
                                              createCluster = true)

      // Resize the cluster in Google
      dataproc <- F.fromOption(googleDataprocService.get(region), RegionNotSupportedException(region, ctx.traceId))
      _ <- dataproc.resizeCluster(
        params.runtimeAndRuntimeConfig.runtime.googleProject,
        region,
        DataprocClusterName(params.runtimeAndRuntimeConfig.runtime.runtimeName.asString),
        params.numWorkers,
        params.numPreemptibles
      )
    } yield ()) recoverWith {
      case e: ApiException =>
        // Typically we will revoke this role in the monitor after everything is complete, but if Google fails to
        // resize the cluster we need to revoke it manually here
        for {
          ctx <- ev.ask
          _ <- removeClusterIamRoles(params.runtimeAndRuntimeConfig.runtime.googleProject,
                                     params.runtimeAndRuntimeConfig.runtime.serviceAccount)
          // Remove member from the Google Group that has the IAM role to pull the Dataproc image
          _ <- updateDataprocImageGroupMembership(params.runtimeAndRuntimeConfig.runtime.googleProject,
                                                  createCluster = false)
          _ <- logger.error(ctx.loggingCtx, e)(
            s"Could not successfully update cluster ${params.runtimeAndRuntimeConfig.runtime.projectNameString}"
          )
          _ <- F.raiseError[Unit](InvalidDataprocMachineConfigException(e.getMessage))
        } yield ()
    }

  //updates machine type in gdDAO
  override protected def setMachineTypeInGoogle(params: SetGoogleMachineType)(
    implicit ev: Ask[F, AppContext]
  ): F[Unit] =
    params.runtimeAndRuntimeConfig.runtime.dataprocInstances
      .find(_.dataprocRole == Master)
      .traverse_(instance =>
        // Note: we don't support changing the machine type for worker instances. While this is possible
        // in GCP, Spark settings are auto-tuned to machine size. Dataproc recommends adding or removing nodes,
        // and rebuilding the cluster if new worker machine/disk sizes are needed.
        googleComputeService
          .setMachineType(instance.key.project, instance.key.zone, instance.key.name, params.machineType)
      )

  // Note: we don't support changing the machine type for worker instances. While this is possible
  // in GCP, Spark settings are auto-tuned to machine size. Dataproc recommends adding or removing nodes,
  // and rebuilding the cluster if new worker machine/disk sizes are needed.
  override def updateDiskSize(params: UpdateDiskSizeParams)(implicit ev: Ask[F, AppContext]): F[Unit] =
    UpdateDiskSizeParams.dataprocPrism
      .getOption(params)
      .traverse_ { p =>
        googleDiskService
          .resizeDisk(p.masterDataprocInstance.key.project,
                      p.masterDataprocInstance.key.zone,
                      DiskName(p.masterDataprocInstance.key.name.value),
                      p.diskSize.gb)
      }

  def createClusterIamRoles(googleProject: GoogleProject, serviceAccountInfo: WorkbenchEmail): F[Unit] =
    updateClusterIamRoles(googleProject, serviceAccountInfo, createCluster = true)

  def removeClusterIamRoles(googleProject: GoogleProject, serviceAccountInfo: WorkbenchEmail): F[Unit] =
    updateClusterIamRoles(googleProject, serviceAccountInfo, createCluster = false)

  def setupDataprocImageGoogleGroup(implicit ev: Ask[F, AppContext]): F[Unit] =
    createDataprocImageUserGoogleGroupIfItDoesntExist >>
      addIamRoleToDataprocImageGroup

  /**
   * Add the user's service account to the Google group.
   * This group has compute.imageUser role on the custom Dataproc image project,
   * which allows the user's cluster to pull the image.
   */
  def updateDataprocImageGroupMembership(googleProject: GoogleProject,
                                         createCluster: Boolean)(implicit ev: Ask[F, AppContext]): F[Unit] =
    parseImageProject(config.dataprocConfig.customDataprocImage).traverse_ { imageProject =>
      for {
        count <- inTransaction(clusterQuery.countActiveByProject(googleProject))
        // Note: Don't remove the account if there are existing active clusters in the same project,
        // because it could potentially break other clusters. We only check this for the 'remove' case.
        _ <- if (count > 0 && !createCluster) {
          F.unit
        } else {
          for {
            projectNumberOpt <- googleResourceService.getProjectNumber(googleProject)

            projectNumber <- F.fromEither(projectNumberOpt.toRight(GoogleProjectNotFoundException(googleProject)))
            // Note that the Dataproc service account is used to retrieve the image, and not the user's
            // pet service account. There is one Dataproc service account per Google project. For more details:
            // https://cloud.google.com/dataproc/docs/concepts/iam/iam#service_accounts

            // Note we add both service-[project-number]@dataproc-accounts.iam.gserviceaccount.com and
            // [project-number]@cloudservices.gserviceaccount.com to the group because both seem to be
            // used in different circumstances (the latter seems to be used for adding preemptibles, for example).
            dataprocServiceAccountEmail = WorkbenchEmail(
              s"service-${projectNumber}@dataproc-accounts.iam.gserviceaccount.com"
            )
            _ <- updateGroupMembership(config.groupsConfig.dataprocImageProjectGroupEmail,
                                       dataprocServiceAccountEmail,
                                       createCluster)
            apiServiceAccountEmail = WorkbenchEmail(
              s"${projectNumber}@cloudservices.gserviceaccount.com"
            )
            _ <- updateGroupMembership(config.groupsConfig.dataprocImageProjectGroupEmail,
                                       apiServiceAccountEmail,
                                       createCluster)
          } yield ()
        }
      } yield ()
    }

  private def cleanUpGoogleResourcesOnError(
    googleProject: GoogleProject,
    clusterName: RuntimeName,
    initBucketName: GcsBucketName,
    serviceAccountInfo: WorkbenchEmail,
    region: RegionName
  )(implicit ev: Ask[F, AppContext]): F[Unit] =
    ev.ask.flatMap { ctx =>
      // Clean up resources in Google
      val deleteBucket = bucketHelper.deleteInitBucket(googleProject, initBucketName).attempt.flatMap {
        case Left(e) =>
          logger.error(ctx.loggingCtx, e)(
            s"Failed to delete init bucket ${initBucketName.value} for ${googleProject.value} / ${clusterName.asString}"
          )
        case _ =>
          logger.info(ctx.loggingCtx)(
            s"Successfully deleted init bucket ${initBucketName.value} for ${googleProject.value} / ${clusterName.asString}"
          )
      }

      // Don't delete the staging bucket so the user can see error logs.

      val deleteCluster = for {
        dataproc <- F.fromOption(googleDataprocService.get(region), RegionNotSupportedException(region, ctx.traceId))
        _ <- dataproc
          .deleteCluster(googleProject, region, DataprocClusterName(clusterName.asString))
          .attempt
          .flatMap {
            case Left(e) =>
              logger.error(ctx.loggingCtx, e)(
                s"Failed to delete cluster ${googleProject.value} / ${clusterName.asString}"
              )
            case _ =>
              logger.info(ctx.loggingCtx)(
                s"Successfully deleted cluster ${googleProject.value} / ${clusterName.asString}"
              )
          }
      } yield ()

      val removeIamRoles = removeClusterIamRoles(googleProject, serviceAccountInfo).attempt.flatMap {
        case Left(e) =>
          logger.error(ctx.loggingCtx, e)(
            s"Failed to remove IAM roles for ${googleProject.value} / ${clusterName.asString}"
          )
        case _ =>
          logger.info(ctx.loggingCtx)(
            s"Successfully removed IAM roles for ${googleProject.value} / ${clusterName.asString}"
          )
      }

      List(deleteBucket, deleteCluster, removeIamRoles).parSequence_
    }

  private[leonardo] def getClusterResourceContraints(runtimeProjectAndName: RuntimeProjectAndName,
                                                     machineType: MachineTypeName,
                                                     region: RegionName)(
    implicit ev: Ask[F, AppContext]
  ): F[RuntimeResourceConstraints] =
    for {
      ctx <- ev.ask
      // Find an arbitrary zone in the configured region in which to query the machine type
      zones <- googleComputeService.getZones(runtimeProjectAndName.googleProject, region)
      zoneUri <- F.fromOption(zones.headOption.map(z => ZoneName(z.getName)),
                              ClusterResourceConstraintsException(runtimeProjectAndName, machineType, region))
      _ <- logger.debug(ctx.loggingCtx)(s"Using zone ${zoneUri} to resolve machine type")

      // Resolve the master machine type in Google to get the total memory.
      resolvedMachineTypeOpt <- googleComputeService.getMachineType(runtimeProjectAndName.googleProject,
                                                                    zoneUri,
                                                                    machineType)
      resolvedMachineType <- F.fromOption(
        resolvedMachineTypeOpt,
        ClusterResourceConstraintsException(runtimeProjectAndName, machineType, region)
      )
      _ <- logger.debug(ctx.loggingCtx)(s"Resolved machine type: ${resolvedMachineType.toString}")
      total = MemorySize.fromMb(resolvedMachineType.getMemoryMb.toDouble)
    } yield {
      // total - dataproc allocated - welder allocated
      val dataprocAllocated = config.dataprocConfig.dataprocReservedMemory.map(_.bytes).getOrElse(0L)
      val welderAllocated = config.welderConfig.welderReservedMemory.map(_.bytes).getOrElse(0L)
      val result = MemorySize(total.bytes - dataprocAllocated - welderAllocated)
      RuntimeResourceConstraints(result)
    }

  /**
   * Add the Dataproc Worker role in the user's project to the cluster service account, if present.
   * This is needed to be able to spin up Dataproc clusters using a custom service account.
   * If the Google Compute default service account is being used, this is not necessary.
   */
  private def updateClusterIamRoles(googleProject: GoogleProject,
                                    serviceAccountInfo: WorkbenchEmail,
                                    createCluster: Boolean): F[Unit] = {
    def retryIam(project: GoogleProject, email: WorkbenchEmail, roles: Set[String]): F[Unit] = {
      val action = if (createCluster) {
        F.liftIO(
          IO.fromFuture(IO(googleIamDAO.addIamRoles(project, email, MemberType.ServiceAccount, roles).void))
        )
      } else {
        F.liftIO(
          IO.fromFuture(IO(googleIamDAO.removeIamRoles(project, email, MemberType.ServiceAccount, roles).void))
        )
      }
      retry(action, when409)
    }
    // Note: don't remove the role if there are existing active clusters owned by the same user,
    // because it could potentially break other clusters. We only check this for the 'remove' case,
    // it's ok to re-add the roles.
    dbRef.inTransaction(clusterQuery.countActiveByClusterServiceAccount(serviceAccountInfo)).flatMap { count =>
      if (count > 0 && !createCluster) {
        F.unit
      } else {
        retryIam(googleProject, serviceAccountInfo, Set("roles/dataproc.worker"))
      }
    }
  }

  private def createDataprocImageUserGoogleGroupIfItDoesntExist(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask
      _ <- logger.debug(ctx.loggingCtx)(
        s"Checking if Dataproc image user Google group '${config.groupsConfig.dataprocImageProjectGroupEmail}' already exists..."
      )

      groupOpt <- F.liftIO(
        IO.fromFuture[Option[Group]](
          IO(googleDirectoryDAO.getGoogleGroup(config.groupsConfig.dataprocImageProjectGroupEmail))
        )
      )
      _ <- groupOpt.fold(
        logger.debug(ctx.loggingCtx)(
          s"Dataproc image user Google group '${config.groupsConfig.dataprocImageProjectGroupEmail}' does not exist. Attempting to create it..."
        ) >> createDataprocImageUserGoogleGroup()
      )(group =>
        logger.debug(ctx.loggingCtx)(
          s"Dataproc image user Google group '${config.groupsConfig.dataprocImageProjectGroupEmail}' already exists: $group \n Won't attempt to create it."
        )
      )
    } yield ()

  private def createDataprocImageUserGoogleGroup(): F[Unit] =
    F.liftIO(
        IO.fromFuture(
          IO(
            googleDirectoryDAO
              .createGroup(
                config.groupsConfig.dataprocImageProjectGroupName,
                config.groupsConfig.dataprocImageProjectGroupEmail,
                Option(googleDirectoryDAO.lockedDownGroupSettings)
              )
          )
        )
      )
      .handleErrorWith {
        case t if when409(t) => F.unit
        case t =>
          F.raiseError(
            GoogleGroupCreationException(config.groupsConfig.dataprocImageProjectGroupEmail, t.getMessage)
          )
      }

  private def addIamRoleToDataprocImageGroup(implicit ev: Ask[F, AppContext]): F[Unit] = {
    val computeImageUserRole = Set("roles/compute.imageUser")

    parseImageProject(config.dataprocConfig.customDataprocImage).fold(
      F.raiseError[Unit](ImageProjectNotFoundException)
    ) { imageProject =>
      for {
        ctx <- ev.ask
        _ <- logger.debug(ctx.loggingCtx)(
          s"Attempting to grant 'compute.imageUser' permissions to '${config.groupsConfig.dataprocImageProjectGroupEmail}' on project '$imageProject' ..."
        )
        _ <- retry(
          F.liftIO(
            IO(
              googleIamDAO.addIamRoles(imageProject,
                                       config.groupsConfig.dataprocImageProjectGroupEmail,
                                       MemberType.Group,
                                       computeImageUserRole)
            )
          ),
          when409
        )
      } yield ()
    }
  }

  private def updateGroupMembership(groupEmail: WorkbenchEmail, memberEmail: WorkbenchEmail, addToGroup: Boolean)(
    implicit ev: Ask[F, AppContext]
  ): F[Unit] = {
    val checkIsMember = retry(
      F.liftIO(IO.fromFuture(IO(googleDirectoryDAO.isGroupMember(groupEmail, memberEmail)))),
      when409
    )
    val addMemberToGroup = retry(
      F.liftIO(IO.fromFuture(IO(googleDirectoryDAO.addMemberToGroup(groupEmail, memberEmail)))),
      when409
    )
    val removeMemberFromGroup = retry(
      F.liftIO(IO.fromFuture(IO(googleDirectoryDAO.removeMemberFromGroup(groupEmail, memberEmail)))),
      when409
    )

    for {
      ctx <- ev.ask
      isMember <- checkIsMember
      _ <- (isMember, addToGroup) match {
        case (false, true) =>
          logger.info(ctx.loggingCtx)(s"Adding '$memberEmail' to group '$groupEmail'...") >> addMemberToGroup
        case (true, false) =>
          logger.info(ctx.loggingCtx)(s"Removing '$memberEmail' from group '$groupEmail'...") >> removeMemberFromGroup
        case _ =>
          F.unit
      }
    } yield ()
  }

  private def retry[A](ioa: F[A], retryable: Throwable => Boolean): F[A] =
    fs2.Stream
      .retry(ioa, 2 seconds, x => x * 2, 5, retryable)
      .compile
      .lastOrError

  // See https://cloud.google.com/dataproc/docs/guides/dataproc-images#custom_image_uri
  private def parseImageProject(customDataprocImage: DataprocCustomImage): Option[GoogleProject] = {
    val regex = ".*projects/(.*)/global/images/(.*)".r
    customDataprocImage.asString match {
      case regex(project, _) => Some(GoogleProject(project))
      case _                 => None
    }
  }

  private def getSoftwareConfig(googleProject: GoogleProject,
                                machineConfig: RuntimeConfig.DataprocConfig): SoftwareConfig = {
    val dataprocProps = if (machineConfig.numberOfWorkers == 0) {
      // Set a SoftwareConfig property that makes the cluster have only one node
      Map("dataproc:dataproc.allow.zero.workers" -> "true")
    } else Map.empty[String, String]

    val yarnProps = Map(
      // Helps with debugging
      "yarn:yarn.log-aggregation-enable" -> "true"
    )

    val stackdriverProps = Map("dataproc:dataproc.monitoring.stackdriver.enable" -> "true")

    // Enable requester pays "auto" mode so Hail users can access reference data in public RP buckets.
    // Since all Leo clusters are in US regions this shouldn't incur extra charges since Hail buckets
    // are also US-based (and replicated in other regions as well).
    // See https://broadworkbench.atlassian.net/browse/IA-2056
    val requesterPaysProps = Map(
      "spark:spark.hadoop.fs.gs.requester.pays.mode" -> "AUTO",
      "spark:spark.hadoop.fs.gs.requester.pays.project.id" -> googleProject.value
    )

    SoftwareConfig
      .newBuilder()
      .putAllProperties(
        (dataprocProps ++ yarnProps ++ stackdriverProps ++ requesterPaysProps ++ machineConfig.properties).asJava
      )
      .build()
  }
}
