package org.broadinstitute.dsde.workbench.leonardo
package util

import _root_.org.typelevel.log4cats.StructuredLogger
import akka.http.scaladsl.model.StatusCodes
import cats.Parallel
import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import com.google.api.gax.longrunning.OperationFuture
import com.google.api.gax.rpc.ApiException
import com.google.api.services.directory.model.Group
import com.google.cloud.compute.v1.{Operation, Tags}
import com.google.cloud.dataproc.v1.{RuntimeConfig => _, _}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.google.GoogleUtilities.RetryPredicates._
import org.broadinstitute.dsde.workbench.google._
import org.broadinstitute.dsde.workbench.google2.{
  streamUntilDoneOrTimeout,
  CreateClusterConfig,
  DataprocClusterName,
  DataprocRole,
  DataprocRoleZonePreemptibility,
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
import org.broadinstitute.dsde.workbench.leonardo.http.{ctxConversion, dataprocInCreateRuntimeMsgToDataprocRuntime}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.RuntimeConfigInCreateRuntimeMessage
import org.broadinstitute.dsde.workbench.leonardo.util.RuntimeInterpreterConfig.DataprocInterpreterConfig
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.model.google.iam.IamMemberTypes
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.broadinstitute.dsde.workbench.util2.InstanceName
import org.broadinstitute.dsde.workbench.{google2, DoneCheckable}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

final case class ClusterIamSetupException(googleProject: GoogleProject)
    extends LeoException(s"Error occurred setting up IAM roles in project ${googleProject.value}", traceId = None)

final case class GoogleGroupCreationException(googleGroup: WorkbenchEmail, msg: String)
    extends LeoException(s"Failed to create the Google group '${googleGroup}': $msg",
                         StatusCodes.InternalServerError,
                         traceId = None
    )

final case class GoogleProjectNotFoundException(project: GoogleProject)
    extends LeoException(s"Google did not have any record of project: $project.", StatusCodes.NotFound, traceId = None)

final case object ImageProjectNotFoundException
    extends LeoException("Custom Dataproc image project not found", StatusCodes.NotFound, traceId = None)

final case class ClusterResourceConstraintsException(clusterProjectAndName: RuntimeProjectAndName,
                                                     machineType: MachineTypeName,
                                                     region: RegionName
) extends LeoException(
      s"Unable to calculate memory constraints for cluster ${clusterProjectAndName.cloudContext}/${clusterProjectAndName.runtimeName} with master machine type ${machineType} in region ${region}",
      traceId = None
    )

final case class RegionNotSupportedException(region: RegionName, traceId: TraceId)
    extends LeoException(s"Region ${region.value} not supported for Dataproc cluster creation",
                         StatusCodes.Conflict,
                         traceId = Some(traceId)
    )

final case class GoogleGroupMembershipException(googleGroup: WorkbenchEmail, traceId: TraceId)
    extends LeoException(s"Google group ${googleGroup} failed to update in a timely manner",
                         StatusCodes.Conflict,
                         traceId = Some(traceId)
    )

class DataprocInterpreter[F[_]: Parallel](
  config: DataprocInterpreterConfig,
  bucketHelper: BucketHelper[F],
  vpcAlg: VPCAlgebra[F],
  googleDataprocService: GoogleDataprocService[F],
  googleComputeService: GoogleComputeService[F],
  googleDiskService: GoogleDiskService[F],
  googleDirectoryDAO: GoogleDirectoryDAO,
  googleIamDAO: GoogleIamDAO,
  googleResourceService: GoogleResourceService[F],
  welderDao: WelderDAO[F]
)(implicit
  val F: Async[F],
  executionContext: ExecutionContext,
  metrics: OpenTelemetryMetrics[F],
  logger: StructuredLogger[F],
  dbRef: DbReference[F]
) extends BaseRuntimeInterpreter[F](config, welderDao, bucketHelper)
    with RuntimeAlgebra[F]
    with LazyLogging {

  import dbRef._
  val isMemberofGroupDoneCheckable = new DoneCheckable[Boolean] {
    override def isDone(a: Boolean): Boolean = a
  }

  override def createRuntime(
    params: CreateRuntimeParams
  )(implicit ev: Ask[F, AppContext]): F[Option[CreateGoogleRuntimeResponse]] = {
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
      googleProject <- F.fromOption(
        LeoLenses.cloudContextToGoogleProject.get(params.runtimeProjectAndName.cloudContext),
        new RuntimeException("this should never happen. Dataproc runtime's cloud context should be a google project")
      )
      initBucketName = generateUniqueBucketName("leoinit-" + params.runtimeProjectAndName.runtimeName.asString)
      stagingBucketName = generateUniqueBucketName("leostaging-" + params.runtimeProjectAndName.runtimeName.asString)

      createOp = for {
        // Set up VPC network and firewall
        (_, subnetwork) <- vpcAlg.setUpProjectNetworkAndFirewalls(
          SetUpProjectNetworkParams(googleProject, machineConfig.region)
        )

        // Add member to the Google Group that has the IAM role to pull the Dataproc image
        _ <- updateDataprocImageGroupMembership(googleProject, createCluster = true)

        // Set up IAM roles for the pet service account necessary to create a cluster.
        _ <- createClusterIamRoles(googleProject, params.serviceAccountInfo)

        // Create the bucket in the cluster's google project and populate with initialization files.
        // ACLs are granted so the cluster service account can access the files at initialization time.
        _ <- bucketHelper
          .createInitBucket(googleProject, initBucketName, params.serviceAccountInfo)
          .compile
          .drain

        // Create the cluster staging bucket. ACLs are granted so the user/pet can access it.
        _ <- bucketHelper
          .createStagingBucket(params.auditInfo.creator, googleProject, stagingBucketName, params.serviceAccountInfo)
          .compile
          .drain

        jupyterResourceConstraints <- getDataprocRuntimeResourceContraints(params.runtimeProjectAndName,
                                                                           machineConfig.masterMachineType,
                                                                           machineConfig.region
        )

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
          Some(jupyterResourceConstraints),
          false
        )
        templateValues = RuntimeTemplateValues(templateParams, Some(ctx.now), false)
        _ <- bucketHelper
          .initializeBucketObjects(initBucketName,
                                   templateParams.serviceAccountKey,
                                   templateValues,
                                   params.customEnvironmentVariables,
                                   config.clusterResourcesConfig,
                                   None
          )
          .compile
          .drain

        // build cluster configuration
        initScriptResources = List(config.clusterResourcesConfig.initScript)
        initScripts = initScriptResources.map(resource => GcsPath(initBucketName, GcsObjectName(resource.asString)))

        // If we need to support 2 version of dataproc custom image, we'll update this
//        dataprocImage = config.dataprocConfig.customDataprocImage

        // We need to maintain the old version of the dataproc image to uncouple the terra from the aou release
        imageUrls = params.runtimeImages.map(_.imageUrl)
        dataprocImage = config.dataprocConfig.customDataprocImage

        // If the cluster is configured with worker private access, then specify the
        // `leonardo-private` network tag. This tag will be removed from the master node
        // once the cluster is running.
        tags =
          if (machineConfig.workerPrivateAccess) {
            List(config.vpcConfig.networkTag.value, config.vpcConfig.privateAccessNetworkTag.value)
          } else {
            List(config.vpcConfig.networkTag.value)
          }

        gceClusterConfig = {
          val bldr = GceClusterConfig
            .newBuilder()
            .addAllTags(tags.asJava)
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

        (workerConfig, secondaryWorkerConfig) =
          if (machineConfig.numberOfWorkers > 0) {
            (machineConfig.workerMachineType,
             machineConfig.workerDiskSize,
             machineConfig.numberOfWorkerLocalSSDs,
             machineConfig.numberOfPreemptibleWorkers
            )
              .mapN { case (machineType, diskSize, numLocalSSDs, numPreemptibles) =>
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

        sparkDriverMemory <- jupyterResourceConstraints.driverMemory match {
          case Some(value) => F.pure(value)
          case None =>
            F.raiseError[MemorySizeBytes](
              new RuntimeException(
                s"Spark driver memory must be specified for DataprocInterpreter. This should never happen"
              )
            )
        }
        softwareConfig = getSoftwareConfig(googleProject,
                                           params.runtimeProjectAndName.runtimeName,
                                           machineConfig,
                                           sparkDriverMemory
        )

        // Enables Dataproc Component Gateway. Used for enabling cluster web UIs.
        // See https://cloud.google.com/dataproc/docs/concepts/accessing/dataproc-gateways
        endpointConfig = EndpointConfig
          .newBuilder()
          .setEnableHttpPortAccess(machineConfig.componentGatewayEnabled)
          .build()

        createClusterConfig = CreateClusterConfig(
          gceClusterConfig,
          nodeInitializationActions,
          masterConfig,
          workerConfig,
          secondaryWorkerConfig,
          stagingBucketName,
          softwareConfig,
          endpointConfig
        )

        op <- googleDataprocService.createCluster(
          googleProject,
          machineConfig.region,
          DataprocClusterName(params.runtimeProjectAndName.runtimeName.asString),
          Some(createClusterConfig)
        )

        asyncRuntimeFields = op.map(o =>
          AsyncRuntimeFields(
            ProxyHostName(o.metadata.getClusterUuid),
            o.name,
            stagingBucketName,
            None
          )
        )

        // If the cluster is configured with worker private access, then remove the private access
        // network tag from the master node as soon as possible.
        //
        // We intentionally do this here instead of DataprocRuntimeMonitor to more quickly remove
        // the tag because the init script may depend on public Internet access.
        _ <-
          if (machineConfig.workerPrivateAccess) {
            val op = for {
              dataprocInstances <- googleDataprocService
                .getClusterInstances(
                  googleProject,
                  machineConfig.region,
                  DataprocClusterName(params.runtimeProjectAndName.runtimeName.asString)
                )
              masterComputeInstanceAndZone <- dataprocInstances.find(_._1.role == DataprocRole.Master).flatTraverse {
                case (DataprocRoleZonePreemptibility(_, z, _), instances) =>
                  instances.headOption
                    .flatTraverse(i => googleComputeService.getInstance(googleProject, z, i))
                    .map(_.map(i => (i, z)))
              }
              op <- masterComputeInstanceAndZone.traverse { case (instance, zone) =>
                googleComputeService.setInstanceTags(
                  googleProject,
                  zone,
                  InstanceName(instance.getName),
                  Tags
                    .newBuilder()
                    .addItems(config.vpcConfig.networkTag.value)
                    .setFingerprint(instance.getTags.getFingerprint)
                    .build()
                )
              }
            } yield op

            streamUntilDoneOrTimeout(op, 60, 1 second, "Could not retrieve Dataproc master instance after 1 minute")
          } else F.unit
      } yield asyncRuntimeFields.map(s =>
        CreateGoogleRuntimeResponse(s, initBucketName, BootSource.VmImage(dataprocImage))
      )

      res <- createOp.handleErrorWith { throwable =>
        cleanUpGoogleResourcesOnError(
          googleProject,
          params.runtimeProjectAndName.runtimeName,
          initBucketName,
          params.serviceAccountInfo,
          machineConfig.region
        ) >> F.raiseError[Option[CreateGoogleRuntimeResponse]](throwable)
      }
    } yield res
  }

  override def deleteRuntime(
    params: DeleteRuntimeParams
  )(implicit
    ev: Ask[F, AppContext]
  ): F[Option[OperationFuture[Operation, Operation]]] =
    if (params.runtimeAndRuntimeConfig.runtime.asyncRuntimeFields.isDefined) { // check if runtime has been created
      for {
        ctx <- ev.ask
        region <- F.fromOption(
          LeoLenses.dataprocRegion.getOption(params.runtimeAndRuntimeConfig.runtimeConfig),
          new RuntimeException("DataprocInterpreter shouldn't get a GCE request")
        )
        metadata <- getShutdownScript(params.runtimeAndRuntimeConfig, false)
        _ <- params.masterInstance.traverse { instance =>
          for {
            opFutureAttempt <- googleComputeService
              .addInstanceMetadata(instance.key.project, instance.key.zone, instance.key.name, metadata)
              .attempt
            _ <- opFutureAttempt match {
              case Left(e) if e.getMessage.contains("Instance not found") =>
                logger.info(ctx.loggingCtx)("Instance is already deleted").as(None)
              case Left(e) =>
                F.raiseError(e)
              case Right(opFuture) =>
                val deleteDatprocCluster = for {
                  googleProject <- F.fromOption(
                    LeoLenses.cloudContextToGoogleProject.get(params.runtimeAndRuntimeConfig.runtime.cloudContext),
                    new RuntimeException(
                      "this should never happen. Dataproc runtime's cloud context should be a google project"
                    )
                  )
                  _ <- googleDataprocService.deleteCluster(
                    googleProject,
                    region,
                    DataprocClusterName(params.runtimeAndRuntimeConfig.runtime.runtimeName.asString)
                  )
                } yield ()

                opFuture match {
                  case None => deleteDatprocCluster
                  case Some(v) =>
                    for {
                      res <- F.delay(v.get())
                      _ <- F.raiseUnless(google2.isSuccess(res.getHttpErrorStatusCode))(
                        new Exception(s"addInstanceMetadata failed")
                      )
                      _ <- deleteDatprocCluster
                    } yield ()
                }
            }
          } yield ()

        }
      } yield None
    } else F.pure(None)

  override def finalizeDelete(params: FinalizeDeleteParams)(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      googleProject <- F.fromOption(
        LeoLenses.cloudContextToGoogleProject.get(params.runtime.cloudContext),
        new RuntimeException("this should never happen. Dataproc runtime's cloud context should be a google project")
      )
      _ <- removeClusterIamRoles(googleProject, params.runtime.serviceAccount)
      _ <- updateDataprocImageGroupMembership(googleProject, createCluster = false)
    } yield ()

  override protected def stopGoogleRuntime(params: StopGoogleRuntime)(implicit
    ev: Ask[F, AppContext]
  ): F[Option[OperationFuture[Operation, Operation]]] =
    for {
      ctx <- ev.ask
      region <- F.fromOption(
        LeoLenses.dataprocRegion.getOption(params.runtimeAndRuntimeConfig.runtimeConfig),
        new RuntimeException("DataprocInterpreter shouldn't get a GCE request")
      )
      metadata <- getShutdownScript(params.runtimeAndRuntimeConfig, false)
      googleProject <- F.fromOption(
        LeoLenses.cloudContextToGoogleProject.get(params.runtimeAndRuntimeConfig.runtime.cloudContext),
        new RuntimeException("this should never happen. Dataproc runtime's cloud context should be a google project")
      )
      _ <- googleDataprocService
        .stopCluster(
          googleProject,
          region,
          DataprocClusterName(params.runtimeAndRuntimeConfig.runtime.runtimeName.asString),
          Some(metadata),
          params.isDataprocFullStop
        )
        .recoverWith { case _: com.google.api.gax.rpc.PermissionDeniedException =>
          logger
            .info(ctx.loggingCtx)(s"Leo SA can't access the project. ${googleProject} might've been deleted.")
            .as(None)
        }
    } yield None

  override protected def startGoogleRuntime(params: StartGoogleRuntime)(implicit
    ev: Ask[F, AppContext]
  ): F[Option[OperationFuture[Operation, Operation]]] =
    for {
      dataprocConfig <- F.fromOption(
        LeoLenses.dataprocPrism.getOption(params.runtimeAndRuntimeConfig.runtimeConfig),
        new RuntimeException("DataprocInterpreter shouldn't get a GCE request")
      )
      googleProject <- F.fromOption(
        LeoLenses.cloudContextToGoogleProject.get(params.runtimeAndRuntimeConfig.runtime.cloudContext),
        new RuntimeException("this should never happen. Dataproc runtime's cloud context should be a google project")
      )
      resourceConstraints <- getDataprocRuntimeResourceContraints(
        RuntimeProjectAndName(params.runtimeAndRuntimeConfig.runtime.cloudContext,
                              params.runtimeAndRuntimeConfig.runtime.runtimeName
        ),
        params.runtimeAndRuntimeConfig.runtimeConfig.machineType,
        dataprocConfig.region
      )
      metadata <- getStartupScript(params.runtimeAndRuntimeConfig,
                                   params.welderAction,
                                   params.initBucket,
                                   resourceConstraints,
                                   false
      )

      _ <- googleDataprocService.startCluster(
        googleProject,
        dataprocConfig.region,
        DataprocClusterName(params.runtimeAndRuntimeConfig.runtime.runtimeName.asString),
        dataprocConfig.numberOfPreemptibleWorkers,
        Some(metadata)
      )
    } yield None

  override def resizeCluster(params: ResizeClusterParams)(implicit ev: Ask[F, AppContext]): F[Unit] =
    (for {
      region <- F.fromOption(
        LeoLenses.dataprocRegion.getOption(params.runtimeAndRuntimeConfig.runtimeConfig),
        new RuntimeException("DataprocInterpreter shouldn't get a GCE request")
      )
      googleProject <- F.fromOption(
        LeoLenses.cloudContextToGoogleProject.get(params.runtimeAndRuntimeConfig.runtime.cloudContext),
        new RuntimeException("this should never happen. Dataproc runtime's cloud context should be a google project")
      )
      // IAM roles should already exist for a non-deleted cluster; this method is a no-op if the roles already exist.
      _ <- createClusterIamRoles(googleProject, params.runtimeAndRuntimeConfig.runtime.serviceAccount)

      _ <- updateDataprocImageGroupMembership(googleProject, createCluster = true)

      // Resize the cluster in Google
      _ <- googleDataprocService.resizeCluster(
        googleProject,
        region,
        DataprocClusterName(params.runtimeAndRuntimeConfig.runtime.runtimeName.asString),
        params.numWorkers,
        params.numPreemptibles
      )
    } yield ()) recoverWith { case e: ApiException =>
      // Typically we will revoke this role in the monitor after everything is complete, but if Google fails to
      // resize the cluster we need to revoke it manually here
      for {
        ctx <- ev.ask
        googleProject <- F.fromOption(
          LeoLenses.cloudContextToGoogleProject.get(params.runtimeAndRuntimeConfig.runtime.cloudContext),
          new RuntimeException(
            "this should never happen. Dataproc runtime's cloud context should be a google project"
          )
        )
        _ <- removeClusterIamRoles(googleProject, params.runtimeAndRuntimeConfig.runtime.serviceAccount)
        // Remove member from the Google Group that has the IAM role to pull the Dataproc image
        _ <- updateDataprocImageGroupMembership(googleProject, createCluster = false)
        _ <- logger.error(ctx.loggingCtx, e)(
          s"Could not successfully update cluster ${params.runtimeAndRuntimeConfig.runtime.projectNameString}"
        )
        _ <- F.raiseError[Unit](InvalidDataprocMachineConfigException(e.getMessage))
      } yield ()
    }

  // updates machine type in gdDAO
  override protected def setMachineTypeInGoogle(params: SetGoogleMachineType)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    params.masterInstance
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
                      p.diskSize.gb
          )
      }

  def createClusterIamRoles(googleProject: GoogleProject, serviceAccountInfo: WorkbenchEmail): F[Unit] =
    updateClusterIamRoles(googleProject, serviceAccountInfo, createCluster = true)

  def removeClusterIamRoles(googleProject: GoogleProject, serviceAccountInfo: WorkbenchEmail): F[Unit] =
    updateClusterIamRoles(googleProject, serviceAccountInfo, createCluster = false)

  // Called at Leo boot time. Sets up the Dataproc image user group if it does not already exist.
  // Group membership is modified at Dataproc cluster creation time.
  def setupDataprocImageGoogleGroup(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask
      _ <- logger.info(ctx.loggingCtx)(
        s"Checking if Dataproc image user Google group '${config.groupsConfig.dataprocImageProjectGroupEmail}' already exists..."
      )
      // Check if the group exists
      groupOpt <- F.fromFuture[Option[Group]](
        F.blocking(googleDirectoryDAO.getGoogleGroup(config.groupsConfig.dataprocImageProjectGroupEmail))
      )
      // Create the group if it does not exist
      _ <- groupOpt match {
        case None =>
          F.fromFuture(
            F.delay(
              googleDirectoryDAO
                .createGroup(
                  config.groupsConfig.dataprocImageProjectGroupName,
                  config.groupsConfig.dataprocImageProjectGroupEmail,
                  Option(googleDirectoryDAO.lockedDownGroupSettings)
                )
            )
          ).handleErrorWith {
            case t if when409(t) => F.unit
            case t =>
              F.raiseError(
                GoogleGroupCreationException(config.groupsConfig.dataprocImageProjectGroupEmail, t.getMessage)
              )
          }
        case Some(group) =>
          logger.info(ctx.loggingCtx)(
            s"Dataproc image user Google group '${config.groupsConfig.dataprocImageProjectGroupEmail}' already exists: $group \n Won't attempt to create it."
          )
      }
      // Add compute.imageUser role to the group in the custom image's project
      imageProject <- F.fromOption(parseImageProject(config.dataprocConfig.customDataprocImage),
                                   ImageProjectNotFoundException
      )
      _ <- logger.info(ctx.loggingCtx)(
        s"Attempting to grant 'compute.imageUser' permissions to '${config.groupsConfig.dataprocImageProjectGroupEmail}' on project '$imageProject' ..."
      )
      _ <- retry(
        F.fromFuture(
          F.delay(
            googleIamDAO.addRoles(imageProject,
                                  config.groupsConfig.dataprocImageProjectGroupEmail,
                                  IamMemberTypes.Group,
                                  Set("roles/compute.imageUser")
            )
          )
        ),
        when409
      )
    } yield ()

  /**
   * Add the user's service account to the Google group.
   * This group has compute.imageUser role on the custom Dataproc image project,
   * which allows the user's cluster to pull the image.
   */
  def updateDataprocImageGroupMembership(googleProject: GoogleProject, createCluster: Boolean)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      count <- inTransaction(clusterQuery.countActiveByProject(CloudContext.Gcp(googleProject)))
      // Note: Don't remove the account if there are existing active clusters in the same project,
      // because it could potentially break other clusters. We only check this for the 'remove' case.
      _ <-
        if (count > 0 && !createCluster) {
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
            // See https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/service-accounts#dataproc_service_accounts_2
            // for more information
            dataprocServiceAccountEmail = WorkbenchEmail(
              s"service-${projectNumber}@dataproc-accounts.iam.gserviceaccount.com"
            )
            _ <- updateGroupMembership(config.groupsConfig.dataprocImageProjectGroupEmail,
                                       dataprocServiceAccountEmail,
                                       createCluster
            )
            apiServiceAccountEmail = WorkbenchEmail(
              s"${projectNumber}@cloudservices.gserviceaccount.com"
            )
            _ <- updateGroupMembership(config.groupsConfig.dataprocImageProjectGroupEmail,
                                       apiServiceAccountEmail,
                                       createCluster
            )
          } yield ()
        }
    } yield ()

  private[util] def waitUntilMemberAdded(memberEmail: WorkbenchEmail)(implicit ev: Ask[F, AppContext]): F[Boolean] = {
    implicit val doneCheckable = isMemberofGroupDoneCheckable
    val checkMemberWithLogs = for {
      ctx <- ev.ask
      isMember <- F.fromFuture(
        F.blocking(
          googleDirectoryDAO.isGroupMember(config.groupsConfig.dataprocImageProjectGroupEmail, memberEmail)
        )
      )
      _ <- logger.info(ctx.loggingCtx)(
        s"Is ${memberEmail.value} a member of ${config.groupsConfig.dataprocImageProjectGroupEmail.value}? ${isMember}"
      )
    } yield isMember

    F.sleep(config.groupsConfig.waitForMemberAddedPollConfig.initialDelay) >> streamUntilDoneOrTimeout(
      checkMemberWithLogs,
      config.groupsConfig.waitForMemberAddedPollConfig.maxAttempts,
      config.groupsConfig.waitForMemberAddedPollConfig.interval,
      s"fail to add ${memberEmail.value} to ${config.groupsConfig.dataprocImageProjectGroupEmail.value}"
    )
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
        _ <- googleDataprocService
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

  private[leonardo] def getDataprocRuntimeResourceContraints(runtimeProjectAndName: RuntimeProjectAndName,
                                                             machineType: MachineTypeName,
                                                             region: RegionName
  )(implicit
    ev: Ask[F, AppContext]
  ): F[RuntimeResourceConstraints] =
    for {
      ctx <- ev.ask
      googleProject <- F.fromOption(
        LeoLenses.cloudContextToGoogleProject.get(runtimeProjectAndName.cloudContext),
        new RuntimeException("this should never happen. Dataproc runtime's cloud context should be a google project")
      )
      // Find an arbitrary zone in the configured region in which to query the machine type
      zones <- googleComputeService.getZones(googleProject, region)
      zoneUri <- F.fromOption(zones.headOption.map(z => ZoneName(z.getName)),
                              ClusterResourceConstraintsException(runtimeProjectAndName, machineType, region)
      )
      _ <- logger.debug(ctx.loggingCtx)(s"Using zone ${zoneUri} to resolve machine type")

      // Resolve the master machine type in Google to get the total memory.
      resolvedMachineTypeOpt <- googleComputeService.getMachineType(googleProject, zoneUri, machineType)
      resolvedMachineType <- F.fromOption(
        resolvedMachineTypeOpt,
        ClusterResourceConstraintsException(runtimeProjectAndName, machineType, region)
      )
      _ <- logger.debug(ctx.loggingCtx)(s"Resolved machine type: ${resolvedMachineType.toString}")
      total = MemorySizeBytes.fromMb(resolvedMachineType.getMemoryMb.toDouble)

      sparkDriverMemory <- machineType match {
        case MachineTypeName(n1standard) if n1standard.startsWith("n1-standard") =>
          F.pure(MemorySizeBytes.fromGb((total.bytes / MemorySizeBytes.gbInBytes - 7) * 0.9))
        case MachineTypeName(n1highmem) if n1highmem.startsWith("n1-highmem") =>
          F.pure(MemorySizeBytes.fromGb((total.bytes / MemorySizeBytes.gbInBytes - 11) * 0.9))
        case x =>
          F.raiseError(
            new RuntimeException(
              s"Machine type (${x.value}) not supported by Hail. Consider use `n1-highmem` machine type"
            )
          )
      }
    } yield {
      // For dataproc, we don't need much memory for Jupyter.

      // We still want a minimum to run Jupyter and other system processes.
      val minRuntimeMemoryGb = MemorySizeBytes.fromGb(config.dataprocConfig.minimumRuntimeMemoryInGb.getOrElse(4.0))
      // Note this algorithm is recommended by Hail team. See more info in https://broadworkbench.atlassian.net/browse/IA-4720

      val runtimeAllocatedMemory = MemorySizeBytes(
        sparkDriverMemory.bytes + minRuntimeMemoryGb.bytes
      )
      // Setting the shared docker memory to 50% of the allocated memory limit, converting from byte to mb
      val shmSize = MemorySizeMegaBytes.fromB(0.5 * runtimeAllocatedMemory.bytes)
      RuntimeResourceConstraints(runtimeAllocatedMemory, shmSize, MemorySizeBytes(total.bytes), Some(sparkDriverMemory))
    }

  /**
   * Add the Dataproc Worker role in the user's project to the cluster service account, if present.
   * This is needed to be able to spin up Dataproc clusters using a custom service account.
   * If the Google Compute default service account is being used, this is not necessary.
   */
  private def updateClusterIamRoles(googleProject: GoogleProject,
                                    serviceAccountInfo: WorkbenchEmail,
                                    createCluster: Boolean
  ): F[Unit] = {
    def retryIam(project: GoogleProject, email: WorkbenchEmail, roles: Set[String]): F[Unit] = {
      val action = if (createCluster) {

        F.fromFuture(F.delay(googleIamDAO.addRoles(project, email, IamMemberTypes.ServiceAccount, roles).void))
      } else {

        F.fromFuture(F.delay(googleIamDAO.removeRoles(project, email, IamMemberTypes.ServiceAccount, roles).void))
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

  private def updateGroupMembership(groupEmail: WorkbenchEmail, memberEmail: WorkbenchEmail, addToGroup: Boolean)(
    implicit ev: Ask[F, AppContext]
  ): F[Unit] = {
    val checkIsMember = retry(
      F.fromFuture(F.blocking(googleDirectoryDAO.isGroupMember(groupEmail, memberEmail))),
      when409
    )
    val addMemberToGroup = retry(
      F.fromFuture(F.blocking(googleDirectoryDAO.addMemberToGroup(groupEmail, memberEmail))),
      when409
    )
    val removeMemberFromGroup = retry(
      F.fromFuture(F.blocking(googleDirectoryDAO.removeMemberFromGroup(groupEmail, memberEmail))),
      when409
    )
    for {
      ctx <- ev.ask
      // Add or remove the member from the group
      isMember <- checkIsMember
      _ <- (isMember, addToGroup) match {
        case (false, true) =>
          // Sometimes adding member to a group can take longer than when it gets to the point when we create dataproc cluster.
          // Hence add polling here to make sure the 2 service accounts are added to the image user group properly before proceeding
          logger.info(ctx.loggingCtx)(s"Adding '$memberEmail' to group '$groupEmail'...") >>
            addMemberToGroup >> waitUntilMemberAdded(memberEmail)
        case (true, false) =>
          logger.info(ctx.loggingCtx)(s"Removing '$memberEmail' from group '$groupEmail'...") >>
            removeMemberFromGroup
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

  // This file is worth keeping track of. It's the source of truth for hail machine provisioning
  // https://github.com/hail-is/hail/blob/75f351d43a6c6b2e87e7d37531be63aaf11af9df/hail/python/hailtop/hailctl/dataproc/start.py#L20
  def getSoftwareConfig(googleProject: GoogleProject,
                        runtimeName: RuntimeName,
                        machineConfig: RuntimeConfig.DataprocConfig,
                        sparkDriverMemory: MemorySizeBytes
  ): SoftwareConfig = {
    val dataprocProps = if (machineConfig.numberOfWorkers == 0) {
      // Set a SoftwareConfig property that makes the cluster have only one node
      Map("dataproc:dataproc.allow.zero.workers" -> "true")
    } else Map.empty[String, String]

    val driverMemoryProp = Map(
      "spark:spark.driver.memory" -> s"${sparkDriverMemory.bytes / MemorySizeBytes.mbInBytes}m"
    )

    val yarnProps = Map(
      // Helps with debugging
      "yarn:yarn.log-aggregation-enable" -> "true",
      // Allows submitting jobs through the YARN Resource Manager web UI
      "yarn:yarn.resourcemanager.webapp.methods-allowed" -> "ALL"
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

    val knoxProps =
      if (machineConfig.componentGatewayEnabled)
        Map(
          "knox:gateway.path" -> s"proxy/${googleProject.value}/${runtimeName.asString}/gateway"
        )
      else Map.empty

    SoftwareConfig
      .newBuilder()
      .putAllProperties(
        (driverMemoryProp ++ dataprocProps ++ yarnProps ++ stackdriverProps ++ requesterPaysProps ++ knoxProps ++ machineConfig.properties).asJava
      )
      .addOptionalComponents(Component.DOCKER)
      .build()
  }

  implicit private def optionDoneCheckable[A]: DoneCheckable[Option[A]] = (a: Option[A]) => a.isDefined
}
