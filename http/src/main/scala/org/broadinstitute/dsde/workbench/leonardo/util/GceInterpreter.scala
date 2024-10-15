package org.broadinstitute.dsde.workbench.leonardo
package util

import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import com.google.api.gax.longrunning.OperationFuture
import com.google.cloud.compute.v1._
import org.broadinstitute.dsde.workbench
import org.broadinstitute.dsde.workbench.google2.{
  isSuccess,
  GoogleComputeService,
  GoogleDiskService,
  MachineTypeName,
  OperationName,
  RegionName,
  SubnetworkName,
  ZoneName
}
import org.broadinstitute.dsde.workbench.leonardo.config.ClusterResourcesConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.WelderDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.google._
import org.broadinstitute.dsde.workbench.leonardo.db.{persistentDiskQuery, DbReference}
import org.broadinstitute.dsde.workbench.leonardo.http.{ctxConversion, dbioToIO, userScriptStartupOutputUriMetadataKey}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.RuntimeConfigInCreateRuntimeMessage
import org.broadinstitute.dsde.workbench.leonardo.util.RuntimeInterpreterConfig.GceInterpreterConfig
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.{generateUniqueBucketName, GcsObjectName, GcsPath, GoogleProject}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.broadinstitute.dsde.workbench.util2.InstanceName
import org.typelevel.log4cats.StructuredLogger

import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

final case class InstanceResourceConstraintsException(project: GoogleProject, machineType: MachineTypeName)
    extends LeoException(
      s"Unable to calculate memory constraints for instance in project ${project.value} with machine type ${machineType.value}",
      traceId = None
    )

final case class MissingServiceAccountException(projectAndName: RuntimeProjectAndName)
    extends LeoException(s"Cannot create instance ${projectAndName}: service account required", traceId = None)

class GceInterpreter[F[_]](
  config: GceInterpreterConfig,
  bucketHelper: BucketHelper[F],
  vpcAlg: VPCAlgebra[F],
  googleComputeService: GoogleComputeService[F],
  googleDiskService: GoogleDiskService[F],
  welderDao: WelderDAO[F]
)(implicit
  val executionContext: ExecutionContext,
  metrics: OpenTelemetryMetrics[F],
  dbRef: DbReference[F],
  F: Async[F],
  logger: StructuredLogger[F]
) extends BaseRuntimeInterpreter[F](config, welderDao, bucketHelper)
    with RuntimeAlgebra[F] {
  override def createRuntime(
    params: CreateRuntimeParams
  )(implicit ev: Ask[F, AppContext]): F[Option[CreateGoogleRuntimeResponse]] =
    // TODO clean up on error
    for {
      ctx <- ev.ask

      (zoneParam, gpuConfig) <- params.runtimeConfig match {
        case x: RuntimeConfigInCreateRuntimeMessage.GceWithPdConfig => F.pure((x.zone, x.gpuConfig))
        case x: RuntimeConfigInCreateRuntimeMessage.GceConfig       => F.pure((x.zone, x.gpuConfig))
        case _ =>
          F.raiseError[(ZoneName, Option[GpuConfig])](
            new RuntimeException(
              "GceInterpreter shouldn't get a dataproc runtime creation request. Something is very wrong"
            )
          )
      }
      googleProject <- F.fromOption(
        LeoLenses.cloudContextToGoogleProject.get(params.runtimeProjectAndName.cloudContext),
        new RuntimeException("this should never happen. GCE runtime's cloud context should be a google project")
      )
      // We get region by removing the last two characters of zone
      regionParam = RegionName(zoneParam.value.substring(0, zoneParam.value.length - 2))

      // Set up VPC and firewall
      (_, subnetwork) <- vpcAlg.setUpProjectNetworkAndFirewalls(
        SetUpProjectNetworkParams(googleProject, regionParam)
      )

      // Get resource (e.g. memory) constraints for the instance
      resourceConstraints <- getResourceConstraints(googleProject, zoneParam, params.runtimeConfig.machineType)

      // Create the bucket in the cluster's google project and populate with initialization files.
      // ACLs are granted so the cluster service account can access the files at initialization time.
      initBucketName = generateUniqueBucketName("leoinit-" + params.runtimeProjectAndName.runtimeName.asString)
      stagingBucketName = generateUniqueBucketName("leostaging-" + params.runtimeProjectAndName.runtimeName.asString)

      _ <- bucketHelper
        .createInitBucket(googleProject, initBucketName, params.serviceAccountInfo)
        .compile
        .drain

      _ <- bucketHelper
        .createStagingBucket(params.auditInfo.creator, googleProject, stagingBucketName, params.serviceAccountInfo)
        .compile
        .drain

      initScript = GcsPath(initBucketName, GcsObjectName(config.clusterResourcesConfig.initScript.asString))
      cloudInit <- F
        .fromOption(config.clusterResourcesConfig.cloudInit,
                    new LeoException("No cloud init file defined for GCE VM.", traceId = Some(ctx.traceId))
        )
      cloudInitFileContent = scala.io.Source
        .fromResource(s"${ClusterResourcesConfig.basePath}/${cloudInit.asString}")
        .getLines()
        .toList
        .mkString("\n")

      // TODO: update bootDiskSize
      bootDiskSize <- params.runtimeConfig match {
        case x: RuntimeConfigInCreateRuntimeMessage.GceWithPdConfig => F.pure(x.bootDiskSize)
        case x: RuntimeConfigInCreateRuntimeMessage.GceConfig       => F.pure(x.bootDiskSize)
        case _ =>
          F.raiseError[DiskSize](
            new RuntimeException(
              "GceInterpreter shouldn't get a dataproc runtime creation request. Something is very wrong"
            )
          )
      }
      bootDisk = AttachedDisk.newBuilder
        .setBoot(true)
        .setInitializeParams(
          AttachedDiskInitializeParams
            .newBuilder()
            .setDescription("Leonardo Managed Boot Disk")
            .setSourceImage(config.gceConfig.sourceImage.asString)
            .setDiskSizeGb(bootDiskSize.gb)
            .putAllLabels(Map("leonardo" -> "true").asJava)
            .build()
        )
        .setAutoDelete(true)
        .build()

      (userDisk, isFormatted) <- params.runtimeConfig match {
        case x: RuntimeConfigInCreateRuntimeMessage.GceWithPdConfig =>
          for {
            persistentDiskOpt <- persistentDiskQuery.getById(x.persistentDiskId).transaction
            persistentDisk <- F.fromEither(
              persistentDiskOpt.toRight(
                new Exception(
                  "Runtime has Persistent Disk enabled, but we can't find it in database. This should never happen."
                )
              )
            )
            isFormatted <- persistentDisk.formattedBy match {
              case Some(FormattedBy.Galaxy) | Some(FormattedBy.Custom) | Some(FormattedBy.Cromwell) | Some(
                    FormattedBy.Allowed
                  ) | Some(FormattedBy.Jupyter) =>
                F.raiseError[Boolean](
                  new RuntimeException(
                    s"Trying to use an app formatted disk for creating GCE runtime. This should never happen. Disk Id: ${x.persistentDiskId}."
                  )
                )
              case Some(FormattedBy.GCE) => F.pure(true)
              case None                  => F.pure(false)
            }
          } yield {
            val disk = AttachedDisk.newBuilder
              .setBoot(false)
              .setDeviceName(config.gceConfig.userDiskDeviceName.asString)
              .setInitializeParams(
                AttachedDiskInitializeParams
                  .newBuilder()
                  .setDiskName(persistentDisk.name.value)
                  .setDiskSizeGb(persistentDisk.size.gb)
                  .putAllLabels(Map("leonardo" -> "true").asJava)
                  .setDiskType(
                    persistentDisk.diskType.googleString(googleProject, persistentDisk.zone)
                  )
                  .build()
              )
              .setAutoDelete(false)
              .build()
            (disk, isFormatted)
          }
        case x: RuntimeConfigInCreateRuntimeMessage.GceConfig =>
          val disk = AttachedDisk.newBuilder
            .setBoot(false)
            .setDeviceName("user-disk")
            .setInitializeParams(
              AttachedDiskInitializeParams
                .newBuilder()
                .setDiskSizeGb(x.diskSize.gb)
                .build()
            )
            .setAutoDelete(true)
            .build()
          F.pure((disk, false))
        case _: RuntimeConfigInCreateRuntimeMessage.DataprocConfig =>
          F.raiseError[(AttachedDisk, Boolean)](
            new Exception("This is wrong! GceInterpreter shouldn't get a dataproc runtimeConfig request")
          )
      }

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
        isFormatted
      )

      templateValues = RuntimeTemplateValues(templateParams, Some(ctx.now), false)

      _ <- bucketHelper
        .initializeBucketObjects(initBucketName,
                                 templateParams.serviceAccountKey,
                                 templateValues,
                                 params.customEnvironmentVariables,
                                 config.clusterResourcesConfig,
                                 gpuConfig
        )
        .compile
        .drain
      instanceBuilder = Instance
        .newBuilder()
        .setName(params.runtimeProjectAndName.runtimeName.asString)
        .setDescription("Leonardo Managed VM")
        .setTags(Tags.newBuilder().addItems(config.vpcConfig.networkTag.value).build())
        .setMachineType(buildMachineTypeUri(zoneParam, params.runtimeConfig.machineType))
        .addNetworkInterfaces(
          buildNetworkInterfaces(params.runtimeProjectAndName, subnetwork, zoneParam, googleProject)
        )
        .addAllDisks(
          List(bootDisk, userDisk).asJava
        )
        .addServiceAccounts(
          ServiceAccount.newBuilder
            .setEmail(params.serviceAccountInfo.value)
            .addAllScopes(params.scopes.toList.asJava)
            .build()
        )
        .setMetadata(
          Metadata.newBuilder
            .addItems(
              Items.newBuilder
                .setKey("google-logging-enabled")
                .setValue("true")
                .build()
            )
            .addItems(
              Items.newBuilder
                .setKey(userScriptStartupOutputUriMetadataKey)
                .setValue(templateValues.startUserScriptOutputUri)
                .build()
            )
            .addItems(
              Items.newBuilder
                .setKey("user-data")
                .setValue(
                  cloudInitFileContent
                ) // cloud init log can be found on the instance at /var/log/cloud-init-output.log
                .build()
            )
            .addItems(
              Items.newBuilder
                .setKey("cos-update-strategy")
                .setValue(
                  "update_disabled"
                ) // `cos-extension install gpu` is only supported on LTS versions. Auto update will update the instance to non lts version
                .build()
            )
            .addItems(
              Items.newBuilder
                .setKey("startup-script-url")
                .setValue(initScript.toUri)
                .build()
            )
            .build()
        )
        .putAllLabels(Map("leonardo" -> "true").asJava)

      instance = gpuConfig match {
        case Some(gc) =>
          val acceleratorType =
            s"projects/${googleProject.value}/zones/${zoneParam.value}/acceleratorTypes/${gc.gpuType.asString}"
          instanceBuilder
            .addGuestAccelerators(
              AcceleratorConfig
                .newBuilder()
                .setAcceleratorType(acceleratorType)
                .setAcceleratorCount(gc.numOfGpus)
                .build()
            )
            .setScheduling(
              Scheduling
                .newBuilder()
                .setOnHostMaintenance(com.google.cloud.compute.v1.Scheduling.OnHostMaintenance.TERMINATE.toString)
                .build()
            )
            // .setShieldedInstanceConfig(???)  // investigate shielded VM on Albano's recommendation
            .build()
        case None => instanceBuilder.build()

      }

      operation <- googleComputeService
        .createInstance(googleProject, zoneParam, instance)

      hostname <- F.delay(UUID.randomUUID().getLeastSignificantBits.toHexString)

//       When there's already an on-ging create request, `.getName` will error with `Conflict`
      opName <- operation.flatTraverse { op =>
        F.delay(op.getName.some)
          .recoverWith {
            case e: java.util.concurrent.ExecutionException if e.getMessage.contains("Conflict") =>
              F.pure(none[String])
          }
      }

      res = opName.map(name =>
        CreateGoogleRuntimeResponse(
          AsyncRuntimeFields(ProxyHostName(hostname), OperationName(name), stagingBucketName, None),
          initBucketName,
          BootSource.VmImage(config.gceConfig.sourceImage)
        )
      )
    } yield res

  override protected def stopGoogleRuntime(params: StopGoogleRuntime)(implicit
    ev: Ask[F, AppContext]
  ): F[Option[OperationFuture[Operation, Operation]]] =
    for {
      ctx <- ev.ask
      runtimeName = params.runtimeAndRuntimeConfig.runtime.runtimeName.asString
      zoneParam <- F.fromOption(
        LeoLenses.gceZone.getOption(params.runtimeAndRuntimeConfig.runtimeConfig),
        new RuntimeException(
          "GceInterpreter shouldn't get a stop dataproc runtime request. Something is very wrong"
        )
      )
      googleProject <- F.fromOption(
        LeoLenses.cloudContextToGoogleProject.get(params.runtimeAndRuntimeConfig.runtime.cloudContext),
        new RuntimeException("this should never happen. GCE runtime's cloud context should be a google project")
      )
      now <- F.realTimeInstant
      _ <- logger.info(
        s"StopRuntimeMessage timing: Getting shutdown script, [runtime = ${runtimeName}, traceId = ${ctx.traceId.asString},time = ${(now.toEpochMilli - ctx.now.toEpochMilli).toString}]"
      )
      metadata <- getShutdownScript(params.runtimeAndRuntimeConfig, false)
      now <- F.realTimeInstant
      _ <- logger.info(
        s"StopRuntimeMessage timing: Adding instance metadata, [runtime = ${runtimeName}, traceId = ${ctx.traceId.asString},time = ${(now.toEpochMilli - ctx.now.toEpochMilli).toString}]"
      )
      _ <- googleComputeService.addInstanceMetadata(
        googleProject,
        zoneParam,
        InstanceName(params.runtimeAndRuntimeConfig.runtime.runtimeName.asString),
        metadata
      )
      now <- F.realTimeInstant
      _ <- logger.info(
        s"StopRuntimeMessage timing: Sending stop message to google, [runtime = ${runtimeName}, traceId = ${ctx.traceId.asString},time = ${(now.toEpochMilli - ctx.now.toEpochMilli).toString}]"
      )
      opFuture <- googleComputeService.stopInstance(
        googleProject,
        zoneParam,
        InstanceName(params.runtimeAndRuntimeConfig.runtime.runtimeName.asString)
      )
    } yield Some(opFuture)

  override protected def startGoogleRuntime(params: StartGoogleRuntime)(implicit
    ev: Ask[F, AppContext]
  ): F[Option[OperationFuture[Operation, Operation]]] =
    for {
      _ <- ev.ask
      googleProject <- F.fromOption(
        LeoLenses.cloudContextToGoogleProject.get(params.runtimeAndRuntimeConfig.runtime.cloudContext),
        new RuntimeException("this should never happen. GCE runtime's cloud context should be a google project")
      )
      zoneParam <- F.fromOption(
        LeoLenses.gceZone.getOption(params.runtimeAndRuntimeConfig.runtimeConfig),
        new RuntimeException(
          "GceInterpreter shouldn't get a dataproc runtime creation request. Something is very wrong"
        )
      )
      resourceConstraints <- getResourceConstraints(googleProject,
                                                    zoneParam,
                                                    params.runtimeAndRuntimeConfig.runtimeConfig.machineType
      )
      metadata <- getStartupScript(params.runtimeAndRuntimeConfig,
                                   params.welderAction,
                                   params.initBucket,
                                   resourceConstraints,
                                   true
      )
      // remove the startup-script-url metadata entry if present which is only used at creation time
      opFutureOpt <- googleComputeService.modifyInstanceMetadata(
        googleProject,
        zoneParam,
        InstanceName(params.runtimeAndRuntimeConfig.runtime.runtimeName.asString),
        metadataToAdd = metadata,
        metadataToRemove = Set("startup-script-url")
      )

      res <- opFutureOpt match {
        case None =>
          F.raiseError(new Exception(s"${params.runtimeAndRuntimeConfig.runtime.projectNameString} not found in GCP"))
        case Some(value) =>
          for {
            res <- F.blocking(value.get())
            _ <- F.raiseUnless(workbench.google2.isSuccess(res.getHttpErrorStatusCode))(
              new Exception(s"modifyInstanceMetadata failed ${res}")
            )
            opFuture <- googleComputeService.startInstance(
              googleProject,
              zoneParam,
              InstanceName(params.runtimeAndRuntimeConfig.runtime.runtimeName.asString)
            )
          } yield opFuture
      }
    } yield res.some

  override protected def setMachineTypeInGoogle(params: SetGoogleMachineType)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      zoneParam <- F.fromOption(
        LeoLenses.gceZone.getOption(params.runtimeAndRuntimeConfig.runtimeConfig),
        new RuntimeException(
          "GceInterpreter shouldn't get a dataproc runtime creation request. Something is very wrong"
        )
      )
      googleProject <- F.fromOption(
        LeoLenses.cloudContextToGoogleProject.get(params.runtimeAndRuntimeConfig.runtime.cloudContext),
        new RuntimeException("this should never happen. GCE runtime's cloud context should be a google project")
      )
      _ <- googleComputeService.setMachineType(
        googleProject,
        zoneParam,
        InstanceName(params.runtimeAndRuntimeConfig.runtime.runtimeName.asString),
        params.machineType
      )
    } yield ()

  override def deleteRuntime(
    params: DeleteRuntimeParams
  )(implicit ev: Ask[F, AppContext]): F[Option[OperationFuture[Operation, Operation]]] =
    if (params.runtimeAndRuntimeConfig.runtime.asyncRuntimeFields.isDefined) {
      for {
        ctx <- ev.ask
        zoneParam <- F.fromOption(
          LeoLenses.gceZone.getOption(params.runtimeAndRuntimeConfig.runtimeConfig),
          new RuntimeException(
            "GceInterpreter shouldn't get a dataproc runtime creation request. Something is very wrong"
          )
        )
        metadata <- getShutdownScript(params.runtimeAndRuntimeConfig, true)
        googleProject <- F.fromOption(
          LeoLenses.cloudContextToGoogleProject.get(params.runtimeAndRuntimeConfig.runtime.cloudContext),
          new RuntimeException("this should never happen. GCE runtime's cloud context should be a google project")
        )
        opFutureAttempt <- googleComputeService
          .addInstanceMetadata(
            googleProject,
            zoneParam,
            InstanceName(params.runtimeAndRuntimeConfig.runtime.runtimeName.asString),
            metadata
          )
          .attempt
        opt <- opFutureAttempt match {
          case Left(e) if e.getMessage.contains("Instance not found") =>
            logger.info(ctx.loggingCtx)("Instance is already deleted").as(None)
          case Left(e) =>
            F.raiseError(e)
          case Right(opFuture) =>
            opFuture match {
              case None => F.pure(None)
              case Some(v) =>
                for {
                  res <- F.delay(v.get())
                  _ <- F.raiseUnless(isSuccess(res.getHttpErrorStatusCode))(
                    new Exception(s"addInstanceMetadata failed")
                  )
                  opFutureOpt <- googleComputeService
                    .deleteInstance(googleProject,
                                    zoneParam,
                                    InstanceName(params.runtimeAndRuntimeConfig.runtime.runtimeName.asString)
                    )
                } yield opFutureOpt
            }
        }
      } yield opt
    } else F.pure(none[OperationFuture[Operation, Operation]])

  override def finalizeDelete(params: FinalizeDeleteParams)(implicit ev: Ask[F, AppContext]): F[Unit] =
    F.unit

  override def updateDiskSize(params: UpdateDiskSizeParams)(implicit ev: Ask[F, AppContext]): F[Unit] =
    UpdateDiskSizeParams.gcePrism
      .getOption(params)
      .traverse_ { p =>
        for {
          ctx <- ev.ask
          _ <- googleDiskService
            .resizeDisk(p.googleProject, p.zone, p.diskName, p.diskSize.gb)
            .void
            .recoverWith {
              case e: com.google.api.gax.rpc.InvalidArgumentException =>
                if (e.getMessage.contains("must be larger than existing size")) {
                  // Sometimes pubsub messages get resent. So we don't want to fail resize if we get the second resize request.
                  // This assumes we never send reducing size request to back leo
                  logger.warn(ctx.loggingCtx, e)(
                    "Disk resize failed due to invalid request. Ignore this error since target size and existing size are the same"
                  )
                } else F.raiseError[Unit](e)
              case e => F.raiseError[Unit](e)
            }
        } yield ()
      }

  override def resizeCluster(params: ResizeClusterParams)(implicit ev: Ask[F, AppContext]): F[Unit] =
    F.unit

  private[leonardo] def getResourceConstraints(
    googleProject: GoogleProject,
    zoneName: ZoneName,
    machineTypeName: MachineTypeName
  )(implicit ev: Ask[F, TraceId]): F[RuntimeResourceConstraints] =
    for {
      // Resolve the machine type in Google to get the total available memory
      machineType <- googleComputeService.getMachineType(googleProject, zoneName, machineTypeName)
      total <- machineType.fold(
        F.raiseError[MemorySizeBytes](InstanceResourceConstraintsException(googleProject, machineTypeName))
      )(mt => F.pure(MemorySizeBytes.fromMb(mt.getMemoryMb.toDouble)))
      // result = total - os allocated - welder allocated
      gceAllocated = config.gceConfig.gceReservedMemory.map(_.bytes).getOrElse(0L)
      welderAllocated = config.welderConfig.welderReservedMemory.map(_.bytes).getOrElse(0L)
      result = MemorySizeBytes(total.bytes - gceAllocated - welderAllocated)
      // Setting the shared docker memory to 50% of the allocated memory limit, converting from byte to mb
      shmSize = MemorySizeMegaBytes.fromB(0.5 * result.bytes)
    } yield RuntimeResourceConstraints(result, shmSize, total, None)

  private def buildNetworkInterfaces(runtimeProjectAndName: RuntimeProjectAndName,
                                     subnetwork: SubnetworkName,
                                     zone: ZoneName,
                                     googleProject: GoogleProject
  ): NetworkInterface =
    // We get region by removing the last two characters of zone
    NetworkInterface
      .newBuilder()
      .setSubnetwork(
        buildSubnetworkUri(googleProject, RegionName(zone.value.substring(0, zone.value.length - 2)), subnetwork)
      )
      .addAccessConfigs(AccessConfig.newBuilder().setName("Leonardo VM external IP").build)
      .build
}

object GceInterpreter {
  def instanceStatusToRuntimeStatus(instance: Option[Instance]): RuntimeStatus =
    instance.fold[RuntimeStatus](RuntimeStatus.Deleted)(s =>
      GceInstanceStatus
        .withNameInsensitiveOption(s.getStatus)
        .map(RuntimeStatus.fromGceInstanceStatus)
        .getOrElse(RuntimeStatus.Unknown)
    )
}
