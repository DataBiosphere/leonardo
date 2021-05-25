package org.broadinstitute.dsde.workbench.leonardo
package util

import cats.Parallel
import cats.effect.{Async, Blocker, ContextShift}
import cats.mtl.Ask
import cats.syntax.all._
import com.google.cloud.compute.v1.{Operation, _}
import org.typelevel.log4cats.StructuredLogger
import org.broadinstitute.dsde.workbench.google2.{
  GoogleComputeService,
  GoogleDiskService,
  InstanceName,
  MachineTypeName,
  OperationName,
  RegionName,
  SubnetworkName,
  ZoneName
}
import org.broadinstitute.dsde.workbench.leonardo.dao.WelderDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.google._
import org.broadinstitute.dsde.workbench.leonardo.db.{persistentDiskQuery, DbReference}
import org.broadinstitute.dsde.workbench.leonardo.http.{dbioToIO, userScriptStartupOutputUriMetadataKey}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.RuntimeConfigInCreateRuntimeMessage
import org.broadinstitute.dsde.workbench.leonardo.util.RuntimeInterpreterConfig.GceInterpreterConfig
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.{generateUniqueBucketName, GcsObjectName, GcsPath, GoogleProject}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics

import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

final case class InstanceResourceConstraintsException(project: GoogleProject, machineType: MachineTypeName)
    extends LeoException(
      s"Unable to calculate memory constraints for instance in project ${project.value} with machine type ${machineType.value}",
      traceId = None
    )

final case class MissingServiceAccountException(projectAndName: RuntimeProjectAndName)
    extends LeoException(s"Cannot create instance ${projectAndName}: service account required", traceId = None)

class GceInterpreter[F[_]: Parallel: ContextShift](
  config: GceInterpreterConfig,
  bucketHelper: BucketHelper[F],
  vpcAlg: VPCAlgebra[F],
  googleComputeService: GoogleComputeService[F],
  googleDiskService: GoogleDiskService[F],
  welderDao: WelderDAO[F],
  blocker: Blocker
)(implicit val executionContext: ExecutionContext,
  metrics: OpenTelemetryMetrics[F],
  dbRef: DbReference[F],
  F: Async[F],
  logger: StructuredLogger[F])
    extends BaseRuntimeInterpreter[F](config, welderDao)
    with RuntimeAlgebra[F] {
  override def createRuntime(
    params: CreateRuntimeParams
  )(implicit ev: Ask[F, AppContext]): F[Option[CreateGoogleRuntimeResponse]] =
    // TODO clean up on error
    for {
      ctx <- ev.ask

      zoneParam <- params.runtimeConfig match {
        case x: RuntimeConfigInCreateRuntimeMessage.GceWithPdConfig => F.pure(x.zone)
        case x: RuntimeConfigInCreateRuntimeMessage.GceConfig       => F.pure(x.zone)
        case _ =>
          F.raiseError[ZoneName](
            new RuntimeException(
              "GceInterpreter shouldn't get a dataproc runtime creation request. Something is very wrong"
            )
          )
      }

      // We get region by removing the last two characters of zone
      regionParam = RegionName(zoneParam.value.substring(0, zoneParam.value.length - 2))

      // Set up VPC and firewall
      (network, subnetwork) <- vpcAlg.setUpProjectNetwork(
        SetUpProjectNetworkParams(params.runtimeProjectAndName.googleProject, regionParam)
      )
      _ <- vpcAlg.setUpProjectFirewalls(
        SetUpProjectFirewallsParams(params.runtimeProjectAndName.googleProject, network, regionParam)
      )

      // Get resource (e.g. memory) constraints for the instance
      resourceConstraints <- getResourceConstraints(params.runtimeProjectAndName.googleProject,
                                                    zoneParam,
                                                    params.runtimeConfig.machineType)

      // Create the bucket in the cluster's google project and populate with initialization files.
      // ACLs are granted so the cluster service account can access the files at initialization time.
      initBucketName = generateUniqueBucketName("leoinit-" + params.runtimeProjectAndName.runtimeName.asString)
      stagingBucketName = generateUniqueBucketName("leostaging-" + params.runtimeProjectAndName.runtimeName.asString)

      _ <- bucketHelper
        .createInitBucket(params.runtimeProjectAndName.googleProject, initBucketName, params.serviceAccountInfo)
        .compile
        .drain

      _ <- bucketHelper
        .createStagingBucket(params.auditInfo.creator,
                             params.runtimeProjectAndName.googleProject,
                             stagingBucketName,
                             params.serviceAccountInfo)
        .compile
        .drain

      initScript = GcsPath(initBucketName, GcsObjectName(config.clusterResourcesConfig.gceInitScript.asString))

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
            .setSourceImage(config.gceConfig.customGceImage.asString)
            .setDiskSizeGb(
              bootDiskSize.gb.toString
            )
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
              case Some(FormattedBy.Galaxy) | Some(FormattedBy.Custom) =>
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
                  .setDiskSizeGb(persistentDisk.size.gb.toString)
                  .putAllLabels(Map("leonardo" -> "true").asJava)
                  .setDiskType(
                    persistentDisk.diskType.googleString(params.runtimeProjectAndName.googleProject,
                                                         persistentDisk.zone)
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
                .setDiskSizeGb(x.diskSize.gb.toString)
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

      templateValues = RuntimeTemplateValues(templateParams, Some(ctx.now))

      _ <- bucketHelper
        .initializeBucketObjects(initBucketName,
                                 templateParams.serviceAccountKey,
                                 templateValues,
                                 params.customEnvironmentVariables)
        .compile
        .drain

      instance = Instance
        .newBuilder()
        .setName(params.runtimeProjectAndName.runtimeName.asString)
        .setDescription("Leonardo Managed VM")
        .setTags(Tags.newBuilder().addItems(config.vpcConfig.networkTag.value).build())
        .setMachineType(buildMachineTypeUri(zoneParam, params.runtimeConfig.machineType))
        .addNetworkInterfaces(buildNetworkInterfaces(params.runtimeProjectAndName, subnetwork, zoneParam))
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
                .setKey("startup-script-url")
                .setValue(initScript.toUri)
                .build()
            )
            .addItems(
              Items.newBuilder
                .setKey(userScriptStartupOutputUriMetadataKey)
                .setValue(templateValues.startUserScriptOutputUri)
                .build()
            )
            .build()
        )
        .putAllLabels(Map("leonardo" -> "true").asJava)
        //.addGuestAccelerators(???)  // add when we support GPUs
        //.setShieldedInstanceConfig(???)  // investigate shielded VM on Albano's recommendation
        .build()

      operation <- googleComputeService.createInstance(params.runtimeProjectAndName.googleProject, zoneParam, instance)

      res = operation.map(o =>
        CreateGoogleRuntimeResponse(
          AsyncRuntimeFields(GoogleId(o.getTargetId), OperationName(o.getName), stagingBucketName, None),
          initBucketName,
          config.gceConfig.customGceImage
        )
      )
    } yield res

  override protected def stopGoogleRuntime(params: StopGoogleRuntime)(
    implicit ev: Ask[F, AppContext]
  ): F[Option[com.google.cloud.compute.v1.Operation]] =
    for {
      zoneParam <- F.fromOption(
        LeoLenses.gceZone.getOption(params.runtimeAndRuntimeConfig.runtimeConfig),
        new RuntimeException(
          "GceInterpreter shouldn't get a dataproc runtime creation request. Something is very wrong"
        )
      )
      metadata <- getShutdownScript(params.runtimeAndRuntimeConfig.runtime, blocker)
      _ <- googleComputeService.addInstanceMetadata(
        params.runtimeAndRuntimeConfig.runtime.googleProject,
        zoneParam,
        InstanceName(params.runtimeAndRuntimeConfig.runtime.runtimeName.asString),
        metadata
      )
      r <- googleComputeService.stopInstance(params.runtimeAndRuntimeConfig.runtime.googleProject,
                                             zoneParam,
                                             InstanceName(params.runtimeAndRuntimeConfig.runtime.runtimeName.asString))
    } yield Some(r)

  override protected def startGoogleRuntime(params: StartGoogleRuntime)(
    implicit ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      zoneParam <- F.fromOption(
        LeoLenses.gceZone.getOption(params.runtimeAndRuntimeConfig.runtimeConfig),
        new RuntimeException(
          "GceInterpreter shouldn't get a dataproc runtime creation request. Something is very wrong"
        )
      )
      resourceConstraints <- getResourceConstraints(params.runtimeAndRuntimeConfig.runtime.googleProject,
                                                    zoneParam,
                                                    params.runtimeAndRuntimeConfig.runtimeConfig.machineType)
      metadata <- getStartupScript(params.runtimeAndRuntimeConfig.runtime,
                                   params.welderAction,
                                   params.initBucket,
                                   blocker,
                                   resourceConstraints,
                                   true)
      // remove the startup-script-url metadata entry if present which is only used at creation time
      _ <- googleComputeService.modifyInstanceMetadata(
        params.runtimeAndRuntimeConfig.runtime.googleProject,
        zoneParam,
        InstanceName(params.runtimeAndRuntimeConfig.runtime.runtimeName.asString),
        metadataToAdd = metadata,
        metadataToRemove = Set("startup-script-url")
      )
      _ <- googleComputeService.startInstance(params.runtimeAndRuntimeConfig.runtime.googleProject,
                                              zoneParam,
                                              InstanceName(params.runtimeAndRuntimeConfig.runtime.runtimeName.asString))
    } yield ()

  override protected def setMachineTypeInGoogle(params: SetGoogleMachineType)(
    implicit ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      zoneParam <- F.fromOption(
        LeoLenses.gceZone.getOption(params.runtimeAndRuntimeConfig.runtimeConfig),
        new RuntimeException(
          "GceInterpreter shouldn't get a dataproc runtime creation request. Something is very wrong"
        )
      )
      _ <- googleComputeService.setMachineType(
        params.runtimeAndRuntimeConfig.runtime.googleProject,
        zoneParam,
        InstanceName(params.runtimeAndRuntimeConfig.runtime.runtimeName.asString),
        params.machineType
      )
    } yield ()

  override def deleteRuntime(
    params: DeleteRuntimeParams
  )(implicit ev: Ask[F, AppContext]): F[Option[Operation]] =
    if (params.runtimeAndRuntimeConfig.runtime.asyncRuntimeFields.isDefined) {
      for {
        zoneParam <- F.fromOption(
          LeoLenses.gceZone.getOption(params.runtimeAndRuntimeConfig.runtimeConfig),
          new RuntimeException(
            "GceInterpreter shouldn't get a dataproc runtime creation request. Something is very wrong"
          )
        )
        metadata <- getShutdownScript(params.runtimeAndRuntimeConfig.runtime, blocker)
        _ <- googleComputeService
          .addInstanceMetadata(
            params.runtimeAndRuntimeConfig.runtime.googleProject,
            zoneParam,
            InstanceName(params.runtimeAndRuntimeConfig.runtime.runtimeName.asString),
            metadata
          )
          .handleErrorWith {
            case e: org.broadinstitute.dsde.workbench.model.WorkbenchException
                if e.getMessage.contains("Instance not found") =>
              F.unit
            case e => F.raiseError[Unit](e)
          }
        op <- googleComputeService
          .deleteInstance(params.runtimeAndRuntimeConfig.runtime.googleProject,
                          zoneParam,
                          InstanceName(params.runtimeAndRuntimeConfig.runtime.runtimeName.asString))
      } yield op
    } else F.pure(None)

  override def finalizeDelete(params: FinalizeDeleteParams)(implicit ev: Ask[F, AppContext]): F[Unit] =
    F.unit

  override def updateDiskSize(params: UpdateDiskSizeParams)(implicit ev: Ask[F, AppContext]): F[Unit] =
    UpdateDiskSizeParams.gcePrism
      .getOption(params)
      .traverse_(p =>
        googleDiskService
          .resizeDisk(p.googleProject, p.zone, p.diskName, p.diskSize.gb)
      )

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
        F.raiseError[MemorySize](InstanceResourceConstraintsException(googleProject, machineTypeName))
      )(mt => F.pure(MemorySize.fromMb(mt.getMemoryMb.toDouble)))
      // result = total - os allocated - welder allocated
      gceAllocated = config.gceConfig.gceReservedMemory.map(_.bytes).getOrElse(0L)
      welderAllocated = config.welderConfig.welderReservedMemory.map(_.bytes).getOrElse(0L)
      result = MemorySize(total.bytes - gceAllocated - welderAllocated)
    } yield RuntimeResourceConstraints(result)

  private def buildNetworkInterfaces(runtimeProjectAndName: RuntimeProjectAndName,
                                     subnetwork: SubnetworkName,
                                     zone: ZoneName): NetworkInterface =
    // We get region by removing the last two characters of zone
    NetworkInterface
      .newBuilder()
      .setSubnetwork(
        buildSubnetworkUri(runtimeProjectAndName.googleProject,
                           RegionName(zone.value.substring(0, zone.value.length - 2)),
                           subnetwork)
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
