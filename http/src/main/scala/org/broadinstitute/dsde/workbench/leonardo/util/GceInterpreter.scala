package org.broadinstitute.dsde.workbench.leonardo
package util

import cats.Parallel
import cats.effect.{Async, Blocker, ContextShift}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.google.cloud.compute.v1.{Operation, _}
import io.chrisdavenport.log4cats.Logger
import org.broadinstitute.dsde.workbench.google2.{
  DiskName,
  GoogleComputeService,
  GoogleDiskService,
  InstanceName,
  MachineTypeName,
  SubnetworkName,
  ZoneName
}
import org.broadinstitute.dsde.workbench.leonardo.dao.WelderDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.google._
import org.broadinstitute.dsde.workbench.leonardo.db.{persistentDiskQuery, DbReference}
import org.broadinstitute.dsde.workbench.leonardo.http.userScriptStartupOutputUriMetadataKey
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.util.GceInterpreter._
import org.broadinstitute.dsde.workbench.leonardo.util.RuntimeInterpreterConfig.GceInterpreterConfig
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.{generateUniqueBucketName, GcsObjectName, GcsPath, GoogleProject}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

final case class InstanceResourceConstaintsException(project: GoogleProject, machineType: MachineTypeName)
    extends LeoException(
      s"Unable to calculate memory constraints for instance in project ${project.value} with machine type ${machineType.value}"
    )

final case class MissingServiceAccountException(projectAndName: RuntimeProjectAndName)
    extends LeoException(s"Cannot create instance ${projectAndName}: service account required")

class GceInterpreter[F[_]: Parallel: ContextShift: Logger](
  config: GceInterpreterConfig,
  bucketHelper: BucketHelper[F],
  vpcAlg: VPCAlgebra[F],
  googleComputeService: GoogleComputeService[F],
  googleDiskService: GoogleDiskService[F],
  welderDao: WelderDAO[F],
  blocker: Blocker
)(implicit val executionContext: ExecutionContext, metrics: OpenTelemetryMetrics[F], dbRef: DbReference[F], F: Async[F])
    extends BaseRuntimeInterpreter[F](config, welderDao)
    with RuntimeAlgebra[F] {

  override def createRuntime(
    params: CreateRuntimeParams
  )(implicit ev: ApplicativeAsk[F, AppContext]): F[CreateRuntimeResponse] =
    // TODO clean up on error
    for {
      ctx <- ev.ask
      // Set up VPC and firewall
      (network, subnetwork) <- vpcAlg.setUpProjectNetwork(
        SetUpProjectNetworkParams(params.runtimeProjectAndName.googleProject)
      )
      _ <- vpcAlg.setUpProjectFirewalls(
        SetUpProjectFirewallsParams(params.runtimeProjectAndName.googleProject, network)
      )

      // Get resource (e.g. memory) constraints for the instance
      resourceConstraints <- getResourceConstraints(params.runtimeProjectAndName.googleProject,
                                                    config.gceConfig.zoneName,
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

      bootDisk = AttachedDisk.newBuilder
        .setBoot(true)
        .setInitializeParams(
          AttachedDiskInitializeParams
            .newBuilder()
            .setDescription("Leonardo Managed Boot Disk")
            .setSourceImage(config.gceConfig.customGceImage.asString)
            .setDiskSizeGb(
              config.gceConfig.runtimeConfigDefaults.bootDiskSize.get.gb.toString //Using `.get` here is okay since `bootDiskSize` always exists in config
            )
            .putAllLabels(Map("leonardo" -> "true").asJava)
            .build()
        )
        .setAutoDelete(true)
        .build()

      (userDisk, isFormatted) <- params.runtimeConfig match {
        case x: RuntimeConfig.GceWithPdConfig =>
          for {
            diskId <- F.fromEither(
              x.persistentDiskId.toRight(
                new RuntimeException("Missing diskId in the request. We should have rejected this request in front leo")
              )
            )
            persistentDiskOpt <- persistentDiskQuery.getById(diskId).transaction
            persistentDisk <- F.fromEither(
              persistentDiskOpt.toRight(
                new Exception(
                  "Runtime has Persistent Disk enabled, but we can't find it in database. This should never happen."
                )
              )
            )
            isFormatted <- persistentDisk.formattedBy match {
              case Some(FormattedBy.Galaxy) =>
                F.raiseError(
                  new RuntimeException(
                    "Trying to use a Galaxy formatted disk for creating GCE runtime. This should never happen."
                  )
                )
              case Some(FormattedBy.GCE) => F.pure(true)
              case None                  => F.pure(false)
            }
          } yield {
            val disk = AttachedDisk.newBuilder
              .setBoot(false)
              .setDeviceName("user-disk")
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
            (disk, persistentDisk.formattedBy.isDefined)
          }
        case x: RuntimeConfig.GceConfig =>
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
        case _: RuntimeConfig.DataprocConfig =>
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
        .setMachineType(buildMachineTypeUri(config.gceConfig.zoneName, params.runtimeConfig.machineType))
        .addNetworkInterfaces(buildNetworkInterfaces(params.runtimeProjectAndName, subnetwork))
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
                .setValue(templateValues.jupyterStartUserScriptOutputUri)
                .build()
            )
            .build()
        )
        .putAllLabels(Map("leonardo" -> "true").asJava)
        //.addGuestAccelerators(???)  // add when we support GPUs
        //.setShieldedInstanceConfig(???)  // investigate shielded VM on Albano's recommendation
        .build()

      operation <- googleComputeService.createInstance(params.runtimeProjectAndName.googleProject,
                                                       config.gceConfig.zoneName,
                                                       instance)

      asyncRuntimeFields = AsyncRuntimeFields(GoogleId(operation.getTargetId),
                                              OperationName(operation.getName),
                                              stagingBucketName,
                                              None)
    } yield CreateRuntimeResponse(asyncRuntimeFields, initBucketName, None, config.gceConfig.customGceImage)

  override def getRuntimeStatus(
    params: GetRuntimeStatusParams
  )(implicit ev: ApplicativeAsk[F, TraceId]): F[RuntimeStatus] =
    for {
      zoneName <- Async[F].fromEither(
        params.zoneName.toRight(new Exception("Missing zone name for getting GCE runtime status"))
      )
      status <- googleComputeService
        .getInstance(params.googleProject, zoneName, InstanceName(params.runtimeName.asString))
        .map(instanceStatusToRuntimeStatus)
    } yield status

  override protected def stopGoogleRuntime(runtime: Runtime, runtimeConfig: RuntimeConfig)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[Option[com.google.cloud.compute.v1.Operation]] =
    for {
      metadata <- getShutdownScript(runtime, blocker)
      _ <- googleComputeService.addInstanceMetadata(runtime.googleProject,
                                                    config.gceConfig.zoneName,
                                                    InstanceName(runtime.runtimeName.asString),
                                                    metadata)
      r <- googleComputeService.stopInstance(runtime.googleProject,
                                             config.gceConfig.zoneName,
                                             InstanceName(runtime.runtimeName.asString))
    } yield Some(r)

  override protected def startGoogleRuntime(runtime: Runtime,
                                            welderAction: Option[WelderAction],
                                            runtimeConfig: RuntimeConfig)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      resourceConstraints <- getResourceConstraints(runtime.googleProject,
                                                    config.gceConfig.zoneName,
                                                    runtimeConfig.machineType)
      metadata <- getStartupScript(runtime, welderAction, ctx.now, blocker, resourceConstraints)
      // remove the startup-script-url metadata entry if present which is only used at creation time
      _ <- googleComputeService.modifyInstanceMetadata(
        runtime.googleProject,
        config.gceConfig.zoneName,
        InstanceName(runtime.runtimeName.asString),
        metadataToAdd = metadata,
        metadataToRemove = Set("startup-script-url")
      )
      _ <- googleComputeService.startInstance(runtime.googleProject,
                                              config.gceConfig.zoneName,
                                              InstanceName(runtime.runtimeName.asString))
    } yield ()

  override protected def setMachineTypeInGoogle(runtime: Runtime, machineType: MachineTypeName)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[Unit] =
    googleComputeService.setMachineType(runtime.googleProject,
                                        config.gceConfig.zoneName,
                                        InstanceName(runtime.runtimeName.asString),
                                        machineType)

  override def deleteRuntime(
    params: DeleteRuntimeParams
  )(implicit ev: ApplicativeAsk[F, TraceId]): F[Option[Operation]] =
    if (params.isAsyncRuntimeFields)
      googleComputeService
        .deleteInstance(params.googleProject,
                        config.gceConfig.zoneName,
                        InstanceName(params.runtimeName.asString),
                        params.autoDeletePersistentDisk.to[Set])
        .map(x => Some(x))
    else Async[F].pure(None)

  override def finalizeDelete(params: FinalizeDeleteParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    Async[F].unit

  override def updateDiskSize(params: UpdateDiskSizeParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    googleDiskService
      .resizeDisk(params.runtime.googleProject,
                  config.gceConfig.zoneName,
                  DiskName(params.runtime.runtimeName.asString),
                  params.diskSize.gb)
      .void

  override def resizeCluster(params: ResizeClusterParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    Async[F].unit

  private[leonardo] def getResourceConstraints(
    googleProject: GoogleProject,
    zoneName: ZoneName,
    machineTypeName: MachineTypeName
  )(implicit ev: ApplicativeAsk[F, TraceId]): F[RuntimeResourceConstraints] =
    for {
      // Resolve the machine type in Google to get the total available memory
      machineType <- googleComputeService.getMachineType(googleProject, zoneName, machineTypeName)
      total <- machineType.fold(
        Async[F].raiseError[MemorySize](InstanceResourceConstaintsException(googleProject, machineTypeName))
      )(mt => Async[F].pure(MemorySize.fromMb(mt.getMemoryMb.toDouble)))
      // result = total - os allocated - welder allocated
      gceAllocated = config.gceConfig.gceReservedMemory.map(_.bytes).getOrElse(0L)
      welderAllocated = config.welderConfig.welderReservedMemory.map(_.bytes).getOrElse(0L)
      result = MemorySize(total.bytes - gceAllocated - welderAllocated)
    } yield RuntimeResourceConstraints(result)

  private def buildNetworkInterfaces(runtimeProjectAndName: RuntimeProjectAndName,
                                     subnetwork: SubnetworkName): NetworkInterface =
    NetworkInterface
      .newBuilder()
      .setSubnetwork(buildSubnetworkUri(runtimeProjectAndName.googleProject, config.gceConfig.regionName, subnetwork))
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
