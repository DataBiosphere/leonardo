package org.broadinstitute.dsde.workbench.leonardo.util

import java.util.UUID

import akka.actor.ActorSystem
import cats.Parallel
import cats.effect.{Async, Blocker, ContextShift}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.google.cloud.compute.v1.{
  AttachedDisk,
  AttachedDiskInitializeParams,
  Instance,
  Items,
  Metadata,
  NetworkInterface,
  ServiceAccount,
  Tags
}
import io.chrisdavenport.log4cats.Logger
import org.broadinstitute.dsde.workbench.google2.{GoogleComputeService, MachineTypeName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.VPCConfig.{VPCNetwork, VPCSubnet}
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.leonardo.config.Config.proxyConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.WelderDAO
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.util.RuntimeInterpreterConfig.GceInterpreterConfig
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.{generateUniqueBucketName, GcsObjectName, GcsPath, GoogleProject}
import org.broadinstitute.dsde.workbench.newrelic.NewRelicMetrics

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

final case class InstanceResourceConstaintsException(project: GoogleProject, machineType: MachineTypeName)
    extends LeoException(
      s"Unable to calculate memory constraints for instance in project ${project.value} with machine type ${machineType.value}"
    )

final case class MissingServiceAccountException(projectAndName: RuntimeProjectAndName)
    extends LeoException(s"Cannot create instance ${projectAndName}: service account required")

class GceInterpreter[F[_]: Async: Parallel: ContextShift: Logger](
  config: GceInterpreterConfig,
  bucketHelper: BucketHelper[F],
  vpcHelper: VPCHelper[F],
  googleComputeService: GoogleComputeService[F],
  welderDao: WelderDAO[F],
  blocker: Blocker
)(implicit val executionContext: ExecutionContext,
  val system: ActorSystem,
  metrics: NewRelicMetrics[F],
  dbRef: DbReference[F])
    extends BaseRuntimeInterpreter[F](config, welderDao)
    with RuntimeAlgebra[F] {

  override def createRuntime(
    params: CreateRuntimeParams
  )(implicit ev: ApplicativeAsk[F, TraceId]): F[CreateRuntimeResponse] =
    // TODO clean up on error
    for {
      // Set up VPC network and firewall
      vpcSettings <- vpcHelper.getOrCreateVPCSettings(params.runtimeProjectAndName.googleProject)
      _ <- vpcHelper.getOrCreateFirewallRule(params.runtimeProjectAndName.googleProject)

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

      templateParams = RuntimeTemplateValuesConfig(
        params.runtimeProjectAndName,
        Some(stagingBucketName),
        params.runtimeImages,
        Some(initBucketName),
        params.jupyterUserScriptUri,
        params.jupyterStartUserScriptUri,
        None,
        params.userJupyterExtensionConfig,
        params.defaultClientId,
        params.welderEnabled,
        params.auditInfo,
        config.imageConfig,
        config.welderConfig,
        config.proxyConfig,
        config.clusterFilesConfig,
        config.clusterResourcesConfig,
        Some(resourceConstraints)
      )

      _ <- bucketHelper.initializeBucketObjects(initBucketName, templateParams, Map.empty).compile.drain

      serviceAccount <- params.serviceAccountInfo.clusterServiceAccount.fold(
        Async[F].raiseError[WorkbenchEmail](MissingServiceAccountException(params.runtimeProjectAndName))
      )(Async[F].pure)
      initScript = GcsPath(initBucketName, GcsObjectName(config.clusterResourcesConfig.gceInitScript.asString))

      instance = Instance
        .newBuilder()
        .setName(params.runtimeProjectAndName.runtimeName.asString)
        .setDescription("Leonardo VM")
        .setTags(Tags.newBuilder().addItems(proxyConfig.networkTag).build())
        .setMachineType(params.runtimeConfig.machineType.value)
        .addNetworkInterfaces(vpcSettings match {
          case VPCNetwork(value) => NetworkInterface.newBuilder().setNetwork(value).build()
          case VPCSubnet(value)  => NetworkInterface.newBuilder().setSubnetwork(value).build()
        })
        .addDisks(
          AttachedDisk.newBuilder
          //.setDeviceName()   // this may become important when we support attaching PDs
            .setBoot(true)
            .setInitializeParams(
              AttachedDiskInitializeParams
                .newBuilder()
                .setDescription("Leonardo Persistent Disk")
                .setSourceImage(config.gceConfig.customGceImage.asString)
                .setDiskSizeGb(params.runtimeConfig.diskSize.toString)
                .putAllLabels(Map("leonardo" -> "true").asJava)
                .build()
            )
            .setAutoDelete(true) // change to false when we support attaching PDs
            .build()
        )
        .addServiceAccounts(
          ServiceAccount.newBuilder
            .setEmail(serviceAccount.value)
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
            .build()
        )
        .putAllLabels(Map("leonardo" -> "true").asJava)
        //.addGuestAccelerators(???)  // add when we support GPUs
        //.setShieldedInstanceConfig(???)  // investigate shielded VM on Albano's recommendation
        .build()

      operation <- googleComputeService.createInstance(params.runtimeProjectAndName.googleProject,
                                                       config.gceConfig.zoneName,
                                                       instance)

      // TODO not sure if this id is a UUID
      asyncRuntimeFields = AsyncRuntimeFields(GoogleId(UUID.fromString(operation.getId)),
                                              OperationName(operation.getName),
                                              stagingBucketName,
                                              None)
    } yield CreateRuntimeResponse(asyncRuntimeFields, initBucketName, None, CustomDataprocImage("foo"))

  override protected def stopGoogleRuntime(runtime: Cluster, runtimeConfig: RuntimeConfig)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[Unit] = ???

  override protected def startGoogleRuntime(runtime: Cluster, welderAction: WelderAction, runtimeConfig: RuntimeConfig)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[Unit] = ???

  override protected def setMachineTypeInGoogle(runtime: Cluster, machineType: MachineTypeName)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[Unit] = ???

  override def deleteRuntime(params: DeleteRuntimeParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] = ???

  override def finalizeDelete(params: FinalizeDeleteParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] = ???

  override def updateDiskSize(params: UpdateDiskSizeParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] = ???

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
}
