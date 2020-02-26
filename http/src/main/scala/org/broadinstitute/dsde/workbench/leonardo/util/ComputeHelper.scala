package org.broadinstitute.dsde.workbench.leonardo.util

import cats.effect.{ContextShift, IO}
import cats.mtl.ApplicativeAsk
import com.google.cloud.compute.v1.Instance
import org.broadinstitute.dsde.workbench.google.GoogleProjectDAO
import org.broadinstitute.dsde.workbench.google2.{GoogleComputeService, MachineTypeName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.config._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.{
  generateUniqueBucketName,
  GcsBucketName,
  GoogleProject,
  ServiceAccountKey
}

final case class InstanceResourceConstaintsException(project: GoogleProject, machineType: MachineTypeName)
    extends LeoException(
      s"Unable to calculate memory constraints for instance in project ${project.value} with machine type ${machineType.value}"
    )

class ComputeHelper(config: ComputeHelperConfig,
                    googleProjectDAO: GoogleProjectDAO,
                    googleComputeService: GoogleComputeService[IO],
                    bucketHelper: BucketHelper,
                    vpcHelper: VPCHelper)(
  implicit contextShift: ContextShift[IO]
  // log: Logger[IO],
  // metrics: NewRelicMetrics[IO]
) {

  def createInstance(params: CreateInstanceParams)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    for {
      // Set up VPC network and firewall
      vpcSettings <- vpcHelper.getOrCreateVPCSettings(params.runtimeProjectAndName.googleProject)
      firewallRule <- vpcHelper.getOrCreateFirewallRule(params.runtimeProjectAndName.googleProject, vpcSettings)

      // Get resource (e.g. memory) constraints for the instance
      resourceConstraints <- getResourceConstraints(params.runtimeProjectAndName.googleProject,
                                                    params.zoneName,
                                                    params.machineConfig.machineType)

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
        params.stagingBucketName,
        params.runtimeImages,
        params.initBucketName,
        params.jupyterUserScriptUri,
        params.jupyterStartUserScriptUri,
        params.serviceAccountKey,
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

      // TODO
      instance = Instance
        .newBuilder()
        .setName(params.runtimeProjectAndName.runtimeName.asString)
        .build

      _ <- googleComputeService.createInstance(params.runtimeProjectAndName.googleProject, params.zoneName, instance)

    } yield ()

  private[leonardo] def getResourceConstraints(
    googleProject: GoogleProject,
    zoneName: ZoneName,
    machineTypeName: MachineTypeName
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[RuntimeResourceConstraints] =
    for {
      // Resolve the machine type in Google to get the total available memory
      machineType <- googleComputeService.getMachineType(googleProject, zoneName, machineTypeName)
      total <- machineType.fold(
        IO.raiseError[MemorySize](InstanceResourceConstaintsException(googleProject, machineTypeName))
      )(mt => IO.pure(MemorySize.fromMb(mt.getMemoryMb.toDouble)))
      // result = total - os allocated - welder allocated
      gceAllocated = config.gceConfig.gceReservedMemory.map(_.bytes).getOrElse(0L)
      welderAllocated = config.welderConfig.welderReservedMemory.map(_.bytes).getOrElse(0L)
      result = MemorySize(total.bytes - gceAllocated - welderAllocated)
    } yield RuntimeResourceConstraints(result)

}

final case class CreateInstanceParams(runtimeProjectAndName: RuntimeProjectAndName,
                                      zoneName: ZoneName,
                                      serviceAccountInfo: ServiceAccountInfo,
                                      machineConfig: RuntimeConfig.GceConfig,
                                      stagingBucketName: Option[GcsBucketName],
                                      runtimeImages: Set[RuntimeImage],
                                      initBucketName: Option[GcsBucketName],
                                      jupyterUserScriptUri: Option[UserScriptPath],
                                      jupyterStartUserScriptUri: Option[UserScriptPath],
                                      serviceAccountKey: Option[ServiceAccountKey],
                                      userJupyterExtensionConfig: Option[UserJupyterExtensionConfig],
                                      defaultClientId: Option[String],
                                      welderEnabled: Boolean,
                                      auditInfo: AuditInfo,
                                      customEnvVars: Map[String, String])

final case class ComputeHelperConfig(projectVPCNetworkLabelName: String,
                                     projectVPCSubnetLabelName: String,
                                     gceConfig: GceConfig,
                                     imageConfig: ImageConfig,
                                     welderConfig: WelderConfig,
                                     proxyConfig: ProxyConfig,
                                     clusterFilesConfig: ClusterFilesConfig,
                                     clusterResourcesConfig: ClusterResourcesConfig)
