package org.broadinstitute.dsde.workbench.leonardo
package util

import com.azure.resourcemanager.msi.models.Identity
import org.broadinstitute.dsde.workbench.azure.{
  AzureCloudContext,
  PrimaryKey,
  RelayHybridConnectionName,
  RelayNamespace
}
import org.broadinstitute.dsde.workbench.google2.DiskName
import org.broadinstitute.dsde.workbench.google2.GKEModels.NodepoolName
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{NamespaceName, ServiceAccountName}
import org.broadinstitute.dsde.workbench.leonardo.AppRestore.GalaxyRestore
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.AppSamResourceId
import org.broadinstitute.dsde.workbench.leonardo.config.SamConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.{CustomAppService, StorageContainerResponse}
import org.broadinstitute.dsde.workbench.leonardo.http.kubernetesProxyHost
import org.broadinstitute.dsde.workbench.leonardo.util.IdentityType.{PodIdentity, WorkloadIdentity}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.{Release, Values}
import org.http4s.Uri

import java.net.URL
private[leonardo] object BuildHelmChartValues {
  def buildGalaxyChartOverrideValuesString(config: GKEInterpreterConfig,
                                           appName: AppName,
                                           release: Release,
                                           cluster: KubernetesCluster,
                                           nodepoolName: NodepoolName,
                                           userEmail: WorkbenchEmail,
                                           customEnvironmentVariables: Map[String, String],
                                           ksa: ServiceAccountName,
                                           namespaceName: NamespaceName,
                                           nfsDisk: PersistentDisk,
                                           postgresDiskName: DiskName,
                                           machineType: AppMachineType,
                                           galaxyRestore: Option[GalaxyRestore]
  ): List[String] = {
    val k8sProxyHost = kubernetesProxyHost(cluster, config.proxyConfig.proxyDomain).address
    val leoProxyhost = config.proxyConfig.getProxyServerHostName
    val ingressPath = s"/proxy/google/v1/apps/${cluster.cloudContext.asString}/${appName.value}/galaxy"
    val workspaceName = customEnvironmentVariables.getOrElse("WORKSPACE_NAME", "")
    val workspaceNamespace = customEnvironmentVariables.getOrElse("WORKSPACE_NAMESPACE", "")
    // Machine type info
    val maxLimitMemory = machineType.memorySizeInGb
    val maxLimitCpu = machineType.numOfCpus
    val maxRequestMemory = maxLimitMemory - 22
    val maxRequestCpu = maxLimitCpu - 6

    // Custom EV configs
    val configs = customEnvironmentVariables.toList.zipWithIndex.flatMap { case ((k, v), i) =>
      List(
        raw"""configs.$k=$v""",
        raw"""extraEnv[$i].name=$k""",
        raw"""extraEnv[$i].valueFrom.configMapKeyRef.name=${release.asString}-galaxykubeman-configs""",
        raw"""extraEnv[$i].valueFrom.configMapKeyRef.key=$k"""
      )
    }

    val galaxyRestoreSettings = galaxyRestore.fold(List.empty[String])(g =>
      List(
        raw"""restore.persistence.nfs.galaxy.pvcID=${g.galaxyPvcId.asString}""",
        raw"""galaxy.persistence.existingClaim=${release.asString}-galaxy-galaxy-pvc"""
      )
    )
    // Using the string interpolator raw""" since the chart keys include quotes to escape Helm
    // value override special characters such as '.'
    // https://helm.sh/docs/intro/using_helm/#the-format-and-limitations-of---set
    List(
      // Storage class configs
      raw"""nfs.storageClass.name=nfs-${release.asString}""",
      raw"""galaxy.persistence.storageClass=nfs-${release.asString}""",
      // Node selector config: this ensures the app is run on the user's nodepool
      raw"""galaxy.nodeSelector.cloud\.google\.com/gke-nodepool=${nodepoolName.value}""",
      raw"""nfs.nodeSelector.cloud\.google\.com/gke-nodepool=${nodepoolName.value}""",
      raw"""galaxy.configs.job_conf\.yml.runners.k8s.k8s_node_selector=cloud.google.com/gke-nodepool: ${nodepoolName.value}""",
      raw"""galaxy.postgresql.master.nodeSelector.cloud\.google\.com/gke-nodepool=${nodepoolName.value}""",
      // Ingress configs
      raw"""galaxy.ingress.path=${ingressPath}""",
      raw"""galaxy.ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-from=https://${k8sProxyHost}""",
      raw"""galaxy.ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-to=${leoProxyhost}""",
      raw"""galaxy.ingress.hosts[0].host=${k8sProxyHost}""",
      raw"""galaxy.ingress.hosts[0].paths[0].path=${ingressPath}""",
      raw"""galaxy.ingress.tls[0].hosts[0]=${k8sProxyHost}""",
      raw"""galaxy.ingress.tls[0].secretName=tls-secret""",
      // CVMFS configs
      raw"""cvmfs.cvmfscsi.cache.alien.pvc.storageClass=nfs-${release.asString}""",
      raw"""cvmfs.cvmfscsi.cache.alien.pvc.name=cvmfs-alien-cache""",
      // Galaxy configs
      raw"""galaxy.configs.galaxy\.yml.galaxy.single_user=${userEmail.value}""",
      raw"""galaxy.configs.galaxy\.yml.galaxy.admin_users=${userEmail.value}""",
      raw"""galaxy.terra.launch.workspace=${workspaceName}""",
      raw"""galaxy.terra.launch.namespace=${workspaceNamespace}""",
      raw"""galaxy.terra.launch.apiURL=${config.galaxyAppConfig.orchUrl.value}""",
      raw"""galaxy.terra.launch.drsURL=${config.galaxyAppConfig.drsUrl.value}""",
      // Tusd ingress configs
      raw"""galaxy.tusd.ingress.hosts[0].host=${k8sProxyHost}""",
      raw"""galaxy.tusd.ingress.hosts[0].paths[0].path=${ingressPath}/api/upload/resumable_upload""",
      raw"""galaxy.tusd.ingress.tls[0].hosts[0]=${k8sProxyHost}""",
      raw"""galaxy.tusd.ingress.tls[0].secretName=tls-secret""",
      // Set RabbitMQ storage class
      raw"""galaxy.rabbitmq.persistence.storageClassName=nfs-${release.asString}""",
      // Set Machine Type specs
      raw"""galaxy.jobs.maxLimits.memory=${maxLimitMemory}""",
      raw"""galaxy.jobs.maxLimits.cpu=${maxLimitCpu}""",
      raw"""galaxy.jobs.maxRequests.memory=${maxRequestMemory}""",
      raw"""galaxy.jobs.maxRequests.cpu=${maxRequestCpu}""",
      raw"""galaxy.jobs.rules.tpv_rules_local\.yml.destinations.k8s.max_mem=${maxRequestMemory}""",
      raw"""galaxy.jobs.rules.tpv_rules_local\.yml.destinations.k8s.max_cores=${maxRequestCpu}""",
      // RBAC configs
      raw"""galaxy.serviceAccount.create=false""",
      raw"""galaxy.serviceAccount.name=${ksa.value}""",
      raw"""rbac.serviceAccount=${ksa.value}""",
      // Persistence configs
      raw"""persistence.nfs.name=${namespaceName.value}-${config.galaxyDiskConfig.nfsPersistenceName}""",
      raw"""persistence.nfs.persistentVolume.extraSpec.gcePersistentDisk.pdName=${nfsDisk.name.value}""",
      raw"""persistence.nfs.size=${nfsDisk.size.gb.toString}Gi""",
      raw"""persistence.postgres.name=${namespaceName.value}-${config.galaxyDiskConfig.postgresPersistenceName}""",
      raw"""galaxy.postgresql.galaxyDatabasePassword=${config.galaxyAppConfig.postgresPassword.value}""",
      raw"""persistence.postgres.persistentVolume.extraSpec.gcePersistentDisk.pdName=${postgresDiskName.value}""",
      raw"""persistence.postgres.size=${config.galaxyDiskConfig.postgresDiskSizeGB.gb.toString}Gi""",
      raw"""nfs.persistence.existingClaim=${namespaceName.value}-${config.galaxyDiskConfig.nfsPersistenceName}-pvc""",
      raw"""nfs.persistence.size=${nfsDisk.size.gb.toString}Gi""",
      raw"""galaxy.postgresql.persistence.existingClaim=${namespaceName.value}-${config.galaxyDiskConfig.postgresPersistenceName}-pvc""",
      // Note Galaxy pvc claim is the nfs disk size minus 50G
      raw"""galaxy.persistence.size=${(nfsDisk.size.gb - 50).toString}Gi"""
    ) ++ configs ++ galaxyRestoreSettings
  }

  def buildCromwellAppChartOverrideValuesString(config: GKEInterpreterConfig,
                                                appName: AppName,
                                                cluster: KubernetesCluster,
                                                nodepoolName: NodepoolName,
                                                namespaceName: NamespaceName,
                                                disk: PersistentDisk,
                                                ksaName: ServiceAccountName,
                                                gsa: WorkbenchEmail,
                                                customEnvironmentVariables: Map[String, String]
  ): List[String] = {
    val proxyPath = s"/proxy/google/v1/apps/${cluster.cloudContext.asString}/${appName.value}/cromwell-service"
    val k8sProxyHost = kubernetesProxyHost(cluster, config.proxyConfig.proxyDomain).address
    val leoProxyhost = config.proxyConfig.getProxyServerHostName
    val gcsBucket = customEnvironmentVariables.getOrElse("WORKSPACE_BUCKET", "<no workspace bucket defined>")

    val rewriteTarget = "$2"
    val ingress = List(
      raw"""ingress.enabled=true""",
      raw"""ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-from=https://${k8sProxyHost}""",
      raw"""ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-to=${leoProxyhost}""",
      raw"""ingress.annotations.nginx\.ingress\.kubernetes\.io/rewrite-target=/${rewriteTarget}""",
      raw"""ingress.annotations.nginx\.ingress\.kubernetes\.io/auth-tls-secret=${namespaceName.value}/ca-secret""",
      raw"""ingress.path=${proxyPath}""",
      raw"""ingress.hosts[0].host=${k8sProxyHost}""",
      raw"""ingress.hosts[0].paths[0]=${proxyPath}${"(/|$)(.*)"}""",
      raw"""ingress.tls[0].secretName=tls-secret""",
      raw"""ingress.tls[0].hosts[0]=${k8sProxyHost}""",
      raw"""db.password=${config.cromwellAppConfig.dbPassword.value}"""
    )

    List(
      // Node selector
      raw"""nodeSelector.cloud\.google\.com/gke-nodepool=${nodepoolName.value}""",
      // Persistence
      raw"""persistence.size=${disk.size.gb.toString}G""",
      raw"""persistence.gcePersistentDisk=${disk.name.value}""",
      raw"""env.swaggerBasePath=$proxyPath/cromwell""",
      // cromwellConfig
      raw"""config.gcsProject=${cluster.cloudContext.asString}""",
      raw"""config.gcsBucket=$gcsBucket/cromwell-execution""",
      // Service Account
      raw"""config.serviceAccount.name=${ksaName.value}""",
      raw"""config.serviceAccount.annotations.gcpServiceAccount=${gsa.value}"""
    ) ++ ingress
  }

  def buildCustomChartOverrideValuesString(config: GKEInterpreterConfig,
                                           appName: AppName,
                                           release: Release,
                                           nodepoolName: NodepoolName,
                                           serviceName: String,
                                           cluster: KubernetesCluster,
                                           namespaceName: NamespaceName,
                                           service: CustomAppService,
                                           extraArgs: List[String],
                                           disk: PersistentDisk,
                                           ksaName: ServiceAccountName,
                                           customEnvironmentVariables: Map[String, String]
  ): String = {
    val k8sProxyHost = kubernetesProxyHost(cluster, config.proxyConfig.proxyDomain).address
    val leoProxyhost = config.proxyConfig.getProxyServerHostName
    val ingressPath = s"/proxy/google/v1/apps/${cluster.cloudContext.asString}/${appName.value}/${serviceName}"

    // Command and args
    val command = service.command.zipWithIndex.map { case (c, i) =>
      raw"""image.command[$i]=$c"""
    }
    val args = service.args.zipWithIndex.map { case (a, i) =>
      raw"""image.args[$i]=$a"""
    } ++ extraArgs.zipWithIndex.map { case (a, i) =>
      raw"""image.args[${i + service.args.length}]=$a"""
    }

    // Custom EVs
    val configs = customEnvironmentVariables.toList.zipWithIndex.flatMap { case ((k, v), i) =>
      List(
        raw"""extraEnv[$i].name=$k""",
        raw"""extraEnv[$i].value=$v"""
      )
    }

    val rewriteTarget = "$2"
    // These nginx an ingress rules are condition.
    // Some apps do not like behind behind a reverse proxy in this way, and require routing specified via this baseUrl
    // The two methods are mutually exclusive
    val ingress = service.baseUrl match {
      case "/" =>
        List(
          raw"""ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-from=https://${k8sProxyHost}""",
          raw"""ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-to=${leoProxyhost}${ingressPath}""",
          raw"""ingress.annotations.nginx\.ingress\.kubernetes\.io/rewrite-target=/${rewriteTarget}""",
          raw"""ingress.hosts[0].paths[0]=${ingressPath}${"(/|$)(.*)"}"""
        )
      case _ => List(raw"""ingress.hosts[0].paths[0]=${service.baseUrl}""")
    }

    (List(
      raw"""nameOverride=${serviceName}""",
      // Image
      raw"""image.image=${service.image.imageUrl}""",
      raw"""image.port=${service.port}""",
      raw"""image.baseUrl=${service.baseUrl}""",
      // Ingress
      raw"""ingress.hosts[0].host=${k8sProxyHost}""",
      raw"""ingress.annotations.nginx\.ingress\.kubernetes\.io/auth-tls-secret=${namespaceName.value}/ca-secret""",
      raw"""ingress.tls[0].secretName=tls-secret""",
      raw"""ingress.tls[0].hosts[0]=${k8sProxyHost}""",
      // Node selector
      raw"""nodeSelector.cloud\.google\.com/gke-nodepool=${nodepoolName.value}""",
      // Persistence
      raw"""persistence.size=${disk.size.gb.toString}G""",
      raw"""persistence.gcePersistentDisk=${disk.name.value}""",
      raw"""persistence.mountPath=${service.pdMountPath}""",
      raw"""persistence.accessMode=${service.pdAccessMode}""",
      raw"""serviceAccount.name=${ksaName.value}"""
    ) ++ command ++ args ++ configs ++ ingress).mkString(",")
  }

  def buildRStudioAppChartOverrideValuesString(config: GKEInterpreterConfig,
                                               appName: AppName,
                                               cluster: KubernetesCluster,
                                               nodepoolName: NodepoolName,
                                               namespaceName: NamespaceName,
                                               disk: PersistentDisk,
                                               ksaName: ServiceAccountName,
                                               userEmail: WorkbenchEmail,
                                               stagingBucket: GcsBucketName,
                                               customEnvironmentVariables: Map[String, String]
  ): List[String] = {
    val rstudioIngressPath = s"/proxy/google/v1/apps/${cluster.cloudContext.asString}/${appName.value}/app"
    val welderIngressPath = s"/proxy/google/v1/apps/${cluster.cloudContext.asString}/${appName.value}/welder-service"
    val k8sProxyHost = kubernetesProxyHost(cluster, config.proxyConfig.proxyDomain).address
    val leoProxyhost = config.proxyConfig.getProxyServerHostName

    // Custom EV configs
    val configs = customEnvironmentVariables.toList.zipWithIndex.flatMap { case ((k, v), i) =>
      List(
        raw"""extraEnv[$i].name=$k""",
        raw"""extraEnv[$i].value=$v"""
      )
    }

    val rewriteTarget = "$2"
    val ingress = List(
      raw"""ingress.enabled=true""",
      raw"""ingress.annotations.nginx\.ingress\.kubernetes\.io/auth-tls-secret=${namespaceName.value}/ca-secret""",
      raw"""ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-from=https://${k8sProxyHost}""",
      raw"""ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-to=${leoProxyhost}${rstudioIngressPath}""",
      raw"""ingress.annotations.nginx\.ingress\.kubernetes\.io/rewrite-target=/${rewriteTarget}""",
      raw"""ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-cookie-path=/ "/; Secure; SameSite=None"""",
      raw"""ingress.host=${k8sProxyHost}""",
      raw"""ingress.rstudio.path=${rstudioIngressPath}${"(/|$)(.*)"}""",
      raw"""ingress.welder.path=${welderIngressPath}${"(/|$)(.*)"}""",
      raw"""ingress.tls[0].secretName=tls-secret""",
      raw"""ingress.tls[0].hosts[0]=${k8sProxyHost}"""
    )

    val welder = List(
      raw"""welder.extraEnv[0].name=GOOGLE_PROJECT""",
      raw"""welder.extraEnv[0].value=${cluster.cloudContext.asString}""",
      raw"""welder.extraEnv[1].name=STAGING_BUCKET""",
      raw"""welder.extraEnv[1].value=${stagingBucket.value}""",
      raw"""welder.extraEnv[2].name=CLUSTER_NAME""",
      raw"""welder.extraEnv[2].value=${appName.value}""",
      raw"""welder.extraEnv[3].name=OWNER_EMAIL""",
      raw"""welder.extraEnv[3].value=${userEmail.value}"""
    )

    List(
      // Node selector
      raw"""nodeSelector.cloud\.google\.com/gke-nodepool=${nodepoolName.value}""",
      // Persistence
      raw"""persistence.size=${disk.size.gb.toString}G""",
      raw"""persistence.gcePersistentDisk=${disk.name.value}""",
      // Service Account
      raw"""serviceAccount.name=${ksaName.value}"""
    ) ++ ingress ++ welder ++ configs
  }

  def buildListenerChartOverrideValuesString(release: Release,
                                             samResourceId: AppSamResourceId,
                                             relayNamespace: RelayNamespace,
                                             relayHcName: RelayHybridConnectionName,
                                             relayPrimaryKey: PrimaryKey,
                                             appType: AppType,
                                             workspaceId: WorkspaceId,
                                             appName: AppName,
                                             validHosts: Set[String],
                                             samConfig: SamConfig,
                                             listenerImage: String,
                                             leoUrlBase: URL
  ): Values = {
    val relayTargetHost = appType match {
      case AppType.Cromwell          => s"http://coa-${release.asString}-reverse-proxy-service:8000/"
      case AppType.CromwellRunnerApp => s"http://cra-${release.asString}-reverse-proxy-service:8000/"
      case AppType.Wds               => s"http://wds-${release.asString}-wds-svc:8080"
      case AppType.HailBatch         => "http://batch:8080"
      case _                         => "uknown"
    }

    // Hail batch serves requests on /{appName}/batch and uses relative redirects,
    // so requires that we don't strip the entity path. For other app types we do
    // strip the entity path.
    val removeEntityPathFromHttpUrl = appType != AppType.HailBatch

    // validHosts can have a different number of hosts, this pre-processes the list as separate chart values
    val validHostValues = validHosts.zipWithIndex.map { case (elem, idx) =>
      raw"connection.validHosts[$idx]=$elem"
    }

    Values(
      List(
        raw"""connection.removeEntityPathFromHttpUrl="${removeEntityPathFromHttpUrl.toString}"""",
        raw"connection.connectionString=Endpoint=sb://${relayNamespace.value}.servicebus.windows.net/;SharedAccessKeyName=listener;SharedAccessKey=${relayPrimaryKey.value};EntityPath=${relayHcName.value}",
        raw"connection.connectionName=${relayHcName.value}",
        raw"connection.endpoint=https://${relayNamespace.value}.servicebus.windows.net",
        raw"connection.targetHost=$relayTargetHost",
        raw"sam.url=${samConfig.server}",
        raw"sam.resourceId=${samResourceId.resourceId}",
        raw"sam.resourceType=${samResourceId.resourceType.asString}",
        raw"sam.action=connect",
        raw"leonardo.url=${leoUrlBase}",
        raw"general.workspaceId=${workspaceId.value.toString}",
        raw"general.appName=${appName.value}",
        raw"listener.image=${listenerImage}"
      ).concat(validHostValues).mkString(",")
    )
  }

  def buildCromwellRunnerChartOverrideValues(config: AKSInterpreterConfig,
                                             release: Release,
                                             appName: AppName,
                                             cloudContext: AzureCloudContext,
                                             workspaceId: WorkspaceId,
                                             landingZoneResources: LandingZoneResources,
                                             relayPath: Uri,
                                             petManagedIdentity: Option[Identity],
                                             storageContainer: StorageContainerResponse,
                                             batchAccountKey: BatchAccountKey,
                                             applicationInsightsConnectionString: String,
                                             sourceWorkspaceId: Option[WorkspaceId],
                                             userAccessToken: String,
                                             identityType: IdentityType,
                                             maybeDatabaseNames: Option[CromwellRunnerDatabaseNames]
  ): Values = {
    val valuesList =
      List(
        // azure resources configs
        raw"config.resourceGroup=${cloudContext.managedResourceGroupName.value}",
        raw"config.batchAccountKey=${batchAccountKey.value}",
        raw"config.batchAccountName=${landingZoneResources.batchAccountName.value}",
        raw"config.batchNodesSubnetId=${landingZoneResources.batchNodesSubnetName.value}",
        raw"config.drsUrl=${config.drsConfig.url}",
        raw"config.landingZoneId=${landingZoneResources.landingZoneId}",
        raw"config.subscriptionId=${cloudContext.subscriptionId.value}",
        raw"config.region=${landingZoneResources.region}",
        raw"config.applicationInsightsConnectionString=${applicationInsightsConnectionString}",

        // relay configs
        raw"relay.path=${relayPath.renderString}",

        // persistence configs
        raw"persistence.storageResourceGroup=${cloudContext.managedResourceGroupName.value}",
        raw"persistence.storageAccount=${landingZoneResources.storageAccountName.value}",
        raw"persistence.blobContainer=${storageContainer.name.value}",
        raw"persistence.leoAppInstanceName=${appName.value}",
        raw"persistence.workspaceManager.url=${config.wsmConfig.uri.renderString}",
        raw"persistence.workspaceManager.workspaceId=${workspaceId.value}",
        raw"persistence.workspaceManager.containerResourceId=${storageContainer.resourceId.value.toString}",

        // identity configs
        raw"identity.enabled=${identityType == PodIdentity}",
        raw"identity.name=${petManagedIdentity.map(_.name).getOrElse("none")}",
        raw"identity.resourceId=${petManagedIdentity.map(_.id).getOrElse("none")}",
        raw"identity.clientId=${petManagedIdentity.map(_.clientId).getOrElse("none")}",
        raw"workloadIdentity.enabled=${identityType == WorkloadIdentity}",
        raw"workloadIdentity.serviceAccountName=${petManagedIdentity.map(_.name).getOrElse("none")}",

        // Enabled services configs
        raw"cromwell.enabled=${config.cromwellRunnerAppConfig.enabled}",

        // general configs
        raw"fullnameOverride=cra-${release.asString}",
        raw"instrumentationEnabled=${config.coaAppConfig.instrumentationEnabled}",
        // provenance (app-cloning) configs
        raw"provenance.userAccessToken=${userAccessToken}"
      )

    val postgresConfig = (maybeDatabaseNames, landingZoneResources.postgresServer, petManagedIdentity) match {
      case (Some(databaseNames), Some(PostgresServer(dbServerName, pgBouncerEnabled)), Some(pet)) =>
        List(
          raw"postgres.podLocalDatabaseEnabled=false",
          raw"postgres.host=$dbServerName.postgres.database.azure.com",
          raw"postgres.pgbouncer.enabled=$pgBouncerEnabled",
          // convention is that the database user is the same as the service account name
          raw"postgres.user=${pet.name()}",
          raw"postgres.dbnames.cromwell=${databaseNames.cromwellRunner}",
          raw"postgres.dbnames.tes=${databaseNames.tes}"
        )
      case _ => List.empty
    }

    Values((valuesList ++ postgresConfig).mkString(","))
  }
}
