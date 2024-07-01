package org.broadinstitute.dsde.workbench.leonardo
package util

import org.broadinstitute.dsde.workbench.leonardo.Autopilot
import org.broadinstitute.dsde.workbench.azure.{PrimaryKey, RelayHybridConnectionName, RelayNamespace}
import org.broadinstitute.dsde.workbench.google2.DiskName
import org.broadinstitute.dsde.workbench.google2.GKEModels.NodepoolName
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{NamespaceName, ServiceAccountName}
import org.broadinstitute.dsde.workbench.leonardo.AppRestore.GalaxyRestore
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.AppSamResourceId
import org.broadinstitute.dsde.workbench.leonardo.config.SamConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.CustomAppService
import org.broadinstitute.dsde.workbench.leonardo.http.kubernetesProxyHost
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.{Release, Values}

import java.net.URL
import java.nio.charset.StandardCharsets

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
                                                nodepoolName: Option[NodepoolName],
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

    val nodepoolSelector = nodepoolName.map(n => raw"""nodeSelector.cloud\.google\.com/gke-nodepool=${n.value}""")
    List(
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
    ) ++ ingress ++ nodepoolSelector
  }

  def buildCustomChartOverrideValuesString(config: GKEInterpreterConfig,
                                           appName: AppName,
                                           release: Release,
                                           nodepoolName: Option[NodepoolName],
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

    val nodepool = nodepoolName.map(n => raw"""nodeSelector.cloud\.google\.com/gke-nodepool=${n.value}""")
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
      // Persistence
      raw"""persistence.size=${disk.size.gb.toString}G""",
      raw"""persistence.gcePersistentDisk=${disk.name.value}""",
      raw"""persistence.mountPath=${service.pdMountPath}""",
      raw"""persistence.accessMode=${service.pdAccessMode}""",
      raw"""serviceAccount.name=${ksaName.value}"""
    ) ++ command ++ args ++ configs ++ ingress ++ nodepool).mkString(",")
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
      case AppType.WorkflowsApp      => s"http://wfa-${release.asString}-reverse-proxy-service:8000/"
      case AppType.Jupyter =>
        s"http://jupyter-${release.asString}:8000/" // TODO (LM) may need to switch order http://jupyter:8000%{REQUEST_URI}
      case _ => "unknown"
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

  def buildAllowedAppChartOverrideValuesString(config: GKEInterpreterConfig,
                                               allowedChartName: AllowedChartName,
                                               appName: AppName,
                                               cluster: KubernetesCluster,
                                               nodepoolName: Option[NodepoolName],
                                               namespaceName: NamespaceName,
                                               disk: PersistentDisk,
                                               ksaName: ServiceAccountName,
                                               userEmail: WorkbenchEmail,
                                               stagingBucket: GcsBucketName,
                                               customEnvironmentVariables: Map[String, String],
                                               autopilot: Option[Autopilot],
                                               bucketNameToMount: Option[GcsBucketName]
  ): List[String] = {
    val ingressPath = s"/proxy/google/v1/apps/${cluster.cloudContext.asString}/${appName.value}/app"
    val welderIngressPath = s"/proxy/google/v1/apps/${cluster.cloudContext.asString}/${appName.value}/welder-service"
    val k8sProxyHost = kubernetesProxyHost(cluster, config.proxyConfig.proxyDomain)
    val common = buildAllowedAppCommonChartValuesString(
      config,
      appName,
      cluster,
      nodepoolName,
      namespaceName,
      disk,
      ksaName,
      userEmail,
      stagingBucket,
      customEnvironmentVariables,
      ingressPath,
      k8sProxyHost,
      autopilot,
      bucketNameToMount
    )

    allowedChartName match {
      case AllowedChartName.RStudio =>
        List(
          raw"""ingress.rstudio.path=${ingressPath}${"(/|$)(.*)"}""",
          raw"""ingress.welder.path=${welderIngressPath}${"(/|$)(.*)"}""",
          raw"""ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-from=https://${k8sProxyHost
              .address()}"""
        ) ++ common
      case AllowedChartName.Sas =>
        List(
          raw"""ingress.path.sas=${ingressPath}${"(/|$)(.*)"}""",
          raw"""ingress.path.welder=${welderIngressPath}${"(/|$)(.*)"}""",
          raw"""ingress.proxyPath=${ingressPath}""",
          raw"""ingress.referer=${config.proxyConfig.getProxyServerHostName}""",
          raw"""ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-from=http://${k8sProxyHost
              .address()}""",
          raw"""imageCredentials.username=${config.allowedAppConfig.sasContainerRegistryCredentials.username.asString}""",
          raw"""imageCredentials.password=${config.allowedAppConfig.sasContainerRegistryCredentials.password.asString}"""
        ) ++ common
    }
  }

  private[util] def buildAllowedAppCommonChartValuesString(config: GKEInterpreterConfig,
                                                           appName: AppName,
                                                           cluster: KubernetesCluster,
                                                           nodepoolName: Option[NodepoolName],
                                                           namespaceName: NamespaceName,
                                                           disk: PersistentDisk,
                                                           ksaName: ServiceAccountName,
                                                           userEmail: WorkbenchEmail,
                                                           stagingBucket: GcsBucketName,
                                                           customEnvironmentVariables: Map[String, String],
                                                           ingressPath: String,
                                                           k8sProxyHost: akka.http.scaladsl.model.Uri.Host,
                                                           autopilot: Option[Autopilot],
                                                           bucketNameToMount: Option[GcsBucketName]
  ): List[String] = {
    val k8sProxyHostString = k8sProxyHost.address
    val leoProxyhost = config.proxyConfig.getProxyServerHostName

    // Custom EV configs
    // todo: This may not apply to SAS apps
    val configs = customEnvironmentVariables.toList.zipWithIndex.flatMap { case ((k, v), i) =>
      List(
        raw"""extraEnv[$i].name=$k""",
        raw"""extraEnv[$i].value=$v""",
        raw"""replicaCount=${config.allowedAppConfig.numOfReplicas.toString}"""
      )
    }

    val rewriteTarget = "$2"
    val ingress = List(
      raw"""ingress.enabled=true""",
      raw"""ingress.annotations.nginx\.ingress\.kubernetes\.io/auth-tls-secret=${namespaceName.value}/ca-secret""",
      raw"""ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-to=${leoProxyhost}${ingressPath}""",
      raw"""ingress.annotations.nginx\.ingress\.kubernetes\.io/rewrite-target=/${rewriteTarget}""",
      raw"""ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-cookie-path=/ "/; Secure; SameSite=None; HttpOnly"""",
      raw"""ingress.host=${k8sProxyHostString}""",
      raw"""ingress.tls[0].secretName=tls-secret""",
      raw"""ingress.tls[0].hosts[0]=${k8sProxyHostString}"""
    )

    // Support workload identity following https://cloud.google.com/kubernetes-engine/docs/how-to/workload-separation#separate-workloads-autopilot.
    // nodeSelector.group value has the following restrictions:
    // a valid label must be an empty string or consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character
    // (e.g. 'MyValue',  or 'my_value',  or '12345', regex used for validation is '(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?'),
    // spec.template.spec.tolerations[0].operator: Invalid value: "xxx": a valid label must be an empty string or
    // consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character
    // (e.g. 'MyValue',  or 'my_value',  or '12345', regex used for validation is '(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?')], string=
    val hashedEmail = com.google.common.hash.Hashing
      .sha256()
      .hashString(userEmail.value, StandardCharsets.UTF_8)
      .toString

    val nodeSelectorGroupValue = s"leo_${hashedEmail}".substring(0, 60)
    val autopilotParams = autopilot match {
      case Some(v) =>
        val ls = List(
          raw"""tolerations.enabled=true""",
          raw"""tolerations.keyValue=${nodeSelectorGroupValue}""",
          raw"""nodeSelector.group=${nodeSelectorGroupValue}""",
          raw"""autopilot.enabled=true""",
          raw"""autopilot.app.cpu=${v.cpuInMillicores}m""",
          raw"""autopilot.app.memory=${v.memoryInGb}Gi""",
          raw"""autopilot.app.ephemeral\-storage=${v.ephemeralStorageInGb}Gi""",
          raw"""autopilot.welder.cpu=${config.clusterConfig.autopilotConfig.welder.cpuInMillicores}m""",
          raw"""autopilot.welder.memory=${config.clusterConfig.autopilotConfig.welder.memoryInGb}Gi""",
          raw"""autopilot.welder.ephemeral\-storage=${config.clusterConfig.autopilotConfig.welder.ephemeralStorageInGb}Gi""",
          raw"""autopilot.wondershaper.cpu=${config.clusterConfig.autopilotConfig.wondershaper.cpuInMillicores}m""",
          raw"""autopilot.wondershaper.memory=${config.clusterConfig.autopilotConfig.wondershaper.memoryInGb}Gi""",
          raw"""autopilot.wondershaper.ephemeral\-storage=${config.clusterConfig.autopilotConfig.wondershaper.ephemeralStorageInGb}Gi"""
        )
        // when it's general purpose, GCP doesn't allow us to pass the compute class value.
        // the API behaves in a way that when the value isn't specified, general-purpose is used
        if (v.computeClass == ComputeClass.GeneralPurpose)
          ls
        else raw"""nodeSelector.cloud\.google\.com/compute-class=${v.computeClass.toString}""" :: ls
      case None => List.empty
    }

    val gcsfuse = bucketNameToMount match {
      case Some(bucketName) =>
        List(
          raw"""gcsfuse.enabled=true""",
          raw"""gcsfuse.bucket=${bucketName.value}"""
        )
      case None =>
        List(
          raw"""gcsfuse.enabled=false"""
        )
    }

    val welder = List(
      raw"""welder.extraEnv[0].name=GOOGLE_PROJECT""",
      raw"""welder.extraEnv[0].value=${cluster.cloudContext.asString}""",
      raw"""welder.extraEnv[1].name=STAGING_BUCKET""",
      raw"""welder.extraEnv[1].value=${stagingBucket.value}""",
      raw"""welder.extraEnv[2].name=CLUSTER_NAME""",
      raw"""welder.extraEnv[2].value=${appName.value}""",
      raw"""welder.extraEnv[3].name=OWNER_EMAIL""",
      raw"""welder.extraEnv[3].value=${userEmail.value}""",
      raw"""welder.extraEnv[4].name=WORKSPACE_ID""",
      raw"""welder.extraEnv[4].value=dummy""", // TODO: welder requires this env, but it's not needed for welders in GCP
      raw"""welder.extraEnv[5].name=WSM_URL""",
      raw"""welder.extraEnv[5].value=dummy""" // TODO: welder requires this env, but it's not needed for welders in GCP
    )

    val nodepoolSelector = nodepoolName match {
      case Some(npn) =>
        List(
          raw"""nodeSelector.cloud\.google\.com/gke-nodepool=${npn.value}"""
        )
      case None =>
        List.empty
    }

    List(
      raw"""fullnameOverride=${appName.value}""",
      // Persistence
      raw"""persistence.size=${disk.size.gb.toString}G""",
      raw"""persistence.gcePersistentDisk=${disk.name.value}""",
      // Service Account
      raw"""serviceAccount.name=${ksaName.value}"""
    ) ++ ingress ++ welder ++ configs ++ nodepoolSelector ++ autopilotParams ++ gcsfuse
  }
}
