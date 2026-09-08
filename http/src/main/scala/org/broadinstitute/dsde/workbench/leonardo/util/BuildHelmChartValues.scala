package org.broadinstitute.dsde.workbench.leonardo
package util

import org.broadinstitute.dsde.workbench.google2.GKEModels.NodepoolName
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{NamespaceName, ServiceAccountName}
import org.broadinstitute.dsde.workbench.leonardo.Autopilot
import org.broadinstitute.dsde.workbench.leonardo.dao.CustomAppService
import org.broadinstitute.dsde.workbench.leonardo.http.kubernetesProxyHost
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.Release

import java.nio.charset.StandardCharsets

private[leonardo] object BuildHelmChartValues {
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
      raw"""config.gcsRegion=us-central1""",
      raw"""config.backend=${config.cromwellAppConfig.backend.value}""",
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
      // [IA-4997] to support CHIPS by setting partitioned cookies
      // raw"""ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-cookie-path=/ "/; Secure; SameSite=None; HttpOnly; Partitioned"""",
      raw"""ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-cookie-path=/ "/; Secure; SameSite=None; HttpOnly"""",
      raw"""ingress.host=${k8sProxyHostString}""",
      raw"""ingress.tls[0].secretName=tls-secret""",
      raw"""ingress.tls[0].hosts[0]=${k8sProxyHostString}"""
    )

    // Support workload identity following https://cloud.google.com/kubernetes-engine/docs/how-to/workload-separation#separate-workloads-autopilot.
    val nodeSelectorGroupValue = getNodeSelectorGroupValue(userEmail)
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

  // nodeSelector.group value has the following restrictions:
  // a valid label must be an empty string or consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character
  // (e.g. 'MyValue',  or 'my_value',  or '12345', regex used for validation is '(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?'),
  // spec.template.spec.tolerations[0].operator: Invalid value: "xxx": a valid label must be an empty string or
  // consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character
  // (e.g. 'MyValue',  or 'my_value',  or '12345', regex used for validation is '(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?')], string=
  //
  // Use sha256 of the user email here so that the group value will always satisfy the naming restrictions
  private[leonardo] def getNodeSelectorGroupValue(userEmail: WorkbenchEmail): String = {
    val hashedEmail = com.google.common.hash.Hashing
      .sha256()
      .hashString(userEmail.value, StandardCharsets.UTF_8)
      .toString

    s"leo_${hashedEmail}".substring(0, 60)
  }
}
