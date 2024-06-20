package org.broadinstitute.dsde.workbench.leonardo
package util

import org.broadinstitute.dsde.workbench.azure.{PrimaryKey, RelayHybridConnectionName, RelayNamespace}
import org.broadinstitute.dsde.workbench.google2.DiskName
import org.broadinstitute.dsde.workbench.google2.GKEModels.NodepoolName
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{NamespaceName, ServiceAccountName}
import org.broadinstitute.dsde.workbench.leonardo.AppRestore.GalaxyRestore
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.{makePersistentDisk, userEmail, userEmail2}
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.{makeCustomAppService, makeKubeCluster}
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.AppSamResourceId
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.util.BuildHelmChartValues._
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.Release
import org.scalatest.flatspec.AnyFlatSpecLike

import java.net.URL
import java.util.UUID

class BuildHelmChartValuesSpec extends AnyFlatSpecLike with LeonardoTestSuite {

  it should "build Galaxy override values string" in {
    val savedCluster1 = makeKubeCluster(1)
    val savedDisk1 = makePersistentDisk(Some(DiskName("disk1")), Some(FormattedBy.Galaxy))
    val res = buildGalaxyChartOverrideValuesString(
      Config.gkeInterpConfig,
      AppName("app1"),
      Release("app1-galaxy-rls"),
      savedCluster1,
      NodepoolName("pool1"),
      userEmail,
      Map("WORKSPACE_NAME" -> "test-workspace",
          "WORKSPACE_BUCKET" -> "gs://test-bucket",
          "WORKSPACE_NAMESPACE" -> "dsp-leo-test1"
      ),
      ServiceAccountName("app1-galaxy-ksa"),
      NamespaceName("ns"),
      savedDisk1,
      DiskName("disk1-gxy-postres-disk"),
      AppMachineType(23, 7),
      None
    )

    res.mkString(
      ","
    ) shouldBe
      """nfs.storageClass.name=nfs-app1-galaxy-rls,""" +
      """galaxy.persistence.storageClass=nfs-app1-galaxy-rls,""" +
      """galaxy.nodeSelector.cloud\.google\.com/gke-nodepool=pool1,""" +
      """nfs.nodeSelector.cloud\.google\.com/gke-nodepool=pool1,""" +
      """galaxy.configs.job_conf\.yml.runners.k8s.k8s_node_selector=cloud.google.com/gke-nodepool: pool1,""" +
      """galaxy.postgresql.master.nodeSelector.cloud\.google\.com/gke-nodepool=pool1,""" +
      """galaxy.ingress.path=/proxy/google/v1/apps/dsp-leo-test1/app1/galaxy,""" +
      """galaxy.ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-from=https://1455694897.jupyter.firecloud.org,""" +
      """galaxy.ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-to=https://leo,""" +
      """galaxy.ingress.hosts[0].host=1455694897.jupyter.firecloud.org,""" +
      """galaxy.ingress.hosts[0].paths[0].path=/proxy/google/v1/apps/dsp-leo-test1/app1/galaxy,""" +
      """galaxy.ingress.tls[0].hosts[0]=1455694897.jupyter.firecloud.org,""" +
      """galaxy.ingress.tls[0].secretName=tls-secret,""" +
      """cvmfs.cvmfscsi.cache.alien.pvc.storageClass=nfs-app1-galaxy-rls,""" +
      """cvmfs.cvmfscsi.cache.alien.pvc.name=cvmfs-alien-cache,""" +
      """galaxy.configs.galaxy\.yml.galaxy.single_user=user1@example.com,""" +
      """galaxy.configs.galaxy\.yml.galaxy.admin_users=user1@example.com,""" +
      """galaxy.terra.launch.workspace=test-workspace,""" +
      """galaxy.terra.launch.namespace=dsp-leo-test1,""" +
      """galaxy.terra.launch.apiURL=https://firecloud-orchestration.dsde-dev.broadinstitute.org/api/,""" +
      """galaxy.terra.launch.drsURL=https://drshub.dsde-dev.broadinstitute.org/api/v4/drs/resolve,""" +
      """galaxy.tusd.ingress.hosts[0].host=1455694897.jupyter.firecloud.org,""" +
      """galaxy.tusd.ingress.hosts[0].paths[0].path=/proxy/google/v1/apps/dsp-leo-test1/app1/galaxy/api/upload/resumable_upload,""" +
      """galaxy.tusd.ingress.tls[0].hosts[0]=1455694897.jupyter.firecloud.org,""" +
      """galaxy.tusd.ingress.tls[0].secretName=tls-secret,""" +
      """galaxy.rabbitmq.persistence.storageClassName=nfs-app1-galaxy-rls,""" +
      """galaxy.jobs.maxLimits.memory=23,""" +
      """galaxy.jobs.maxLimits.cpu=7,""" +
      """galaxy.jobs.maxRequests.memory=1,""" +
      """galaxy.jobs.maxRequests.cpu=1,""" +
      """galaxy.jobs.rules.tpv_rules_local\.yml.destinations.k8s.max_mem=1,""" +
      """galaxy.jobs.rules.tpv_rules_local\.yml.destinations.k8s.max_cores=1,""" +
      """galaxy.serviceAccount.create=false,""" +
      """galaxy.serviceAccount.name=app1-galaxy-ksa,""" +
      """rbac.serviceAccount=app1-galaxy-ksa,persistence.nfs.name=ns-nfs-disk,""" +
      """persistence.nfs.persistentVolume.extraSpec.gcePersistentDisk.pdName=disk1,""" +
      """persistence.nfs.size=250Gi,""" +
      """persistence.postgres.name=ns-postgres-disk,""" +
      """galaxy.postgresql.galaxyDatabasePassword=replace-me,""" +
      """persistence.postgres.persistentVolume.extraSpec.gcePersistentDisk.pdName=disk1-gxy-postres-disk,""" +
      """persistence.postgres.size=10Gi,""" +
      """nfs.persistence.existingClaim=ns-nfs-disk-pvc,""" +
      """nfs.persistence.size=250Gi,""" +
      """galaxy.postgresql.persistence.existingClaim=ns-postgres-disk-pvc,""" +
      """galaxy.persistence.size=200Gi,""" +
      """configs.WORKSPACE_NAME=test-workspace,""" +
      """extraEnv[0].name=WORKSPACE_NAME,extraEnv[0].valueFrom.configMapKeyRef.name=app1-galaxy-rls-galaxykubeman-configs,""" +
      """extraEnv[0].valueFrom.configMapKeyRef.key=WORKSPACE_NAME,""" +
      """configs.WORKSPACE_BUCKET=gs://test-bucket,""" +
      """extraEnv[1].name=WORKSPACE_BUCKET,""" +
      """extraEnv[1].valueFrom.configMapKeyRef.name=app1-galaxy-rls-galaxykubeman-configs,""" +
      """extraEnv[1].valueFrom.configMapKeyRef.key=WORKSPACE_BUCKET,""" +
      """configs.WORKSPACE_NAMESPACE=dsp-leo-test1,""" +
      """extraEnv[2].name=WORKSPACE_NAMESPACE,""" +
      """extraEnv[2].valueFrom.configMapKeyRef.name=app1-galaxy-rls-galaxykubeman-configs,""" +
      """extraEnv[2].valueFrom.configMapKeyRef.key=WORKSPACE_NAMESPACE"""
  }

  it should "build Galaxy override values string with restore info" in {
    val savedCluster1 = makeKubeCluster(1)
    val savedDisk1 = makePersistentDisk(Some(DiskName("disk1")), Some(FormattedBy.Galaxy))
    val result =
      buildGalaxyChartOverrideValuesString(
        Config.gkeInterpConfig,
        AppName("app1"),
        Release("app1-galaxy-rls"),
        savedCluster1,
        NodepoolName("pool1"),
        userEmail,
        Map("WORKSPACE_NAME" -> "test-workspace",
            "WORKSPACE_BUCKET" -> "gs://test-bucket",
            "WORKSPACE_NAMESPACE" -> "dsp-leo-test1"
        ),
        ServiceAccountName("app1-galaxy-ksa"),
        NamespaceName("ns"),
        savedDisk1,
        DiskName("disk1-gxy-postres"),
        AppMachineType(23, 7),
        Some(
          GalaxyRestore(PvcId("galaxy-pvc-id"), AppId(123))
        )
      )
    result.mkString(
      ","
    ) shouldBe
      """nfs.storageClass.name=nfs-app1-galaxy-rls,""" +
      """galaxy.persistence.storageClass=nfs-app1-galaxy-rls,""" +
      """galaxy.nodeSelector.cloud\.google\.com/gke-nodepool=pool1,""" +
      """nfs.nodeSelector.cloud\.google\.com/gke-nodepool=pool1,""" +
      """galaxy.configs.job_conf\.yml.runners.k8s.k8s_node_selector=cloud.google.com/gke-nodepool: pool1,""" +
      """galaxy.postgresql.master.nodeSelector.cloud\.google\.com/gke-nodepool=pool1,""" +
      """galaxy.ingress.path=/proxy/google/v1/apps/dsp-leo-test1/app1/galaxy,""" +
      """galaxy.ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-from=https://1455694897.jupyter.firecloud.org,""" +
      """galaxy.ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-to=https://leo,""" +
      """galaxy.ingress.hosts[0].host=1455694897.jupyter.firecloud.org,""" +
      """galaxy.ingress.hosts[0].paths[0].path=/proxy/google/v1/apps/dsp-leo-test1/app1/galaxy,""" +
      """galaxy.ingress.tls[0].hosts[0]=1455694897.jupyter.firecloud.org,""" +
      """galaxy.ingress.tls[0].secretName=tls-secret,""" +
      """cvmfs.cvmfscsi.cache.alien.pvc.storageClass=nfs-app1-galaxy-rls,""" +
      """cvmfs.cvmfscsi.cache.alien.pvc.name=cvmfs-alien-cache,""" +
      """galaxy.configs.galaxy\.yml.galaxy.single_user=user1@example.com,""" +
      """galaxy.configs.galaxy\.yml.galaxy.admin_users=user1@example.com,""" +
      """galaxy.terra.launch.workspace=test-workspace,""" +
      """galaxy.terra.launch.namespace=dsp-leo-test1,""" +
      """galaxy.terra.launch.apiURL=https://firecloud-orchestration.dsde-dev.broadinstitute.org/api/,""" +
      """galaxy.terra.launch.drsURL=https://drshub.dsde-dev.broadinstitute.org/api/v4/drs/resolve,""" +
      """galaxy.tusd.ingress.hosts[0].host=1455694897.jupyter.firecloud.org,""" +
      """galaxy.tusd.ingress.hosts[0].paths[0].path=/proxy/google/v1/apps/dsp-leo-test1/app1/galaxy/api/upload/resumable_upload,""" +
      """galaxy.tusd.ingress.tls[0].hosts[0]=1455694897.jupyter.firecloud.org,""" +
      """galaxy.tusd.ingress.tls[0].secretName=tls-secret,""" +
      """galaxy.rabbitmq.persistence.storageClassName=nfs-app1-galaxy-rls,""" +
      """galaxy.jobs.maxLimits.memory=23,""" +
      """galaxy.jobs.maxLimits.cpu=7,""" +
      """galaxy.jobs.maxRequests.memory=1,""" +
      """galaxy.jobs.maxRequests.cpu=1,""" +
      """galaxy.jobs.rules.tpv_rules_local\.yml.destinations.k8s.max_mem=1,""" +
      """galaxy.jobs.rules.tpv_rules_local\.yml.destinations.k8s.max_cores=1,""" +
      """galaxy.serviceAccount.create=false,""" +
      """galaxy.serviceAccount.name=app1-galaxy-ksa,""" +
      """rbac.serviceAccount=app1-galaxy-ksa,""" +
      """persistence.nfs.name=ns-nfs-disk,""" +
      """persistence.nfs.persistentVolume.extraSpec.gcePersistentDisk.pdName=disk1,persistence.nfs.size=250Gi,""" +
      """persistence.postgres.name=ns-postgres-disk,""" +
      """galaxy.postgresql.galaxyDatabasePassword=replace-me,""" +
      """persistence.postgres.persistentVolume.extraSpec.gcePersistentDisk.pdName=disk1-gxy-postres,""" +
      """persistence.postgres.size=10Gi,""" +
      """nfs.persistence.existingClaim=ns-nfs-disk-pvc,""" +
      """nfs.persistence.size=250Gi,""" +
      """galaxy.postgresql.persistence.existingClaim=ns-postgres-disk-pvc,""" +
      """galaxy.persistence.size=200Gi,""" +
      """configs.WORKSPACE_NAME=test-workspace,""" +
      """extraEnv[0].name=WORKSPACE_NAME,""" +
      """extraEnv[0].valueFrom.configMapKeyRef.name=app1-galaxy-rls-galaxykubeman-configs,""" +
      """extraEnv[0].valueFrom.configMapKeyRef.key=WORKSPACE_NAME,""" +
      """configs.WORKSPACE_BUCKET=gs://test-bucket,""" +
      """extraEnv[1].name=WORKSPACE_BUCKET,""" +
      """extraEnv[1].valueFrom.configMapKeyRef.name=app1-galaxy-rls-galaxykubeman-configs,""" +
      """extraEnv[1].valueFrom.configMapKeyRef.key=WORKSPACE_BUCKET,""" +
      """configs.WORKSPACE_NAMESPACE=dsp-leo-test1,""" +
      """extraEnv[2].name=WORKSPACE_NAMESPACE,""" +
      """extraEnv[2].valueFrom.configMapKeyRef.name=app1-galaxy-rls-galaxykubeman-configs,""" +
      """extraEnv[2].valueFrom.configMapKeyRef.key=WORKSPACE_NAMESPACE,""" +
      """restore.persistence.nfs.galaxy.pvcID=galaxy-pvc-id,""" +
      """galaxy.persistence.existingClaim=app1-galaxy-rls-galaxy-galaxy-pvc""".stripMargin
  }

  it should "build Cromwell override values string" in {
    val savedCluster1 = makeKubeCluster(1)
    val savedDisk1 = makePersistentDisk(Some(DiskName("disk1")))
    val envVariables = Map("WORKSPACE_BUCKET" -> "gs://test-bucket")
    val res = buildCromwellAppChartOverrideValuesString(
      Config.gkeInterpConfig,
      appName = AppName("app1"),
      cluster = savedCluster1,
      nodepoolName = Some(NodepoolName("pool1")),
      namespaceName = NamespaceName("ns"),
      disk = savedDisk1,
      ksaName = ServiceAccountName("app1-cromwell-ksa"),
      gsa = WorkbenchEmail("pet123-abc@terra-test-abc.iam.gserviceaccount.com"),
      customEnvironmentVariables = envVariables
    )

    res.mkString(",") shouldBe
      """persistence.size=250G,""" +
      """persistence.gcePersistentDisk=disk1,""" +
      """env.swaggerBasePath=/proxy/google/v1/apps/dsp-leo-test1/app1/cromwell-service/cromwell,""" +
      """config.gcsProject=dsp-leo-test1,""" +
      """config.gcsBucket=gs://test-bucket/cromwell-execution,""" +
      """config.serviceAccount.name=app1-cromwell-ksa,""" +
      """config.serviceAccount.annotations.gcpServiceAccount=pet123-abc@terra-test-abc.iam.gserviceaccount.com,""" +
      """ingress.enabled=true,""" +
      """ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-from=https://1455694897.jupyter.firecloud.org,""" +
      """ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-to=https://leo,""" +
      """ingress.annotations.nginx\.ingress\.kubernetes\.io/rewrite-target=/$2,""" +
      """ingress.annotations.nginx\.ingress\.kubernetes\.io/auth-tls-secret=ns/ca-secret,""" +
      """ingress.path=/proxy/google/v1/apps/dsp-leo-test1/app1/cromwell-service,""" +
      """ingress.hosts[0].host=1455694897.jupyter.firecloud.org,""" +
      """ingress.hosts[0].paths[0]=/proxy/google/v1/apps/dsp-leo-test1/app1/cromwell-service(/|$)(.*),""" +
      """ingress.tls[0].secretName=tls-secret,""" +
      """ingress.tls[0].hosts[0]=1455694897.""" +
      """jupyter.firecloud.org,""" +
      """db.password=replace-me,""" +
      """nodeSelector.cloud\.google\.com/gke-nodepool=pool1"""
  }

  it should "build Custom App override values string" in {
    val savedCluster1 = makeKubeCluster(1)
    val customService = makeCustomAppService();
    val savedDisk1 = makePersistentDisk(Some(DiskName("disk1")))
    val envVariables = Map("WORKSPACE_BUCKET" -> "gs://test-bucket")
    val res = buildCustomChartOverrideValuesString(
      Config.gkeInterpConfig,
      appName = AppName("app1"),
      release = Release("app1-custom-rls"),
      nodepoolName = Some(NodepoolName("pool1")),
      serviceName = "custom-service",
      savedCluster1,
      namespaceName = NamespaceName("ns"),
      customService,
      extraArgs = List("/usr/bin", "extra"),
      disk = savedDisk1,
      ksaName = ServiceAccountName("app1-ksa"),
      customEnvironmentVariables = envVariables
    )

    res shouldBe
      """nameOverride=custom-service,""" +
      """image.image=us.gcr.io/anvil-gcr-public/anvil-rstudio-bioconductor:0.0.10,""" +
      """image.port=8001,""" +
      """image.baseUrl=/,""" +
      """ingress.hosts[0].host=1455694897.jupyter.firecloud.org,""" +
      """ingress.annotations.nginx\.ingress\.kubernetes\.io/auth-tls-secret=ns/ca-secret,""" +
      """ingress.tls[0].secretName=tls-secret,""" +
      """ingress.tls[0].hosts[0]=1455694897.jupyter.firecloud.org,""" +
      """persistence.size=250G,""" +
      """persistence.gcePersistentDisk=disk1,""" +
      """persistence.mountPath=/data,""" +
      """persistence.accessMode=ReadWriteOnce,""" +
      """serviceAccount.name=app1-ksa,""" +
      """image.command[0]=/bin/sh,""" +
      """image.command[1]=-c,""" +
      """image.args[0]=sed -i 's/^www-address.*$//' $RSTUDIO_HOME/rserver.conf && /init,""" +
      """image.args[1]=/usr/bin,""" +
      """image.args[2]=extra,""" +
      """extraEnv[0].name=WORKSPACE_BUCKET,""" +
      """extraEnv[0].value=gs://test-bucket,""" +
      """ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-from=https://1455694897.jupyter.firecloud.org,""" +
      """ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-to=https://leo/proxy/google/v1/apps/dsp-leo-test1/app1/custom-service,""" +
      """ingress.annotations.nginx\.ingress\.kubernetes\.io/rewrite-target=/$2,""" +
      """ingress.hosts[0].paths[0]=/proxy/google/v1/apps/dsp-leo-test1/app1/custom-service(/|$)(.*),""" +
      """nodeSelector.cloud\.google\.com/gke-nodepool=pool1"""
  }

  it should "build RStudio override values string" in {
    val savedCluster1 = makeKubeCluster(1)
    val savedDisk1 = makePersistentDisk(Some(DiskName("disk1")))
    val envVariables = Map("WORKSPACE_NAME" -> "test-workspace-name")
    val res = buildAllowedAppChartOverrideValuesString(
      Config.gkeInterpConfig,
      AllowedChartName.RStudio,
      appName = AppName("app1"),
      cluster = savedCluster1,
      nodepoolName = Some(NodepoolName("pool1")),
      namespaceName = NamespaceName("ns"),
      disk = savedDisk1,
      ksaName = ServiceAccountName("app1-rstudio-ksa"),
      userEmail = userEmail2,
      stagingBucket = GcsBucketName("test-staging-bucket"),
      envVariables,
      None,
      Some(GcsBucketName("fc-bucket"))
    )

    res.mkString(",") shouldBe
      """ingress.rstudio.path=/proxy/google/v1/apps/dsp-leo-test1/app1/app(/|$)(.*),""" +
      """ingress.welder.path=/proxy/google/v1/apps/dsp-leo-test1/app1/welder-service(/|$)(.*),""" +
      """ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-from=https://1455694897.jupyter.firecloud.org,""" +
      """fullnameOverride=app1,""" +
      """persistence.size=250G,""" +
      """persistence.gcePersistentDisk=disk1,""" +
      """serviceAccount.name=app1-rstudio-ksa,""" +
      """ingress.enabled=true,""" +
      """ingress.annotations.nginx\.ingress\.kubernetes\.io/auth-tls-secret=ns/ca-secret,""" +
      """ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-to=https://leo/proxy/google/v1/apps/dsp-leo-test1/app1/app,""" +
      """ingress.annotations.nginx\.ingress\.kubernetes\.io/rewrite-target=/$2,""" +
      """ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-cookie-path=/ "/; Secure; SameSite=None; HttpOnly; Partitioned",""" +
      """ingress.host=1455694897.jupyter.firecloud.org,""" +
      """ingress.tls[0].secretName=tls-secret,""" +
      """ingress.tls[0].hosts[0]=1455694897.jupyter.firecloud.org,""" +
      """welder.extraEnv[0].name=GOOGLE_PROJECT,""" +
      """welder.extraEnv[0].value=dsp-leo-test1,""" +
      """welder.extraEnv[1].name=STAGING_BUCKET,""" +
      """welder.extraEnv[1].value=test-staging-bucket,""" +
      """welder.extraEnv[2].name=CLUSTER_NAME,""" +
      """welder.extraEnv[2].value=app1,""" +
      """welder.extraEnv[3].name=OWNER_EMAIL,""" +
      """welder.extraEnv[3].value=user2@example.com,""" +
      """welder.extraEnv[4].name=WORKSPACE_ID,""" +
      """welder.extraEnv[4].value=dummy,""" +
      """welder.extraEnv[5].name=WSM_URL,""" +
      """welder.extraEnv[5].value=dummy,""" +
      """extraEnv[0].name=WORKSPACE_NAME,""" +
      """extraEnv[0].value=test-workspace-name,""" +
      """replicaCount=1,""" +
      """nodeSelector.cloud\.google\.com/gke-nodepool=pool1,""" +
      """gcsfuse.enabled=true,""" +
      """gcsfuse.bucket=fc-bucket"""
  }

  it should "build SAS override values string" in {
    val savedCluster1 = makeKubeCluster(1)
    val savedDisk1 = makePersistentDisk(Some(DiskName("disk1")))
    val envVariables = Map("WORKSPACE_NAME" -> "test-workspace-name")
    val res = buildAllowedAppChartOverrideValuesString(
      Config.gkeInterpConfig,
      AllowedChartName.Sas,
      appName = AppName("app1"),
      cluster = savedCluster1,
      nodepoolName = Some(NodepoolName("pool1")),
      namespaceName = NamespaceName("ns"),
      disk = savedDisk1,
      ksaName = ServiceAccountName("app1-rstudio-ksa"),
      userEmail = userEmail2,
      stagingBucket = GcsBucketName("test-staging-bucket"),
      envVariables,
      None,
      Some(GcsBucketName("fc-bucket"))
    )

    res.mkString(",") shouldBe
      """ingress.path.sas=/proxy/google/v1/apps/dsp-leo-test1/app1/app(/|$)(.*),""" +
      """ingress.path.welder=/proxy/google/v1/apps/dsp-leo-test1/app1/welder-service(/|$)(.*),""" +
      """ingress.proxyPath=/proxy/google/v1/apps/dsp-leo-test1/app1/app,""" +
      """ingress.referer=https://leo,""" +
      """ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-from=http://1455694897.jupyter.firecloud.org,""" +
      """imageCredentials.username=sasUserName,""" +
      """imageCredentials.password=sasPassword,""" +
      """fullnameOverride=app1,""" +
      """persistence.size=250G,""" +
      """persistence.gcePersistentDisk=disk1,""" +
      """serviceAccount.name=app1-rstudio-ksa,""" +
      """ingress.enabled=true,""" +
      """ingress.annotations.nginx\.ingress\.kubernetes\.io/auth-tls-secret=ns/ca-secret,""" +
      """ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-to=https://leo/proxy/google/v1/apps/dsp-leo-test1/app1/app,""" +
      """ingress.annotations.nginx\.ingress\.kubernetes\.io/rewrite-target=/$2,""" +
      """ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-cookie-path=/ "/; Secure; SameSite=None; HttpOnly; Partitioned",""" +
      """ingress.host=1455694897.jupyter.firecloud.org,""" +
      """ingress.tls[0].secretName=tls-secret,""" +
      """ingress.tls[0].hosts[0]=1455694897.jupyter.firecloud.org,""" +
      """welder.extraEnv[0].name=GOOGLE_PROJECT,""" +
      """welder.extraEnv[0].value=dsp-leo-test1,""" +
      """welder.extraEnv[1].name=STAGING_BUCKET,""" +
      """welder.extraEnv[1].value=test-staging-bucket,""" +
      """welder.extraEnv[2].name=CLUSTER_NAME,""" +
      """welder.extraEnv[2].value=app1,""" +
      """welder.extraEnv[3].name=OWNER_EMAIL,""" +
      """welder.extraEnv[3].value=user2@example.com,""" +
      """welder.extraEnv[4].name=WORKSPACE_ID,""" +
      """welder.extraEnv[4].value=dummy,""" +
      """welder.extraEnv[5].name=WSM_URL,""" +
      """welder.extraEnv[5].value=dummy,""" +
      """extraEnv[0].name=WORKSPACE_NAME,""" +
      """extraEnv[0].value=test-workspace-name,""" +
      """replicaCount=1,""" +
      """nodeSelector.cloud\.google\.com/gke-nodepool=pool1,""" +
      """gcsfuse.enabled=true,""" +
      """gcsfuse.bucket=fc-bucket"""
  }

  it should "build SAS override values string in autopilot mode" in {
    val savedCluster1 = makeKubeCluster(1)
    val savedDisk1 = makePersistentDisk(Some(DiskName("disk1")))
    val envVariables = Map("WORKSPACE_NAME" -> "test-workspace-name")
    val res = buildAllowedAppChartOverrideValuesString(
      Config.gkeInterpConfig,
      AllowedChartName.Sas,
      appName = AppName("app1"),
      cluster = savedCluster1,
      nodepoolName = None,
      namespaceName = NamespaceName("ns"),
      disk = savedDisk1,
      ksaName = ServiceAccountName("app1-rstudio-ksa"),
      userEmail = userEmail2,
      stagingBucket = GcsBucketName("test-staging-bucket"),
      envVariables,
      Some(Autopilot(ComputeClass.Balanced, 500, 1, 2)),
      Some(GcsBucketName("fc-bucket"))
    )

    res.mkString(",") shouldBe
      """ingress.path.sas=/proxy/google/v1/apps/dsp-leo-test1/app1/app(/|$)(.*),""" +
      """ingress.path.welder=/proxy/google/v1/apps/dsp-leo-test1/app1/welder-service(/|$)(.*),""" +
      """ingress.proxyPath=/proxy/google/v1/apps/dsp-leo-test1/app1/app,""" +
      """ingress.referer=https://leo,""" +
      """ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-from=http://1455694897.jupyter.firecloud.org,""" +
      """imageCredentials.username=sasUserName,""" +
      """imageCredentials.password=sasPassword,""" +
      """fullnameOverride=app1,""" +
      """persistence.size=250G,""" +
      """persistence.gcePersistentDisk=disk1,""" +
      """serviceAccount.name=app1-rstudio-ksa,""" +
      """ingress.enabled=true,""" +
      """ingress.annotations.nginx\.ingress\.kubernetes\.io/auth-tls-secret=ns/ca-secret,""" +
      """ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-to=https://leo/proxy/google/v1/apps/dsp-leo-test1/app1/app,""" +
      """ingress.annotations.nginx\.ingress\.kubernetes\.io/rewrite-target=/$2,""" +
      """ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-cookie-path=/ "/; Secure; SameSite=None; HttpOnly; Partitioned",""" +
      """ingress.host=1455694897.jupyter.firecloud.org,""" +
      """ingress.tls[0].secretName=tls-secret,""" +
      """ingress.tls[0].hosts[0]=1455694897.jupyter.firecloud.org,""" +
      """welder.extraEnv[0].name=GOOGLE_PROJECT,""" +
      """welder.extraEnv[0].value=dsp-leo-test1,""" +
      """welder.extraEnv[1].name=STAGING_BUCKET,""" +
      """welder.extraEnv[1].value=test-staging-bucket,""" +
      """welder.extraEnv[2].name=CLUSTER_NAME,""" +
      """welder.extraEnv[2].value=app1,""" +
      """welder.extraEnv[3].name=OWNER_EMAIL,""" +
      """welder.extraEnv[3].value=user2@example.com,""" +
      """welder.extraEnv[4].name=WORKSPACE_ID,""" +
      """welder.extraEnv[4].value=dummy,""" +
      """welder.extraEnv[5].name=WSM_URL,""" +
      """welder.extraEnv[5].value=dummy,""" +
      """extraEnv[0].name=WORKSPACE_NAME,""" +
      """extraEnv[0].value=test-workspace-name,""" +
      """replicaCount=1,""" +
      """nodeSelector.cloud\.google\.com/compute-class=Balanced,""" +
      """autopilot.enabled=true,autopilot.app.cpu=500m,""" +
      """autopilot.app.memory=1Gi,autopilot.app.ephemeral\-storage=2Gi,""" +
      """autopilot.welder.cpu=500m,autopilot.welder.memory=3Gi,""" +
      """autopilot.welder.ephemeral\-storage=1Gi,""" +
      """autopilot.wondershaper.cpu=500m,""" +
      """autopilot.wondershaper.memory=3Gi,""" +
      """autopilot.wondershaper.ephemeral\-storage=1Gi""".stripMargin + "," +
      """gcsfuse.enabled=true,""" +
      """gcsfuse.bucket=fc-bucket"""
  }

  it should "build SAS override values string in autopilot mode without compute-class when it's General-purpose" in {
    val savedCluster1 = makeKubeCluster(1)
    val savedDisk1 = makePersistentDisk(Some(DiskName("disk1")))
    val envVariables = Map("WORKSPACE_NAME" -> "test-workspace-name")
    val res = buildAllowedAppChartOverrideValuesString(
      Config.gkeInterpConfig,
      AllowedChartName.Sas,
      appName = AppName("app1"),
      cluster = savedCluster1,
      nodepoolName = None,
      namespaceName = NamespaceName("ns"),
      disk = savedDisk1,
      ksaName = ServiceAccountName("app1-rstudio-ksa"),
      userEmail = userEmail2,
      stagingBucket = GcsBucketName("test-staging-bucket"),
      envVariables,
      Some(Autopilot(ComputeClass.GeneralPurpose, 500, 1, 2)),
      Some(GcsBucketName("fc-bucket"))
    )

    res.mkString(",") shouldBe
      """ingress.path.sas=/proxy/google/v1/apps/dsp-leo-test1/app1/app(/|$)(.*),""" +
      """ingress.path.welder=/proxy/google/v1/apps/dsp-leo-test1/app1/welder-service(/|$)(.*),""" +
      """ingress.proxyPath=/proxy/google/v1/apps/dsp-leo-test1/app1/app,""" +
      """ingress.referer=https://leo,""" +
      """ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-from=http://1455694897.jupyter.firecloud.org,""" +
      """imageCredentials.username=sasUserName,""" +
      """imageCredentials.password=sasPassword,""" +
      """fullnameOverride=app1,""" +
      """persistence.size=250G,""" +
      """persistence.gcePersistentDisk=disk1,""" +
      """serviceAccount.name=app1-rstudio-ksa,""" +
      """ingress.enabled=true,""" +
      """ingress.annotations.nginx\.ingress\.kubernetes\.io/auth-tls-secret=ns/ca-secret,""" +
      """ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-to=https://leo/proxy/google/v1/apps/dsp-leo-test1/app1/app,""" +
      """ingress.annotations.nginx\.ingress\.kubernetes\.io/rewrite-target=/$2,""" +
      """ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-cookie-path=/ "/; Secure; SameSite=None; HttpOnly; Partitioned",""" +
      """ingress.host=1455694897.jupyter.firecloud.org,""" +
      """ingress.tls[0].secretName=tls-secret,""" +
      """ingress.tls[0].hosts[0]=1455694897.jupyter.firecloud.org,""" +
      """welder.extraEnv[0].name=GOOGLE_PROJECT,""" +
      """welder.extraEnv[0].value=dsp-leo-test1,""" +
      """welder.extraEnv[1].name=STAGING_BUCKET,""" +
      """welder.extraEnv[1].value=test-staging-bucket,""" +
      """welder.extraEnv[2].name=CLUSTER_NAME,""" +
      """welder.extraEnv[2].value=app1,""" +
      """welder.extraEnv[3].name=OWNER_EMAIL,""" +
      """welder.extraEnv[3].value=user2@example.com,""" +
      """welder.extraEnv[4].name=WORKSPACE_ID,""" +
      """welder.extraEnv[4].value=dummy,""" +
      """welder.extraEnv[5].name=WSM_URL,""" +
      """welder.extraEnv[5].value=dummy,""" +
      """extraEnv[0].name=WORKSPACE_NAME,""" +
      """extraEnv[0].value=test-workspace-name,""" +
      """replicaCount=1,""" +
      """autopilot.enabled=true,autopilot.app.cpu=500m,""" +
      """autopilot.app.memory=1Gi,autopilot.app.ephemeral\-storage=2Gi,""" +
      """autopilot.welder.cpu=500m,autopilot.welder.memory=3Gi,""" +
      """autopilot.welder.ephemeral\-storage=1Gi,""" +
      """autopilot.wondershaper.cpu=500m,""" +
      """autopilot.wondershaper.memory=3Gi,""" +
      """autopilot.wondershaper.ephemeral\-storage=1Gi""".stripMargin + "," +
      """gcsfuse.enabled=true,""" +
      """gcsfuse.bucket=fc-bucket"""
  }

  it should "build relay listener override values string" in {
    val workspaceId = WorkspaceId(UUID.randomUUID)
    val res = buildListenerChartOverrideValuesString(
      Release("rl-rls"),
      AppSamResourceId("sam-id", Some(AppAccessScope.WorkspaceShared)),
      RelayNamespace("relay-ns"),
      RelayHybridConnectionName("hc-name"),
      PrimaryKey("hc-name"),
      AppType.Wds,
      workspaceId,
      AppName("app1"),
      Set("example.com", "foo.com", "bar.org"),
      Config.samConfig,
      "acr/listener:1",
      new URL("https://leo.com")
    )
    res.asString shouldBe
      "connection.removeEntityPathFromHttpUrl=\"true\"," +
      "connection.connectionString=Endpoint=sb://relay-ns.servicebus.windows.net/;SharedAccessKeyName=listener;SharedAccessKey=hc-name;EntityPath=hc-name," +
      "connection.connectionName=hc-name," +
      "connection.endpoint=https://relay-ns.servicebus.windows.net," +
      "connection.targetHost=http://wds-rl-rls-wds-svc:8080," +
      "sam.url=https://sam.test.org:443," +
      "sam.resourceId=sam-id," +
      "sam.resourceType=kubernetes-app-shared," +
      "sam.action=connect," +
      "leonardo.url=https://leo.com," +
      s"general.workspaceId=${workspaceId.value.toString}," +
      "general.appName=app1," +
      "listener.image=acr/listener:1," +
      "connection.validHosts[0]=example.com," +
      "connection.validHosts[1]=foo.com," +
      "connection.validHosts[2]=bar.org"
  }
}
