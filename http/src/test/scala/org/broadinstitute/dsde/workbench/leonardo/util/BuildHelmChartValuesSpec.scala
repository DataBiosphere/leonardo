package org.broadinstitute.dsde.workbench.leonardo
package util

import org.broadinstitute.dsde.workbench.google2.DiskName
import org.broadinstitute.dsde.workbench.google2.GKEModels.NodepoolName
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{NamespaceName, ServiceAccountName}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.{makePersistentDisk, userEmail, userEmail2}
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.{makeCustomAppService, makeKubeCluster}
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.util.BuildHelmChartValues._
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.Release
import org.scalatest.flatspec.AnyFlatSpecLike

class BuildHelmChartValuesSpec extends AnyFlatSpecLike with LeonardoTestSuite {

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
      """config.gcsRegion=us-central1,""" +
      """config.backend=replace-me,""" +
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
      // [IA-4997] to support CHIPS by setting partitioned cookies
      // """ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-cookie-path=/ "/; Secure; SameSite=None; HttpOnly; Partitioned",""" +
      """ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-cookie-path=/ "/; Secure; SameSite=None; HttpOnly",""" +
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
      None
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
      // [IA-4997] to support CHIPS by setting partitioned cookies
      // """ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-cookie-path=/ "/; Secure; SameSite=None; HttpOnly; Partitioned",""" +
      """ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-cookie-path=/ "/; Secure; SameSite=None; HttpOnly",""" +
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
      """gcsfuse.enabled=false"""
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
      // [IA-4997] to support CHIPS by setting partitioned cookies
      // """ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-cookie-path=/ "/; Secure; SameSite=None; HttpOnly; Partitioned",""" +
      """ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-cookie-path=/ "/; Secure; SameSite=None; HttpOnly",""" +
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
      """tolerations.enabled=true,""" +
      s"""tolerations.keyValue=${BuildHelmChartValues.getNodeSelectorGroupValue(userEmail2)},""" +
      s"""nodeSelector.group=${BuildHelmChartValues.getNodeSelectorGroupValue(userEmail2)},""" +
      """autopilot.enabled=true,autopilot.app.cpu=500m,""" +
      """autopilot.app.memory=1Gi,autopilot.app.ephemeral\-storage=2Gi,""" +
      """autopilot.welder.cpu=500m,autopilot.welder.memory=3Gi,""" +
      """autopilot.welder.ephemeral\-storage=1Gi,""" +
      """autopilot.wondershaper.cpu=500m,""" +
      """autopilot.wondershaper.memory=3Gi,""" +
      """autopilot.wondershaper.ephemeral\-storage=1Gi,""" +
      """gcsfuse.enabled=true,""" +
      """gcsfuse.bucket=fc-bucket""".stripMargin
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
      // [IA-4997] to support CHIPS by setting partitioned cookies
      // """ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-cookie-path=/ "/; Secure; SameSite=None; HttpOnly; Partitioned",""" +
      """ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-cookie-path=/ "/; Secure; SameSite=None; HttpOnly",""" +
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
      """tolerations.enabled=true,""" +
      s"""tolerations.keyValue=${BuildHelmChartValues.getNodeSelectorGroupValue(userEmail2)},""" +
      s"""nodeSelector.group=${BuildHelmChartValues.getNodeSelectorGroupValue(userEmail2)},""" +
      """autopilot.enabled=true,autopilot.app.cpu=500m,""" +
      """autopilot.app.memory=1Gi,autopilot.app.ephemeral\-storage=2Gi,""" +
      """autopilot.welder.cpu=500m,autopilot.welder.memory=3Gi,""" +
      """autopilot.welder.ephemeral\-storage=1Gi,""" +
      """autopilot.wondershaper.cpu=500m,""" +
      """autopilot.wondershaper.memory=3Gi,""" +
      """autopilot.wondershaper.ephemeral\-storage=1Gi,""" +
      """gcsfuse.enabled=true,""" +
      """gcsfuse.bucket=fc-bucket""".stripMargin
  }
}
