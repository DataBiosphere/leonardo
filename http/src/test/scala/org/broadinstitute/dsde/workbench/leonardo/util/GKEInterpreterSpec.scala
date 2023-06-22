package org.broadinstitute.dsde.workbench.leonardo
package util

import cats.effect.IO
import cats.mtl.Ask
import com.google.container.v1.Operation
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleIamDAO
import org.broadinstitute.dsde.workbench.google2.GKEModels.NodepoolName
import org.broadinstitute.dsde.workbench.google2.KubernetesModels.{KubernetesPodStatus, PodStatus}
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName._
import org.broadinstitute.dsde.workbench.google2.mock._
import org.broadinstitute.dsde.workbench.google2.{DiskName, GKEModels, KubernetesClusterNotFoundException}
import org.broadinstitute.dsde.workbench.leonardo.AppRestore.GalaxyRestore
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.{
  makeApp,
  makeCustomAppService,
  makeKubeCluster,
  makeNodepool
}
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao.{MockAppDAO, MockAppDescriptorDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.{
  kubernetesClusterQuery,
  nodepoolQuery,
  KubernetesServiceDbQueries,
  TestComponent
}
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.broadinstitute.dsde.workbench.leonardo.http.service.AppNotFoundException
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsp.Release
import org.broadinstitute.dsp.mocks._
import org.scalatest.flatspec.AnyFlatSpecLike

import java.nio.file.Files
import java.util.Base64
import scala.concurrent.ExecutionContext.Implicits.global
import scala.jdk.CollectionConverters._

class GKEInterpreterSpec extends AnyFlatSpecLike with TestComponent with LeonardoTestSuite {
  val googleIamDao = new MockGoogleIamDAO

  val vpcInterp =
    new VPCInterpreter[IO](Config.vpcInterpreterConfig, FakeGoogleResourceService, FakeGoogleComputeService)

  val bucketHelperConfig =
    BucketHelperConfig(imageConfig, welderConfig, proxyConfig, clusterFilesConfig)

  val bucketHelper =
    new BucketHelper[IO](bucketHelperConfig, FakeGoogleStorageService, serviceAccountProvider)

  val gkeInterp =
    new GKEInterpreter[IO](
      Config.gkeInterpConfig,
      bucketHelper,
      vpcInterp,
      MockGKEService,
      MockKubernetesService,
      MockHelm,
      MockAppDAO,
      credentials,
      googleIamDao,
      MockGoogleDiskService,
      MockAppDescriptorDAO,
      nodepoolLock,
      FakeGoogleResourceService,
      FakeGoogleComputeService
    )

  "GKEInterpreter" should "create a nodepool with autoscaling" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val minNodes = 0
    val maxNodes = 2
    val savedNodepool1 = makeNodepool(1, savedCluster1.id)
      .copy(autoscalingEnabled = true,
            autoscalingConfig = Some(AutoscalingConfig(AutoscalingMin(minNodes), AutoscalingMax(maxNodes)))
      )
      .save()
    val googleNodepool =
      gkeInterp.buildGoogleNodepool(savedNodepool1,
                                    savedCluster1.cloudContext.asInstanceOf[CloudContext.Gcp].value,
                                    Some(Map("gke-default-sa" -> "gke-node-default-sa"))
      )
    googleNodepool.getAutoscaling.getEnabled shouldBe true
    googleNodepool.getAutoscaling.getMinNodeCount shouldBe minNodes
    googleNodepool.getAutoscaling.getMaxNodeCount shouldBe maxNodes
    googleNodepool.getConfig.getServiceAccount shouldBe s"gke-node-default-sa@${savedCluster1.cloudContext.asString}.iam.gserviceaccount.com"
  }

  it should "get a helm auth context" in {
    val googleCluster = com.google.container.v1.Cluster
      .newBuilder()
      .setEndpoint("1.2.3.4")
      .setMasterAuth(
        com.google.container.v1.MasterAuth
          .newBuilder()
          .setClusterCaCertificate(Base64.getEncoder.encodeToString("ca_cert".getBytes()))
      )
      .build

    val authContext =
      gkeInterp
        .getHelmAuthContext(googleCluster, makeKubeCluster(1), NamespaceName("ns"))
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    authContext.namespace.asString shouldBe "ns"
    authContext.kubeApiServer.asString shouldBe "https://1.2.3.4"
    authContext.kubeToken.asString shouldBe "accessToken"
    Files.exists(authContext.caCertFile.path) shouldBe true
    Files.readAllLines(authContext.caCertFile.path).asScala.mkString shouldBe "ca_cert"
  }

  it should "build Galaxy override values string" in {
    val savedCluster1 = makeKubeCluster(1)
    val savedDisk1 = makePersistentDisk(Some(DiskName("disk1")), Some(FormattedBy.Galaxy))
    val res = gkeInterp.buildGalaxyChartOverrideValuesString(
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
    ) shouldBe """nfs.storageClass.name=nfs-app1-galaxy-rls,""" +
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
      gkeInterp.buildGalaxyChartOverrideValuesString(
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
    ) shouldBe """nfs.storageClass.name=nfs-app1-galaxy-rls,""" +
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
    val res = gkeInterp.buildCromwellAppChartOverrideValuesString(
      appName = AppName("app1"),
      cluster = savedCluster1,
      nodepoolName = NodepoolName("pool1"),
      namespaceName = NamespaceName("ns"),
      disk = savedDisk1,
      ksaName = ServiceAccountName("app1-cromwell-ksa"),
      gsa = WorkbenchEmail("pet123-abc@terra-test-abc.iam.gserviceaccount.com"),
      customEnvironmentVariables = envVariables
    )

    res.mkString(",") shouldBe
      """nodeSelector.cloud\.google\.com/gke-nodepool=pool1,""" +
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
      """db.password=replace-me"""
  }

  it should "build Custom App override values string" in {
    val savedCluster1 = makeKubeCluster(1)
    val customService = makeCustomAppService();
    val savedDisk1 = makePersistentDisk(Some(DiskName("disk1")))
    val envVariables = Map("WORKSPACE_BUCKET" -> "gs://test-bucket")
    val res = gkeInterp.buildCustomChartOverrideValuesString(
      appName = AppName("app1"),
      release = Release("app1-custom-rls"),
      nodepoolName = NodepoolName("pool1"),
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
      """nodeSelector.cloud\.google\.com/gke-nodepool=pool1,""" +
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
      """ingress.hosts[0].paths[0]=/proxy/google/v1/apps/dsp-leo-test1/app1/custom-service(/|$)(.*)"""
  }

  it should "build RStudio override values string" in {
    val savedCluster1 = makeKubeCluster(1)
    val savedDisk1 = makePersistentDisk(Some(DiskName("disk1")))
    val envVariables = Map("WORKSPACE_NAME" -> "test-workspace-name")
    val res = gkeInterp.buildRStudioAppChartOverrideValuesString(
      appName = AppName("app1"),
      cluster = savedCluster1,
      nodepoolName = NodepoolName("pool1"),
      namespaceName = NamespaceName("ns"),
      disk = savedDisk1,
      ksaName = ServiceAccountName("app1-rstudio-ksa"),
      userEmail = userEmail2,
      stagingBucket = GcsBucketName("test-staging-bucket"),
      envVariables
    )

    res.mkString(",") shouldBe
      """nodeSelector.cloud\.google\.com/gke-nodepool=pool1,""" +
      """persistence.size=250G,""" +
      """persistence.gcePersistentDisk=disk1,""" +
      """serviceAccount.name=app1-rstudio-ksa,""" +
      """ingress.enabled=true,""" +
      """ingress.annotations.nginx\.ingress\.kubernetes\.io/auth-tls-secret=ns/ca-secret,""" +
      """ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-from=https://1455694897.jupyter.firecloud.org,""" +
      """ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-to=https://leo/proxy/google/v1/apps/dsp-leo-test1/app1/rstudio-service,""" +
      """ingress.annotations.nginx\.ingress\.kubernetes\.io/rewrite-target=/$2,""" +
      """ingress.host=1455694897.jupyter.firecloud.org,""" +
      """ingress.rstudio.path=/proxy/google/v1/apps/dsp-leo-test1/app1/rstudio-service(/|$)(.*),""" +
      """ingress.welder.path=/proxy/google/v1/apps/dsp-leo-test1/app1/welder-service(/|$)(.*),""" +
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
      """extraEnv[0].name=WORKSPACE_NAME,""" +
      """extraEnv[0].value=test-workspace-name"""

  }

  it should "check if a pod is done" in {
    gkeInterp.isPodDone(KubernetesPodStatus(PodName("pod1"), PodStatus.Succeeded)) shouldBe true
    gkeInterp.isPodDone(KubernetesPodStatus(PodName("pod2"), PodStatus.Pending)) shouldBe false
    gkeInterp.isPodDone(KubernetesPodStatus(PodName("pod3"), PodStatus.Failed)) shouldBe true
    gkeInterp.isPodDone(KubernetesPodStatus(PodName("pod4"), PodStatus.Unknown)) shouldBe false
    gkeInterp.isPodDone(KubernetesPodStatus(PodName("pod5"), PodStatus.Running)) shouldBe false
  }

  it should "deleteAndPollCluster properly" in isolatedDbTest {
    val res = for {
      savedCluster <- IO(makeKubeCluster(1).save())
      m = DeleteClusterParams(
        savedCluster.id,
        savedCluster.cloudContext.asInstanceOf[CloudContext.Gcp].value
      )
      _ <- gkeInterp.deleteAndPollCluster(m)
      clusterOpt <- kubernetesClusterQuery.getMinimalClusterById(savedCluster.id).transaction
    } yield clusterOpt.get.status shouldBe KubernetesClusterStatus.Deleted

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "deleteAndPollNodepool properly" in isolatedDbTest {
    val res = for {
      savedCluster <- IO(makeKubeCluster(1).save())
      savedNodepool <- IO(makeNodepool(1, savedCluster.id).save())
      m = DeleteNodepoolParams(savedNodepool.id, savedCluster.cloudContext.asInstanceOf[CloudContext.Gcp].value)
      _ <- gkeInterp.deleteAndPollNodepool(m)
      nodepoolOpt <- nodepoolQuery.getMinimalById(savedNodepool.id).transaction
    } yield nodepoolOpt.get.status shouldBe NodepoolStatus.Deleted

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "mark a nodepool as Deleted in DB when it doesn't exist in Google" in isolatedDbTest {
    val mockGKEService = new MockGKEService {
      override def deleteNodepool(nodepoolId: GKEModels.NodepoolId)(implicit
        ev: Ask[IO, TraceId]
      ): IO[Option[Operation]] = IO.pure(None)
    }

    val gkeInterpDelete =
      new GKEInterpreter[IO](
        Config.gkeInterpConfig,
        bucketHelper,
        vpcInterp,
        mockGKEService,
        MockKubernetesService,
        MockHelm,
        MockAppDAO,
        credentials,
        googleIamDao,
        MockGoogleDiskService,
        MockAppDescriptorDAO,
        nodepoolLock,
        FakeGoogleResourceService,
        FakeGoogleComputeService
      )

    val res = for {
      savedCluster <- IO(makeKubeCluster(1).save())
      savedNodepool <- IO(makeNodepool(1, savedCluster.id).save())
      m = DeleteNodepoolParams(savedNodepool.id, savedCluster.cloudContext.asInstanceOf[CloudContext.Gcp].value)
      _ <- gkeInterpDelete.deleteAndPollNodepool(m)
      nodepoolOpt <- nodepoolQuery.getMinimalById(savedNodepool.id).transaction
    } yield nodepoolOpt.get.status shouldBe NodepoolStatus.Deleted

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "stopAndPollApp properly" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).copy(status = KubernetesClusterStatus.Running).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).copy(status = NodepoolStatus.Running).save()
    val savedApp1 = makeApp(1, savedNodepool1.id).copy(status = AppStatus.Stopping).save()

    val res = for {
      _ <- gkeInterp.stopAndPollApp(
        StopAppParams(savedApp1.id, savedApp1.appName, savedCluster1.cloudContext.asInstanceOf[CloudContext.Gcp].value)
      )
      getAppOpt <- KubernetesServiceDbQueries
        .getFullAppById(savedCluster1.cloudContext, savedApp1.id)
        .transaction
      getApp = getAppOpt.get
    } yield {
      getApp.app.errors.size shouldBe 0
      getApp.app.status shouldBe AppStatus.Stopped
      getApp.nodepool.status shouldBe NodepoolStatus.Running
      getApp.nodepool.autoscalingEnabled shouldBe true
      getApp.nodepool.numNodes shouldBe NumNodes(2)
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "startAndPollApp properly" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).copy(status = KubernetesClusterStatus.Running).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).copy(status = NodepoolStatus.Running).save()
    val savedApp1 = makeApp(1, savedNodepool1.id).copy(status = AppStatus.Stopping).save()

    val res = for {
      _ <- gkeInterp.startAndPollApp(
        StartAppParams(savedApp1.id, savedApp1.appName, savedCluster1.cloudContext.asInstanceOf[CloudContext.Gcp].value)
      )
      getAppOpt <- KubernetesServiceDbQueries
        .getFullAppById(savedCluster1.cloudContext, savedApp1.id)
        .transaction
      getApp = getAppOpt.get
    } yield {
      getApp.app.errors.size shouldBe 0
      getApp.app.status shouldBe AppStatus.Running
      getApp.nodepool.status shouldBe NodepoolStatus.Running
      getApp.nodepool.autoscalingEnabled shouldBe true
      getApp.nodepool.numNodes shouldBe NumNodes(2)
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "start custom APP without descriptor will fail" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).copy(status = KubernetesClusterStatus.Running).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).copy(status = NodepoolStatus.Running).save()
    val savedApp1 = makeApp(1, savedNodepool1.id, appType = AppType.Custom).copy(status = AppStatus.Stopping).save()

    val res = for {
      result <- gkeInterp
        .startAndPollApp(
          StartAppParams(savedApp1.id,
                         savedApp1.appName,
                         savedCluster1.cloudContext.asInstanceOf[CloudContext.Gcp].value
          )
        )
        .attempt
    } yield result shouldBe Left(AppRequiresDescriptorException(savedApp1.id))

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "error during createCluster if cluster doesn't exist in database" in isolatedDbTest {
    val res = for {
      ctx <- appContext.ask[AppContext]
      r <- gkeInterp
        .createCluster(
          CreateClusterParams(KubernetesClusterLeoId(-1),
                              GoogleProject("fake"),
                              List(NodepoolLeoId(-1), NodepoolLeoId(-1))
          )
        )
        .attempt
    } yield r shouldBe (Left(
      KubernetesClusterNotFoundException(
        s"Failed kubernetes cluster creation. Cluster with id -1 not found in database | trace id: ${ctx.traceId}"
      )
    ))
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "error on pollCluster if default nodepool doesn't exist" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1, false).saveWithOutDefaultNodepool()
    val res = for {
      createResult <- gkeInterp
        .createCluster(
          CreateClusterParams(savedCluster1.id, savedCluster1.cloudContext.asInstanceOf[CloudContext.Gcp].value, List())
        )
      r <- gkeInterp
        .pollCluster(
          PollClusterParams(savedCluster1.id,
                            savedCluster1.cloudContext.asInstanceOf[CloudContext.Gcp].value,
                            createResult.get
          )
        )
        .attempt
    } yield r shouldBe (Left(
      DefaultNodepoolNotFoundException(savedCluster1.id)
    ))
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "error on createCluster if user nodepool doesn't exist" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1, false).save()
    val res = for {
      ctx <- appContext.ask[AppContext]

      r <- gkeInterp
        .createCluster(
          CreateClusterParams(savedCluster1.id,
                              savedCluster1.cloudContext.asInstanceOf[CloudContext.Gcp].value,
                              List(NodepoolLeoId(-2))
          )
        )
        .attempt
    } yield r shouldBe (Left(
      org.broadinstitute.dsde.workbench.leonardo.util.ClusterCreationException(
        ctx.traceId,
        s"CreateCluster was called with nodepools that are not present in the database for cluster ${savedCluster1.getClusterId.toString}"
      )
    ))
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "error on createAndPollApp if app doesn't exist" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1, false).save()
    val res = for {
      ctx <- appContext.ask[AppContext]

      r <- gkeInterp
        .createAndPollApp(
          CreateAppParams(AppId(-1),
                          savedCluster1.cloudContext.asInstanceOf[CloudContext.Gcp].value,
                          AppName("non-existent"),
                          None
          )
        )
        .attempt
    } yield r shouldBe (Left(
      AppNotFoundException(
        savedCluster1.cloudContext,
        AppName("non-existent"),
        ctx.traceId,
        "No active app found in DB"
      )
    ))
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }
}
