package org.broadinstitute.dsde.workbench.leonardo.util

import java.nio.file.Files
import java.util.Base64

import cats.effect.IO
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleProjectDAO
import org.broadinstitute.dsde.workbench.google2.GKEModels.NodepoolName
import org.broadinstitute.dsde.workbench.google2.KubernetesModels.{KubernetesPodStatus, PodStatus}
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{
  NamespaceName,
  PodName,
  SecretKey,
  SecretName
}
import org.broadinstitute.dsde.workbench.google2.mock.{
  FakeGoogleComputeService,
  MockComputePollOperation,
  MockGKEService,
  MockKubernetesService
}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.{makeApp, makeKubeCluster, makeNodepool}
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao.MockGalaxyDAO
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.broadinstitute.dsde.workbench.leonardo.{
  AppName,
  AutoscalingConfig,
  AutoscalingMax,
  AutoscalingMin,
  KubernetesClusterLeoId,
  KubernetesServiceAccount,
  LeonardoTestSuite
}
import org.broadinstitute.dsp.Release
import org.broadinstitute.dsp.mocks._
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global

class GKEInterpreterSpec extends AnyFlatSpecLike with TestComponent with LeonardoTestSuite {

  val projectDAO = new MockGoogleProjectDAO

  val vpcInterp =
    new VPCInterpreter[IO](Config.vpcInterpreterConfig,
                           projectDAO,
                           FakeGoogleComputeService,
                           new MockComputePollOperation)

  val gkeInterp =
    new GKEInterpreter[IO](Config.gkeInterpConfig,
                           vpcInterp,
                           MockGKEService,
                           MockKubernetesService,
                           MockHelm,
                           MockGalaxyDAO,
                           credentials,
                           blocker)

  "GKEInterpreter" should "create a nodepool with autoscaling" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val minNodes = 0
    val maxNodes = 2
    val savedNodepool1 = makeNodepool(1, savedCluster1.id)
      .copy(autoscalingEnabled = true,
            autoscalingConfig = Some(AutoscalingConfig(AutoscalingMin(minNodes), AutoscalingMax(maxNodes))))
      .save()

    val googleNodepool = gkeInterp.buildGoogleNodepool(savedNodepool1)
    googleNodepool.getAutoscaling.getEnabled shouldBe true
    googleNodepool.getAutoscaling.getMinNodeCount shouldBe minNodes
    googleNodepool.getAutoscaling.getMaxNodeCount shouldBe maxNodes
  }

  it should "create secrets properly" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val savedApp1 = makeApp(1, savedNodepool1.id).save()

    val secrets = gkeInterp.getSecrets(savedApp1.appResources.namespace.name).unsafeRunSync()

    //we don't check the byte arrays here for the files
    secrets.size shouldBe 2
    secrets.map(_.secrets.keys.size).sum shouldBe 3
    secrets.flatMap(_.secrets.keys).sortBy(_.value) shouldBe List(SecretKey("ca.crt"),
                                                                  SecretKey("tls.crt"),
                                                                  SecretKey("tls.key")).sortBy(_.value)
    val emptyFileSecrets = secrets.map(s => (s.name, s.namespaceName))
    emptyFileSecrets should contain((SecretName("ca-secret"), savedApp1.appResources.namespace.name))
    emptyFileSecrets should contain((SecretName("tls-secret"), savedApp1.appResources.namespace.name))
    secrets.flatMap(_.secrets.values).map(s => s.isEmpty shouldBe false)
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
      gkeInterp.getHelmAuthContext(googleCluster, KubernetesClusterLeoId(1), NamespaceName("ns")).unsafeRunSync()

    authContext.namespace.asString shouldBe "ns"
    authContext.kubeApiServer.asString shouldBe "https://1.2.3.4"
    authContext.kubeToken.asString shouldBe "accessToken"
    Files.exists(authContext.caCertFile.path) shouldBe true
    Files.readAllLines(authContext.caCertFile.path).asScala.mkString shouldBe "ca_cert"
  }

  it should "build Galaxy override values string" in {
    val savedCluster1 = makeKubeCluster(1).save()
    val result = gkeInterp.buildGalaxyChartOverrideValuesString(
      AppName("app1"),
      Release("app1-galaxy-rls"),
      savedCluster1,
      NodepoolName("pool1"),
      userEmail,
      Map("WORKSPACE_NAME" -> "test-workspace", "WORKSPACE_BUCKET" -> "gs://test-bucket"),
      KubernetesServiceAccount("app1-galaxy-ksa")
    )

    result shouldBe """nfs.storageClass.name=nfs-app1-galaxy-rls,cvmfs.repositories.cvmfs-gxy-data-app1-galaxy-rls=data.galaxyproject.org,cvmfs.repositories.cvmfs-gxy-main-app1-galaxy-rls=main.galaxyproject.org,cvmfs.cache.alienCache.storageClass=nfs-app1-galaxy-rls,galaxy.persistence.storageClass=nfs-app1-galaxy-rls,galaxy.cvmfs.data.pvc.storageClassName=cvmfs-gxy-data-app1-galaxy-rls,galaxy.cvmfs.main.pvc.storageClassName=cvmfs-gxy-main-app1-galaxy-rls,galaxy.nodeSelector.cloud\.google\.com/gke-nodepool=pool1,nfs.nodeSelector.cloud\.google\.com/gke-nodepool=pool1,galaxy.ingress.path=/proxy/google/v1/apps/dsp-leo-test1/app1/galaxy,galaxy.ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-from=https://1211904326.jupyter.firecloud.org,galaxy.ingress.annotations.nginx\.ingress\.kubernetes\.io/proxy-redirect-to=https://leo,galaxy.ingress.hosts[0]=1211904326.jupyter.firecloud.org,galaxy.configs.galaxy\.yml.galaxy.single_user=user1@example.com,galaxy.configs.galaxy\.yml.galaxy.admin_users=user1@example.com,rbac.serviceAccount=app1-galaxy-ksa,persistence={},configs.WORKSPACE_NAME=test-workspace,extraEnv[0].name=WORKSPACE_NAME,extraEnv[0].valueFrom.configMapKeyRef.name=app1-galaxy-rls-galaxykubeman-configs,extraEnv[0].valueFrom.configMapKeyRef.key=WORKSPACE_NAME,configs.WORKSPACE_BUCKET=gs://test-bucket,extraEnv[1].name=WORKSPACE_BUCKET,extraEnv[1].valueFrom.configMapKeyRef.name=app1-galaxy-rls-galaxykubeman-configs,extraEnv[1].valueFrom.configMapKeyRef.key=WORKSPACE_BUCKET"""
  }

  it should "check if a pod is done" in {
    gkeInterp.isPodDone(KubernetesPodStatus(PodName("pod1"), PodStatus.Succeeded)) shouldBe true
    gkeInterp.isPodDone(KubernetesPodStatus(PodName("pod2"), PodStatus.Pending)) shouldBe false
    gkeInterp.isPodDone(KubernetesPodStatus(PodName("pod3"), PodStatus.Failed)) shouldBe true
    gkeInterp.isPodDone(KubernetesPodStatus(PodName("pod4"), PodStatus.Unknown)) shouldBe false
    gkeInterp.isPodDone(KubernetesPodStatus(PodName("pod5"), PodStatus.Running)) shouldBe false
  }
}
