package org.broadinstitute.dsde.workbench.leonardo.util

import cats.effect.IO
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleProjectDAO
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{SecretKey, SecretName}
import org.broadinstitute.dsde.workbench.google2.mock.{
  FakeGoogleComputeService,
  MockComputePollOperation,
  MockGKEService,
  MockKubernetesService
}
import org.broadinstitute.dsde.workbench.leonardo.{AutoscalingConfig, AutoscalingMax, AutoscalingMin, LeonardoTestSuite}
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.{makeApp, makeKubeCluster, makeNodepool}
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.concurrent.ExecutionContext.Implicits.global

class GKEInterpreterSpec extends AnyFlatSpecLike with TestComponent with LeonardoTestSuite {

  val projectDAO = new MockGoogleProjectDAO

  val vpcInterp =
    new VPCInterpreter[IO](Config.vpcInterpreterConfig,
                           projectDAO,
                           FakeGoogleComputeService,
                           new MockComputePollOperation)

  val gkeInterp =
    new GKEInterpreter[IO](Config.gkeInterpConfig, vpcInterp, MockGKEService, MockKubernetesService, blocker)

  it should "create a nodepool with autoscaling" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val minNodes = 0
    val maxNodes = 2
    val savedNodepool1 = makeNodepool(1, savedCluster1.id)
      .copy(autoscalingEnabled = true,
            autoscalingConfig = Some(AutoscalingConfig(AutoscalingMin(minNodes), AutoscalingMax(maxNodes))))
      .save()

    val googleNodepool = gkeInterp.getGoogleNodepool(savedNodepool1)
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
    secrets.flatMap(_.secrets.keys).sortBy(_.value) shouldBe List(SecretKey("ca-crt"),
                                                                  SecretKey("tls-crt"),
                                                                  SecretKey("tls-key")).sortBy(_.value)
    val emptyFileSecrets = secrets.map(s => (s.name, s.namespaceName))
    emptyFileSecrets should contain((SecretName("ca-secret"), savedApp1.appResources.namespace.name))
    emptyFileSecrets should contain((SecretName("tls-secret"), savedApp1.appResources.namespace.name))
    secrets.flatMap(_.secrets.values).map(s => s.isEmpty shouldBe false)
  }
}
