package org.broadinstitute.dsde.workbench.leonardo.monitor

import cats.Eq
import cats.effect.IO
import cats.effect.std.Queue
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.google2.mock.FakeGoogleComputeService
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData._
import org.broadinstitute.dsde.workbench.leonardo.dao.{MockSamDAO, MockWsmClientProvider, MockWsmDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterNodepoolAction.{
  CreateClusterAndNodepool,
  CreateNodepool
}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.{
  CreateAppMessage,
  CreateAppV2Message,
  DeleteAppMessage,
  DeleteAppV2Message
}
import org.broadinstitute.dsde.workbench.leonardo.{
  AppMachineType,
  AppStatus,
  AppType,
  CloudContext,
  DiskStatus,
  KubernetesClusterStatus,
  LeoLenses,
  LeonardoTestSuite,
  NodepoolStatus,
  RuntimeStatus
}
import org.scalatest.Assertions
import org.scalatest.flatspec.AnyFlatSpec

import scala.concurrent.ExecutionContext.Implicits.global
class MonitorAtBootSpec extends AnyFlatSpec with TestComponent with LeonardoTestSuite {
  implicit val msgEq: Eq[LeoPubsubMessage] =
    Eq.instance[LeoPubsubMessage]((x, y) =>
      (x, y) match {
        case (xx: LeoPubsubMessage.StopRuntimeMessage, yy: LeoPubsubMessage.StopRuntimeMessage) =>
          xx.copy(traceId = None) == yy.copy(traceId = None)
        case (xx: LeoPubsubMessage.DeleteRuntimeMessage, yy: LeoPubsubMessage.DeleteRuntimeMessage) =>
          xx.copy(traceId = None) == yy.copy(traceId = None)
        case (xx: LeoPubsubMessage.CreateRuntimeMessage, yy: LeoPubsubMessage.CreateRuntimeMessage) =>
          xx.copy(traceId = None) == yy.copy(traceId = None)
        case (xx: LeoPubsubMessage.StartRuntimeMessage, yy: LeoPubsubMessage.StartRuntimeMessage) =>
          xx.copy(traceId = None) == yy.copy(traceId = None)
        case (xx: LeoPubsubMessage.CreateAppMessage, yy: LeoPubsubMessage.CreateAppMessage) =>
          xx.copy(traceId = None) == yy.copy(traceId = None)
        case (xx: LeoPubsubMessage.DeleteAppMessage, yy: LeoPubsubMessage.DeleteAppMessage) =>
          xx.copy(traceId = None) == yy.copy(traceId = None)
        case (xx, yy) =>
          Assertions.fail(s"unexpected messages ${xx}, ${yy}", null)
      }
    )

  // See https://github.com/broadinstitute/workbench-libs/blob/develop/google2/src/test/scala/org/broadinstitute/dsde/workbench/google2/mock/FakeGoogleComputeService.scala#L69
  val defaultFakeAppMachineType = AppMachineType(7, 4)

  it should "recover RuntimeStatus.Stopping properly for gcp apps" in isolatedDbTest {
    val res = for {
      queue <- Queue.bounded[IO, LeoPubsubMessage](10)
      monitorAtBoot = createMonitorAtBoot(queue)
      runtime <- IO(makeCluster(0).copy(status = RuntimeStatus.Stopping).save())
      _ <- monitorAtBoot.process.take(1).compile.drain
      msg <- queue.tryTake
    } yield (msg eqv Some(LeoPubsubMessage.StopRuntimeMessage(runtime.id, None))) shouldBe true
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "recover RuntimeStatus.Deleting properly" in isolatedDbTest {
    val res = for {
      queue <- Queue.bounded[IO, LeoPubsubMessage](10)
      monitorAtBoot = createMonitorAtBoot(queue)
      runtime <- IO(makeCluster(0).copy(status = RuntimeStatus.Deleting).save())
      _ <- monitorAtBoot.process.take(1).compile.drain
      msg <- queue.tryTake
    } yield (msg eqv Some(LeoPubsubMessage.DeleteRuntimeMessage(runtime.id, None, None))) shouldBe true
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "recover RuntimeStatus.Starting properly" in isolatedDbTest {
    val res = for {
      queue <- Queue.bounded[IO, LeoPubsubMessage](10)
      monitorAtBoot = createMonitorAtBoot(queue)
      runtime <- IO(makeCluster(0).copy(status = RuntimeStatus.Starting).save())
      _ <- monitorAtBoot.process.take(1).compile.drain
      msg <- queue.tryTake
    } yield (msg eqv Some(LeoPubsubMessage.StartRuntimeMessage(runtime.id, None))) shouldBe true
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "recover RuntimeStatus.Creating properly" in isolatedDbTest {
    val res = for {
      queue <- Queue.bounded[IO, LeoPubsubMessage](10)
      monitorAtBoot = createMonitorAtBoot(queue)
      runtime <- IO(makeCluster(0).copy(status = RuntimeStatus.Creating).save())
      _ <- monitorAtBoot.process.take(1).compile.drain
      msg <- queue.tryTake
    } yield {
      val runtimeConfigInCreateRuntimeMessage = LeoLenses.runtimeConfigPrism.getOption(defaultDataprocRuntimeConfig).get
      val expectedMessage = LeoPubsubMessage.CreateRuntimeMessage.fromRuntime(
        runtime,
        runtimeConfigInCreateRuntimeMessage,
        None,
        None
      )
      (msg eqv Some(
        expectedMessage
      )) shouldBe true
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "recover AppStatus.Provisioning properly with cluster and nodepool creation in GCP" in isolatedDbTest {
    val res = for {
      queue <- Queue.bounded[IO, LeoPubsubMessage](10)
      monitorAtBoot = createMonitorAtBoot(queue)
      cluster <- IO(makeKubeCluster(1).copy(status = KubernetesClusterStatus.Provisioning).save())
      nodepool <- IO(makeNodepool(2, cluster.id).copy(status = NodepoolStatus.Provisioning).save())
      defaultNodepool = cluster.nodepools.find(_.isDefault).get
      disk <- makePersistentDisk(None).copy(status = DiskStatus.Creating).save()
      app = makeApp(1, nodepool.id).copy(status = AppStatus.Provisioning)
      appWithDisk = LeoLenses.appToDisk.set(Some(disk))(app)
      savedApp <- IO(appWithDisk.save())
      _ <- monitorAtBoot.process.take(1).compile.drain
      msg <- queue.tryTake
    } yield {
      val expected = CreateAppMessage(
        cluster.cloudContext.asInstanceOf[CloudContext.Gcp].value,
        Some(CreateClusterAndNodepool(cluster.id, defaultNodepool.id, nodepool.id)),
        savedApp.id,
        savedApp.appName,
        Some(disk.id),
        customEnvironmentVariables,
        AppType.Galaxy,
        savedApp.appResources.namespace,
        Some(defaultFakeAppMachineType),
        None,
        false,
        None
      )
      (msg eqv Some(expected)) shouldBe true
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "recover AppStatus.Provisioning properly with cluster and nodepool creation in Azure" in isolatedDbTest {
    val res = for {
      queue <- Queue.bounded[IO, LeoPubsubMessage](10)
      monitorAtBoot = createMonitorAtBoot(queue)
      cluster <- IO(makeAzureCluster(1).copy(status = KubernetesClusterStatus.Provisioning).save())
      nodepool <- IO(makeNodepool(2, cluster.id).copy(status = NodepoolStatus.Provisioning).save())
      disk <- makePersistentDisk(None).copy(status = DiskStatus.Creating).save()
      app = makeApp(1, nodepool.id).copy(status = AppStatus.Provisioning)
      appWithDisk = LeoLenses.appToDisk.set(Some(disk))(app)
      savedApp <- IO(appWithDisk.save())
      _ <- monitorAtBoot.process.take(1).compile.drain
      msg <- queue.tryTake
    } yield {
      msg.isDefined shouldBe true
      val createMsg = msg.get.asInstanceOf[CreateAppV2Message]
      createMsg.cloudContext shouldBe cluster.cloudContext
      createMsg.appId shouldBe savedApp.id
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "recover AppStatus.Provisioning properly with nodepool creation in GCP" in isolatedDbTest {
    val res = for {
      queue <- Queue.bounded[IO, LeoPubsubMessage](10)
      monitorAtBoot = createMonitorAtBoot(queue)
      cluster <- IO(makeKubeCluster(1).copy(status = KubernetesClusterStatus.Running).save())
      nodepool <- IO(makeNodepool(2, cluster.id).copy(status = NodepoolStatus.Provisioning).save())
      disk <- makePersistentDisk(None).copy(status = DiskStatus.Creating).save()
      app = makeApp(1, nodepool.id).copy(status = AppStatus.Provisioning)
      appWithDisk = LeoLenses.appToDisk.set(Some(disk))(app)
      savedApp <- IO(appWithDisk.save())
      _ <- monitorAtBoot.process.take(1).compile.drain
      msg <- queue.tryTake
    } yield {
      val expected = CreateAppMessage(
        cluster.cloudContext.asInstanceOf[CloudContext.Gcp].value,
        Some(CreateNodepool(nodepool.id)),
        savedApp.id,
        savedApp.appName,
        Some(disk.id),
        customEnvironmentVariables,
        AppType.Galaxy,
        savedApp.appResources.namespace,
        Some(defaultFakeAppMachineType),
        None,
        false,
        None
      )
      (msg eqv Some(expected)) shouldBe true
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "recover AppStatus.Provisioning properly with nodepool creation in Azure" in isolatedDbTest {
    val res = for {
      queue <- Queue.bounded[IO, LeoPubsubMessage](10)
      monitorAtBoot = createMonitorAtBoot(queue)
      cluster <- IO(makeAzureCluster(1).copy(status = KubernetesClusterStatus.Running).save())
      nodepool <- IO(makeNodepool(2, cluster.id).copy(status = NodepoolStatus.Provisioning).save())
      disk <- makePersistentDisk(None).copy(status = DiskStatus.Creating).save()
      app = makeApp(1, nodepool.id).copy(status = AppStatus.Provisioning)
      appWithDisk = LeoLenses.appToDisk.set(Some(disk))(app)
      savedApp <- IO(appWithDisk.save())
      _ <- monitorAtBoot.process.take(1).compile.drain
      msg <- queue.tryTake
    } yield {
      msg.isDefined shouldBe true
      val createMsg = msg.get.asInstanceOf[CreateAppV2Message]
      createMsg.cloudContext shouldBe cluster.cloudContext
      createMsg.appId shouldBe savedApp.id
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "recover AppStatus.Provisioning properly in GCP" in isolatedDbTest {
    val res = for {
      queue <- Queue.bounded[IO, LeoPubsubMessage](10)
      monitorAtBoot = createMonitorAtBoot(queue)
      cluster <- IO(makeKubeCluster(1).copy(status = KubernetesClusterStatus.Running).save())
      nodepool <- IO(makeNodepool(2, cluster.id).copy(status = NodepoolStatus.Running).save())
      disk <- makePersistentDisk(None).copy(status = DiskStatus.Creating).save()
      app = makeApp(1, nodepool.id).copy(status = AppStatus.Provisioning)
      appWithDisk = LeoLenses.appToDisk.set(Some(disk))(app)
      savedApp <- IO(appWithDisk.save())
      _ <- monitorAtBoot.process.take(1).compile.drain
      msg <- queue.tryTake
    } yield {
      val expected = CreateAppMessage(
        cluster.cloudContext.asInstanceOf[CloudContext.Gcp].value,
        None,
        savedApp.id,
        savedApp.appName,
        Some(disk.id),
        customEnvironmentVariables,
        AppType.Galaxy,
        savedApp.appResources.namespace,
        Some(defaultFakeAppMachineType),
        None,
        false,
        None
      )
      (msg eqv Some(expected)) shouldBe true
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "recover AppStatus.Provisioning properly in Azure" in isolatedDbTest {
    val res = for {
      queue <- Queue.bounded[IO, LeoPubsubMessage](10)
      monitorAtBoot = createMonitorAtBoot(queue)
      cluster <- IO(makeAzureCluster(1).copy(status = KubernetesClusterStatus.Running).save())
      nodepool <- IO(makeNodepool(2, cluster.id).copy(status = NodepoolStatus.Running).save())
      disk <- makePersistentDisk(None).copy(status = DiskStatus.Creating).save()
      app = makeApp(1, nodepool.id).copy(status = AppStatus.Provisioning)
      appWithDisk = LeoLenses.appToDisk.set(Some(disk))(app)
      savedApp <- IO(appWithDisk.save())
      _ <- monitorAtBoot.process.take(1).compile.drain
      msg <- queue.tryTake
    } yield {
      msg.isDefined shouldBe true
      val createMsg = msg.get.asInstanceOf[CreateAppV2Message]
      createMsg.cloudContext shouldBe cluster.cloudContext
      createMsg.appId shouldBe savedApp.id
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "recover AppStatus.Deleting properly in GCP" in isolatedDbTest {
    val res = for {
      queue <- Queue.bounded[IO, LeoPubsubMessage](10)
      monitorAtBoot = createMonitorAtBoot(queue)
      cluster <- IO(makeKubeCluster(1).copy(status = KubernetesClusterStatus.Running).save())
      nodepool <- IO(makeNodepool(2, cluster.id).copy(status = NodepoolStatus.Deleting).save())
      disk <- makePersistentDisk(None).copy(status = DiskStatus.Ready).save()
      app = makeApp(1, nodepool.id).copy(status = AppStatus.Deleting)
      appWithDisk = LeoLenses.appToDisk.set(Some(disk))(app)
      savedApp <- IO(appWithDisk.save())
      _ <- monitorAtBoot.process.take(1).compile.drain
      msg <- queue.tryTake
    } yield {
      val expected = DeleteAppMessage(
        savedApp.id,
        savedApp.appName,
        cluster.cloudContext.asInstanceOf[CloudContext.Gcp].value,
        None,
        None
      )
      (msg eqv Some(expected)) shouldBe true
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "recover AppStatus.Deleting properly in Azure" in isolatedDbTest {
    val res = for {
      queue <- Queue.bounded[IO, LeoPubsubMessage](10)
      monitorAtBoot = createMonitorAtBoot(queue)
      cluster <- IO(makeAzureCluster(1).copy(status = KubernetesClusterStatus.Running).save())
      nodepool <- IO(makeNodepool(2, cluster.id).copy(status = NodepoolStatus.Deleting).save())
      disk <- makePersistentDisk(None).copy(status = DiskStatus.Ready).save()
      app = makeApp(1, nodepool.id).copy(status = AppStatus.Deleting)
      appWithDisk = LeoLenses.appToDisk.set(Some(disk))(app)
      savedApp <- IO(appWithDisk.save())
      _ <- monitorAtBoot.process.take(1).compile.drain
      msg <- queue.tryTake
    } yield {
      msg.isDefined shouldBe true
      val createMsg = msg.get.asInstanceOf[DeleteAppV2Message]
      createMsg.cloudContext shouldBe cluster.cloudContext
      createMsg.appId shouldBe savedApp.id
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "ignore non-monitored apps in GCP" in isolatedDbTest {
    val res = for {
      queue <- Queue.bounded[IO, LeoPubsubMessage](10)
      monitorAtBoot = createMonitorAtBoot(queue)
      cluster <- IO(makeKubeCluster(1).copy(status = KubernetesClusterStatus.Running).save())
      nodepool <- IO(makeNodepool(2, cluster.id).copy(status = NodepoolStatus.Running).save())
      disk <- makePersistentDisk(None).copy(status = DiskStatus.Ready).save()
      app = makeApp(1, nodepool.id).copy(status = AppStatus.Running)
      appWithDisk = LeoLenses.appToDisk.set(Some(disk))(app)
      _ <- IO(appWithDisk.save())
      _ <- monitorAtBoot.process.take(1).compile.drain
      msg <- queue.tryTake
    } yield msg shouldBe None
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "ignore non-monitored apps in Azure" in isolatedDbTest {
    val res = for {
      queue <- Queue.bounded[IO, LeoPubsubMessage](10)
      monitorAtBoot = createMonitorAtBoot(queue)
      cluster <- IO(makeAzureCluster(1).copy(status = KubernetesClusterStatus.Running).save())
      nodepool <- IO(makeNodepool(2, cluster.id).copy(status = NodepoolStatus.Running).save())
      disk <- makePersistentDisk(None).copy(status = DiskStatus.Ready).save()
      app = makeApp(1, nodepool.id).copy(status = AppStatus.Running)
      appWithDisk = LeoLenses.appToDisk.set(Some(disk))(app)
      _ <- IO(appWithDisk.save())
      _ <- monitorAtBoot.process.take(1).compile.drain
      msg <- queue.tryTake
    } yield msg shouldBe None
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  def createMonitorAtBoot(
    queue: Queue[IO, LeoPubsubMessage] =
      Queue.bounded[IO, LeoPubsubMessage](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  ): MonitorAtBoot[IO] =
    new MonitorAtBoot[IO](queue, Some(FakeGoogleComputeService), new MockSamDAO(), new MockWsmClientProvider())
}
