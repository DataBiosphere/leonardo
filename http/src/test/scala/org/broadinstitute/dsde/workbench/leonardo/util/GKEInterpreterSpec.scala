package org.broadinstitute.dsde.workbench.leonardo
package util

import cats.effect.IO
import cats.mtl.Ask
import com.google.container.v1.Operation
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleIamDAO
import org.broadinstitute.dsde.workbench.google2.KubernetesModels.{KubernetesPodStatus, PodStatus}
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName._
import org.broadinstitute.dsde.workbench.google2.mock._
import org.broadinstitute.dsde.workbench.google2.{DiskName, GKEModels, KubernetesClusterNotFoundException}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.{makeApp, makeKubeCluster, makeNodepool}
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao.{MockAppDAO, MockAppDescriptorDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.dummyDate
import org.broadinstitute.dsde.workbench.leonardo.db.{
  kubernetesClusterQuery,
  nodepoolQuery,
  KubernetesServiceDbQueries,
  TestComponent
}
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.broadinstitute.dsde.workbench.leonardo.http.service.AppNotFoundException
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsp.ChartVersion
import org.broadinstitute.dsp.mocks._
import org.scalatest.flatspec.AnyFlatSpecLike

import java.nio.file.Files
import java.util.Base64
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.jdk.CollectionConverters._

class GKEInterpreterSpec extends AnyFlatSpecLike with TestComponent with LeonardoTestSuite {
  val googleIamDao = new MockGoogleIamDAO {
    override def addIamPolicyBindingOnServiceAccount(serviceAccountProject: GoogleProject,
                                                     serviceAccountEmail: WorkbenchEmail,
                                                     memberEmail: WorkbenchEmail,
                                                     rolesToAdd: Set[String]
    ): Future[Unit] = Future.unit
  }

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

  it should "create a nodepool with autoscaling" in isolatedDbTest {
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
    val chart = Chart.fromString("/leonardo/sas-0.1.0")
    val savedApp1 = makeApp(1, savedNodepool1.id, appType = AppType.Allowed, chart = chart.get)
      .copy(status = AppStatus.Stopping)
      .save()

    val res = for {
      _ <- gkeInterp.startAndPollApp(
        StartAppParams(savedApp1.id, savedApp1.appName, savedCluster1.cloudContext.asInstanceOf[CloudContext.Gcp].value)
      )
      getAppOpt <- KubernetesServiceDbQueries
        .getFullAppById(savedCluster1.cloudContext, savedApp1.id)
        .transaction
      getApp = getAppOpt.get
      appUsageRows <- getAllAppUsage.transaction
    } yield {
      getApp.app.errors.size shouldBe 0
      getApp.app.status shouldBe AppStatus.Running
      getApp.nodepool.status shouldBe NodepoolStatus.Running
      getApp.nodepool.autoscalingEnabled shouldBe true
      getApp.nodepool.numNodes shouldBe NumNodes(2)
      val appUsage = appUsageRows.headOption.get
      appUsage.appId shouldBe (getApp.app.id)
      appUsage.stopTime shouldBe dummyDate
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
                              List(NodepoolLeoId(-1), NodepoolLeoId(-1)),
                              false,
                              false
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
          CreateClusterParams(savedCluster1.id,
                              savedCluster1.cloudContext.asInstanceOf[CloudContext.Gcp].value,
                              List(),
                              false,
                              false
          )
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
                              List(NodepoolLeoId(-2)),
                              false,
                              false
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

  it should "createAndPollApp properly" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).copy(status = KubernetesClusterStatus.Running).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).copy(status = NodepoolStatus.Running).save()
    val disk = makePersistentDisk(Some(DiskName("d1"))).save().unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val chart = Chart.fromString("/leonardo/sas-0.1.0")
    val savedApp1 = makeApp(1,
                            savedNodepool1.id,
                            appType = AppType.Allowed,
                            chart = chart.get,
                            disk = Some(disk),
                            kubernetesServiceAccountName = Some(ServiceAccountName("ksa"))
    )
      .copy(status = AppStatus.Stopping)
      .save()

    val res = for {
      _ <- gkeInterp.createAndPollApp(
        CreateAppParams(savedApp1.id,
                        savedCluster1.cloudContext.asInstanceOf[CloudContext.Gcp].value,
                        savedApp1.appName,
                        None
        )
      )
      getAppOpt <- KubernetesServiceDbQueries
        .getFullAppById(savedCluster1.cloudContext, savedApp1.id)
        .transaction
      getApp = getAppOpt.get
      appUsageRows <- getAllAppUsage.transaction
    } yield {
      getApp.app.errors.size shouldBe 0
      getApp.app.status shouldBe AppStatus.Running
      getApp.nodepool.status shouldBe NodepoolStatus.Running
      getApp.nodepool.autoscalingEnabled shouldBe true
      getApp.nodepool.numNodes shouldBe NumNodes(2)
      val appUsage = appUsageRows.headOption.get
      appUsage.appId shouldBe (getApp.app.id)
      appUsage.stopTime shouldBe dummyDate
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should s"emit an exception if the app is not alive prior to upgrading" in isolatedDbTest {
    val appDao = new MockAppDAO {
      override def isProxyAvailable(googleProject: GoogleProject,
                                    appName: AppName,
                                    serviceName: ServiceName,
                                    traceId: TraceId
      ): IO[Boolean] = IO(false)
    }
    val gkeInterp =
      new GKEInterpreter[IO](
        Config.gkeInterpConfig,
        bucketHelper,
        vpcInterp,
        MockGKEService,
        MockKubernetesService,
        MockHelm,
        appDao,
        credentials,
        googleIamDao,
        MockGoogleDiskService,
        MockAppDescriptorDAO,
        nodepoolLock,
        FakeGoogleResourceService,
        FakeGoogleComputeService
      )

    val savedCluster1 = makeKubeCluster(1).copy(status = KubernetesClusterStatus.Running).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).copy(status = NodepoolStatus.Running).save()
    val disk = makePersistentDisk(Some(DiskName("d1"))).save().unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val chart = Chart.fromString("/leonardo/sas-0.1.0")
    val savedApp1 = makeApp(1,
                            savedNodepool1.id,
                            appType = AppType.Allowed,
                            chart = chart.get,
                            disk = Some(disk),
                            kubernetesServiceAccountName = Some(ServiceAccountName("ksa"))
    ).save()

    val res = for {
      _ <- gkeInterp
        .updateAndPollApp(
          UpdateAppParams(savedApp1.id,
                          savedApp1.appName,
                          ChartVersion("0.0.2"),
                          Some(GoogleProject(savedCluster1.cloudContext.asCloudContextDb.value))
          )
        )
    } yield ()

    val either = res.attempt.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    either.isLeft shouldBe true
    either match {
      case Left(value) =>
        value.getMessage should include("is not live")
        value.getClass shouldBe AppUpdatePollingException("message", None).getClass
      case _ => fail()
    }
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
