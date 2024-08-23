package org.broadinstitute.dsde.workbench.leonardo.http

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.effect.std.Queue
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import com.google.api.gax.longrunning.OperationFuture
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.compute.v1.Operation
import fs2.Stream
import org.broadinstitute.dsde.workbench.azure._
import org.broadinstitute.dsde.workbench.google.{GoogleProjectDAO, HttpGoogleDirectoryDAO, HttpGoogleIamDAO}
import org.broadinstitute.dsde.workbench.google2.GKEModels.KubernetesClusterId
import org.broadinstitute.dsde.workbench.google2.{
  GKEService,
  GoogleComputeService,
  GoogleDataprocService,
  GoogleDiskService,
  GoogleResourceService,
  GoogleStorageService,
  KubernetesService
}
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.Task
import org.broadinstitute.dsde.workbench.leonardo.auth.SamAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleOAuth2Service
import org.broadinstitute.dsde.workbench.leonardo.dao.sam.SamService
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.dns.{KubernetesDnsCache, ProxyResolver, RuntimeDnsCache}
import org.broadinstitute.dsde.workbench.leonardo.http.service._
import org.broadinstitute.dsde.workbench.leonardo.model.ServiceAccountProvider
import org.broadinstitute.dsde.workbench.leonardo.monitor._
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.leonardo.{AppAccessScope, CloudService, KeyLock, LeoPublisher}
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.oauth2.OpenIDConnectConfiguration
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.broadinstitute.dsde.workbench.util2.messaging.{CloudPublisher, CloudSubscriber}
import org.broadinstitute.dsp.HelmInterpreter
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doReturn, spy, when}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.must.Matchers.have
import org.scalatest.matchers.should.Matchers.{a, convertToAnyShouldWrapper}
import org.scalatestplus.mockito.MockitoSugar
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import scalacache.Cache

import java.time.Instant
import javax.net.ssl.SSLContext
import scala.concurrent.ExecutionContext

class GcpDependenciesBuilderSpec
    extends TestKit(ActorSystem("leonardotest"))
    with AnyFlatSpecLike
    with MockitoSugar
    with BeforeAndAfterEach {
  implicit val logger: StructuredLogger[IO] = Slf4jLogger.getLogger[IO]
  implicit val ec: ExecutionContext = cats.effect.unsafe.IORuntime.global.compute
  implicit val dbRef: DbReference[IO] = createDbRefMock
  implicit val metrics: OpenTelemetryMetrics[IO] = mock[OpenTelemetryMetrics[IO]]

  private val baselineDependenciesMock =
    createBaselineDependenciesWithMocks
  private val gcpDependenciesMock = createGcpDependenciesWithMocks
  private val gcpDependenciesRegistryWithMocks = createGcpDependenciesRegistryWithMocks

  "GcpDependenciesBuilder" should "create dependency registry with expected services" in {
    val gcpDependencyBuilderSpy = spy(new GcpDependencyBuilder())
    doReturn(Resource.pure(gcpDependenciesMock))
      .when(gcpDependencyBuilderSpy)
      .createGcpDependencies[IO](any())(any(), any(), any(), any(), any(), any(), any())

    val registryResource = gcpDependencyBuilderSpy.createDependenciesRegistry(baselineDependenciesMock)

    val registry = registryResource
      .use { registry =>
        IO(registry)
      }
      .unsafeRunSync()

    registry.lookup[ProxyService].get shouldBe a[ProxyService]
    registry.lookup[RuntimeService[IO]].get shouldBe a[RuntimeService[IO]]
    registry.lookup[DiskService[IO]].get shouldBe a[DiskService[IO]]
    registry.lookup[DataprocInterpreter[IO]].get shouldBe a[DataprocInterpreter[IO]]
    registry.lookup[GceInterpreter[IO]].get shouldBe a[GceInterpreter[IO]]
    registry.lookup[GcpDependencies[IO]].get shouldBe a[GcpDependencies[IO]]
    registry.lookup[GKEAlgebra[IO]].get shouldBe a[GKEAlgebra[IO]]
    registry.lookup[RuntimeMonitor[IO, CloudService]].get shouldBe a[RuntimeMonitor[IO, CloudService]]
    registry.lookup[ResourcesService[IO]].get shouldBe a[ResourcesServiceInterp[IO]]
    registry.lookup[LeoAppServiceInterp[IO]].get shouldBe a[LeoAppServiceInterp[IO]]
    registry.lookup[RuntimeInstances[IO]].get shouldBe a[RuntimeInstances[IO]]
  }

  // NonLeoMessageSubscriber[IO], CloudSubscriber (NonLeoMessageSubscriber), and MonitorAtBoot
  private val expectedNumberGcpSpecificProcesses = 3

  it should "create a list of back-end processes" in {
    val gcpDependencyBuilderSpy = spy(new GcpDependencyBuilder())
    doReturn(Resource.pure(gcpDependenciesMock))
      .when(gcpDependencyBuilderSpy)
      .createGcpDependencies[IO](any())(any(), any(), any(), any(), any(), any(), any())

    val processes = gcpDependencyBuilderSpy.createCloudSpecificProcessesList(baselineDependenciesMock,
                                                                             gcpDependenciesRegistryWithMocks
    )

    processes shouldBe a[List[_]]
    // processesList should have size expectedNumberOfProcesses
    processes should have size expectedNumberGcpSpecificProcesses
  }

  private def createBaselineDependenciesWithMocks: BaselineDependencies[IO] =
    BaselineDependencies[IO](
      mock[SSLContext],
      mock[RuntimeDnsCache[IO]],
      mock[HttpSamDAO[IO]],
      mock[HttpDockerDAO[IO]],
      mock[HttpJupyterDAO[IO]],
      mock[HttpRStudioDAO[IO]],
      mock[HttpWelderDAO[IO]],
      mock[HttpWsmDao[IO]],
      mock[ServiceAccountProvider[IO]],
      mock[SamAuthProvider[IO]],
      mock[LeoPublisher[IO]],
      mock[Queue[IO, LeoPubsubMessage]],
      mock[Queue[IO, UpdateDateAccessedMessage]],
      mock[CloudSubscriber[IO, LeoPubsubMessage]],
      mock[Queue[IO, Task[IO]]],
      mock[KeyLock[IO, KubernetesClusterId]],
      mock[ProxyResolver[IO]],
      List.empty,
      mock[Cache[IO, String, (UserInfo, Instant)]],
      mock[Cache[IO, SamResourceCacheKey, (Option[String], Option[AppAccessScope])]],
      mock[OpenIDConnectConfiguration],
      mock[AppDAO[IO]],
      mock[WdsDAO[IO]],
      mock[CbasDAO[IO]],
      mock[CromwellDAO[IO]],
      mock[HailBatchDAO[IO]],
      mock[ListenerDAO[IO]],
      mock[HttpWsmClientProvider[IO]],
      mock[HttpBpmClientProvider[IO]],
      mock[AzureContainerService[IO]],
      mock[RuntimeServiceConfig],
      mock[KubernetesDnsCache[IO]],
      mock[HttpAppDescriptorDAO[IO]],
      mock[HelmInterpreter[IO]],
      mock[AzureRelayService[IO]],
      mock[AzureVmService[IO]],
      mock[Cache[IO, Long, OperationFuture[Operation, Operation]]],
      mock[AzureBatchService[IO]],
      mock[AzureApplicationInsightsService[IO]],
      mock[OpenTelemetryMetrics[IO]],
      mock[SamService[IO]]
    )
  private def createGcpDependenciesRegistryWithMocks = {
    val registry = ServicesRegistry()
    registry.register[ProxyService](mock[ProxyService])
    registry.register[RuntimeService[IO]](mock[RuntimeService[IO]])
    registry.register[DiskService[IO]](mock[DiskService[IO]])
    registry.register[DataprocInterpreter[IO]](mock[DataprocInterpreter[IO]])
    registry.register[GceInterpreter[IO]](mock[GceInterpreter[IO]])
    registry.register[GKEAlgebra[IO]](mock[GKEAlgebra[IO]])
    registry.register[RuntimeMonitor[IO, CloudService]](mock[RuntimeMonitor[IO, CloudService]])
    registry.register[ResourcesServiceInterp[IO]](mock[ResourcesServiceInterp[IO]])
    registry.register[LeoAppServiceInterp[IO]](mock[LeoAppServiceInterp[IO]])
    registry.register[GcpDependencies[IO]](createGcpDependenciesWithMocks)
    registry
  }

  private def createGcpDependenciesWithMocks = {
    val nonLeoSubscriberMock = mock[CloudSubscriber[IO, NonLeoMessage]]
    when(nonLeoSubscriberMock.messages).thenReturn(Stream.emits(List.empty).covary[IO])
    GcpDependencies[IO](
      mock[GoogleStorageService[IO]],
      mock[GoogleComputeService[IO]],
      mock[GoogleResourceService[IO]],
      mock[HttpGoogleDirectoryDAO],
      mock[CloudPublisher[IO]],
      mock[CloudPublisher[IO]],
      mock[HttpGoogleIamDAO],
      mock[GoogleDataprocService[IO]],
      mock[KubernetesDnsCache[IO]],
      mock[GKEService[IO]],
      mock[OpenTelemetryMetrics[IO]],
      mock[GoogleCredentials],
      mock[GoogleOAuth2Service[IO]],
      mock[GoogleDiskService[IO]],
      mock[GoogleProjectDAO],
      mock[BucketHelper[IO]],
      mock[VPCInterpreter[IO]],
      mock[KubernetesService[IO]],
      nonLeoSubscriberMock
    )
  }

  private def createDbRefMock: DbReference[IO] = {
    val dbRef = mock[DbReference[IO]]
    when(dbRef.inTransaction[Seq[RuntimeToMonitor]](any(), any())).thenReturn(IO(Seq.empty))
    dbRef
  }
}
