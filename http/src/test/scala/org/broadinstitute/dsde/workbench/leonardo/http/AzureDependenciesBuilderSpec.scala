package org.broadinstitute.dsde.workbench.leonardo.http

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.effect.IO
import cats.effect.std.Queue
import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.leonardo.dao.{HttpSamDAO, HttpWsmDao}
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.http.service.LeoAppServiceInterp
import org.broadinstitute.dsde.workbench.leonardo.monitor.{LeoPubsubMessage, RuntimeToMonitor}
import org.broadinstitute.dsde.workbench.leonardo.util.ServicesRegistry
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.must.Matchers.have
import org.scalatest.matchers.should.Matchers.{a, convertToAnyShouldWrapper}
import org.scalatestplus.mockito.MockitoSugar
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.ExecutionContext

class AzureDependenciesBuilderSpec
    extends TestKit(ActorSystem("leonardotest"))
    with AnyFlatSpecLike
    with MockitoSugar
    with BeforeAndAfterEach {
  implicit val logger: StructuredLogger[IO] = Slf4jLogger.getLogger[IO]
  implicit val ec: ExecutionContext = cats.effect.unsafe.IORuntime.global.compute
  implicit val dbRef: DbReference[IO] = createDbRefMock
  implicit val metrics: OpenTelemetryMetrics[IO] = mock[OpenTelemetryMetrics[IO]]

  private val baselineDependenciesMock = createBaselineDependenciesMock

  // Expected number of back-end processes:
  // 1. Monitor at Boot
  val numberOfExpectedProcesses = 1
  "AzureDependenciesBuilder" should "return expected number of back-end processes" in {
    val azureDependenciesBuilder = new AzureDependenciesBuilder
    val cloudSpecificDependenciesRegistry = ServicesRegistry()

    val processes =
      azureDependenciesBuilder.createCloudSpecificProcessesList(baselineDependenciesMock,
                                                                cloudSpecificDependenciesRegistry
      )

    processes should have size numberOfExpectedProcesses
  }

  it should "create a dependency registry with an instance of LeoAppServiceInterp[IO]" in {
    val azureDependenciesBuilder = new AzureDependenciesBuilder
    val baselineDependencies = mock[BaselineDependencies[IO]]

    val registry = azureDependenciesBuilder.createDependenciesRegistry(baselineDependencies)

    val result = registry.use { registry =>
      IO(registry.lookup[LeoAppServiceInterp[IO]])
    }

    result.unsafeRunSync().get shouldBe a[LeoAppServiceInterp[IO]]
  }
  private def createDbRefMock: DbReference[IO] = {
    val dbRef = mock[DbReference[IO]]
    when(dbRef.inTransaction[Seq[RuntimeToMonitor]](any(), any())).thenReturn(IO(Seq.empty))
    dbRef
  }
  private def createBaselineDependenciesMock = {
    val baselineDependencies = mock[BaselineDependencies[IO]]
    val publisherQueue =
      Queue.bounded[IO, LeoPubsubMessage](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    when(baselineDependencies.publisherQueue).thenReturn(publisherQueue)
    when(baselineDependencies.samDAO).thenReturn(mock[HttpSamDAO[IO]])
    when(baselineDependencies.wsmDAO).thenReturn(mock[HttpWsmDao[IO]])
    baselineDependencies
  }
}
