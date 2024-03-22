package org.broadinstitute.dsde.workbench.leonardo.http

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.effect.std.Queue
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import fs2.Stream
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.Task
import org.broadinstitute.dsde.workbench.leonardo.LeoPublisher
import org.broadinstitute.dsde.workbench.leonardo.config.LeoExecutionModeConfig
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.http.service.LeoAppServiceInterp
import org.broadinstitute.dsde.workbench.leonardo.monitor.{LeoPubsubMessage, UpdateDateAccessedMessage}
import org.broadinstitute.dsde.workbench.leonardo.util.ServicesRegistry
import org.broadinstitute.dsde.workbench.openTelemetry.{FakeOpenTelemetryMetricsInterpreter, OpenTelemetryMetrics}
import org.broadinstitute.dsde.workbench.util2.messaging.CloudSubscriber
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{verify, when}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.must.Matchers.have
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatestplus.mockito.MockitoSugar
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.ExecutionContext

class AppDependenciesBuilderSpec
    extends TestKit(ActorSystem("leonardotest"))
    with AnyFlatSpecLike
    with MockitoSugar
    with BeforeAndAfterEach {
  implicit val logger: StructuredLogger[IO] = Slf4jLogger.getLogger[IO]
  implicit val ec: ExecutionContext = cats.effect.unsafe.IORuntime.global.compute
  implicit val dbRef: DbReference[IO] = mock[DbReference[IO]]
  implicit val metrics: OpenTelemetryMetrics[IO] = mock[OpenTelemetryMetrics[IO]]

  private val dependencyRegistry = {
    val servicesRegistry = ServicesRegistry()
    servicesRegistry.register[LeoAppServiceInterp[IO]](mock[LeoAppServiceInterp[IO]])

    servicesRegistry
  }

  private val cloudSpecificProcessesList = List(Stream.eval(IO.unit))

  private var baselineDependenciesMock: BaselineDependencies[IO] = _

  override def beforeEach(): Unit =
    baselineDependenciesMock = createBaselineDependenciesMock

  "AppDependenciesBuilder" should "call cloud dependency builder to get dependency registry and process list" in {

    val cloudHostDependenciesBuilder = createCloudHostDependenciesBuilderMock(cloudSpecificProcessesList)

    val appDependenciesBuilder =
      new AppDependenciesBuilder(createBaselineDependenciesBuilderMock, cloudHostDependenciesBuilder)

    val result = for {
      _ <- appDependenciesBuilder.createAppDependencies()
    } yield ()

    result.use(_ => IO.unit).unsafeRunSync()

    verify(cloudHostDependenciesBuilder).createDependenciesRegistry(any())(any(), any(), any(), any(), any())
    verify(cloudHostDependenciesBuilder).createCloudSpecificProcessesList(any(), any())(any(), any(), any(), any())
  }

  // There are 6 processes that are common to backend and combined modes:
  // 1. AsyncTaskProcessor
  // 2. LeoPubsubMessageSubscriber
  // 3. CloudSubscriber (Leo messaging subscriber)
  // 4. AutopauseMonitor
  // 5. AutodeleteMonitor
  // 6. LeoMetricsMonitor
  private val baselineNumberOfExpectedProcessesForBackendAndCombinedModes = 6

  // There are 2 processes that are specific to frontend:
  // 1. DateAccessedUpdater
  // 2. BaselineDependencies.recordMetricsProcesses, which is a list but the mock returns a single process
  private val baselineNumberOfExpectedProcessesForFrontendAndCombinedModes = 2

  // There is 1 process that is expected for all cases:
  // 1.- LeoPublisher
  private val baselineNumberOfExpectedProcessesForAllModes = 1

  it should "return expected number of processes when running in combined mode" in {

    val appDependenciesBuilder = createAppDependenciesBuilder()

    val result = for {
      backEndProcesses <- appDependenciesBuilder.createBackEndDependencies(baselineDependenciesMock,
                                                                           dependencyRegistry,
                                                                           LeoExecutionModeConfig.Combined
      )
    } yield backEndProcesses

    val processesList = result.use(b => IO(b.processesList)).unsafeRunSync()

    // Validate the process list based on the expected number of processes.
    // This is because the returned type cannot be tied back to the actual processes
    val numberOfCloudSpecificProcesses =
      cloudSpecificProcessesList.length // processes that are specific to cloud hosting provider
    val expectedNumberOfProcesses = baselineNumberOfExpectedProcessesForBackendAndCombinedModes +
      numberOfCloudSpecificProcesses +
      baselineNumberOfExpectedProcessesForFrontendAndCombinedModes +
      baselineNumberOfExpectedProcessesForAllModes

    processesList should have size expectedNumberOfProcesses
    // also validate that the recordMetricsProcesses was called (frontLeoOnly and combined mode)
    verify(baselineDependenciesMock).recordMetricsProcesses
  }

  it should "return expected number of processes when running in backend mode" in {

    val appDependenciesBuilder = createAppDependenciesBuilder()

    val result = for {
      backEndProcesses <- appDependenciesBuilder.createBackEndDependencies(baselineDependenciesMock,
                                                                           dependencyRegistry,
                                                                           LeoExecutionModeConfig.BackLeoOnly
      )
    } yield backEndProcesses

    val processesList = result.use(b => IO(b.processesList)).unsafeRunSync()

    // Validate the process list based on the expected number of processes.
    // This is because the returned type cannot be tied back to the actual processes
    val numberOfCloudSpecificProcesses =
      cloudSpecificProcessesList.length // processes that are specific to cloud hosting provider
    val expectedNumberOfProcesses = baselineNumberOfExpectedProcessesForBackendAndCombinedModes +
      numberOfCloudSpecificProcesses +
      baselineNumberOfExpectedProcessesForAllModes

    processesList should have size expectedNumberOfProcesses
  }

  it should "return expected number of processes when running in frontend mode" in {

    val appDependenciesBuilder = createAppDependenciesBuilder()

    val result = for {
      backEndProcesses <- appDependenciesBuilder.createBackEndDependencies(baselineDependenciesMock,
                                                                           dependencyRegistry,
                                                                           LeoExecutionModeConfig.FrontLeoOnly
      )
    } yield backEndProcesses

    val processesList = result.use(b => IO(b.processesList)).unsafeRunSync()

    // Validate the process list based on the expected number of processes.
    // This is because the returned type cannot be tied back to the actual processes
    val numberOfCloudSpecificProcesses =
      cloudSpecificProcessesList.length // processes that are specific to cloud hosting provider
    val expectedNumberOfProcesses = baselineNumberOfExpectedProcessesForFrontendAndCombinedModes +
      numberOfCloudSpecificProcesses +
      baselineNumberOfExpectedProcessesForAllModes

    processesList should have size expectedNumberOfProcesses
    // also validate that the recordMetricsProcesses was called (frontLeoOnly and combined mode)
    verify(baselineDependenciesMock).recordMetricsProcesses
  }

  private def createAppDependenciesBuilder() = {
    val cloudHostDependenciesBuilder = createCloudHostDependenciesBuilderMock(cloudSpecificProcessesList)

    val appDependenciesBuilder =
      new AppDependenciesBuilder(createBaselineDependenciesBuilderMock, cloudHostDependenciesBuilder)
    appDependenciesBuilder
  }

  private def createBaselineDependenciesBuilderMock = {
    val baselineDependenciesBuilder = mock[BaselineDependenciesBuilder]
    when(baselineDependenciesBuilder.createBaselineDependencies[IO]()(any(), any(), any(), any(), any(), any(), any()))
      .thenReturn(Resource.pure[IO, BaselineDependencies[IO]](baselineDependenciesMock))

    baselineDependenciesBuilder
  }
  private def createCloudHostDependenciesBuilderMock(processList: List[Stream[IO, Unit]]) = {
    val cloudHostDependenciesBuilder = mock[CloudDependenciesBuilder]
    when(cloudHostDependenciesBuilder.createDependenciesRegistry(any())(any(), any(), any(), any(), any()))
      .thenReturn(Resource.pure[IO, ServicesRegistry](dependencyRegistry))
    when(cloudHostDependenciesBuilder.createCloudSpecificProcessesList(any(), any())(any(), any(), any(), any()))
      .thenReturn(processList)
    when(cloudHostDependenciesBuilder.registryOpenTelemetryTracing)
      .thenReturn(Resource.pure[IO, Unit](()))
    cloudHostDependenciesBuilder
  }
  private def createBaselineDependenciesMock = {
    val baselineDependencies = mock[BaselineDependencies[IO]]
    when(baselineDependencies.openTelemetryMetrics).thenReturn(FakeOpenTelemetryMetricsInterpreter)
    val updaterQueue =
      Queue.bounded[IO, UpdateDateAccessedMessage](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    when(baselineDependencies.dateAccessedUpdaterQueue).thenReturn(updaterQueue)
    val asyncTasks = Queue.bounded[IO, Task[IO]](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    when(baselineDependencies.asyncTasksQueue).thenReturn(asyncTasks)
    val cloudSubscriber = mock[CloudSubscriber[IO, LeoPubsubMessage]]
    when(cloudSubscriber.messages).thenReturn(Stream.emits(List.empty).covary[IO])
    when(baselineDependencies.subscriber).thenReturn(cloudSubscriber)
    when(baselineDependencies.recordMetricsProcesses).thenReturn(List.empty)
    when(baselineDependencies.leoPublisher).thenReturn(mock[LeoPublisher[IO]])
    // this means there is one recordMetricsProcesses expected while testing
    when(baselineDependencies.recordMetricsProcesses).thenReturn(List(Stream.eval(IO.unit)))

    baselineDependencies
  }
}
