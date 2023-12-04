package org.broadinstitute.dede.workbench.leonardo.provider

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.google2.{KubernetesSerializableName, MachineTypeName, RegionName}
import org.broadinstitute.dsde.workbench.leonardo.config.{ContentSecurityPolicyConfig, RefererConfig}
import org.broadinstitute.dsde.workbench.leonardo.http.GetAppResponse
import org.broadinstitute.dsde.workbench.leonardo.http.api.{HttpRoutes, MockUserInfoDirectives, UserInfoDirectives}
import org.broadinstitute.dsde.workbench.leonardo.http.service._
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, AppError, AppName, AppStatus, AppType, CloudContext, KubernetesRuntimeConfig, NumNodes}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.oauth2.OpenIDConnectConfiguration
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.broadinstitute.dsp.ChartName
import org.mockito.ArgumentMatchers.{any, anyLong, anyMap, anyString}
import org.mockito.Mockito.{reset, when}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import org.scalatestplus.mockito.MockitoSugar.mock
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pact4s.provider.StateManagement.StateManagementFunction
import pact4s.provider._
import pact4s.scalatest.PactVerifier

import java.io.File
import java.lang.Thread.sleep
import java.net.URL
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import org.broadinstitute.dsde.workbench.leonardo.AuditInfo
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.defaultUserInfo

import java.time.Instant

object States {
  val AppExists = "a"
}

class LeoProvider
  extends AnyFlatSpec
  with BeforeAndAfterAll
  with PactVerifier {


implicit val metrics: OpenTelemetryMetrics[IO] = mock[OpenTelemetryMetrics[IO]]
implicit val loggerIO: StructuredLogger[IO] = Slf4jLogger.getLogger[IO]
implicit val ec: ExecutionContextExecutor = ExecutionContext.global
implicit val system: ActorSystem = ActorSystem("leotests")

  val mockOpenIDConnectConfiguration: OpenIDConnectConfiguration = mock[OpenIDConnectConfiguration];
  val mockStatusService: StatusService = mock[StatusService]
  val mockProxyService: ProxyService = mock[ProxyService]
  val mockRuntimeService: RuntimeService[IO] = mock[RuntimeService[IO]]
  val mockDiskService: DiskService[IO] = mock[DiskService[IO]]
  val mockDiskV2Service: DiskV2Service[IO] = mock[DiskV2Service[IO]]
  val mockAppService: AppService[IO] = mock[AppService[IO]]
  val mockRuntimeV2Service: RuntimeV2Service[IO] = mock[RuntimeV2Service[IO]]
  val mockAdminService: AdminService[IO] = mock[AdminService[IO]]
//  val mockUserInfoDirectives: UserInfoDirectives = mock[UserInfoDirectives];
  val mockContentSecurityPolicyConfig: ContentSecurityPolicyConfig = mock[ContentSecurityPolicyConfig];
  val refererConfig: RefererConfig = new RefererConfig(Set("*"),true, false)
  val mockUserInfoDirectives = new MockUserInfoDirectives {
    override val userInfo: UserInfo = defaultUserInfo
  }

  val routes =
    new HttpRoutes(
      mockOpenIDConnectConfiguration,
      mockStatusService,
      mockProxyService,
      mockRuntimeService,
      mockDiskService,
      mockDiskV2Service,
      mockAppService,
      mockRuntimeV2Service,
      mockAdminService,
      mockUserInfoDirectives,
      mockContentSecurityPolicyConfig,
      refererConfig
    )

  override def beforeAll(): Unit = {
    startLeo.unsafeToFuture()
    sleep(5000)

  }

  def resetMocks() = {
    reset(mockOpenIDConnectConfiguration);
    reset(mockStatusService);
    reset(mockProxyService);
    reset(mockRuntimeService);
    reset(mockDiskService);
    reset(mockDiskV2Service);
    reset(mockAppService);
    reset(mockRuntimeV2Service);
    reset(mockAdminService);
//    reset(mockUserInfoDirectives);
    reset(mockContentSecurityPolicyConfig);
    when(metrics.incrementCounter(anyString(),anyLong(), any())).thenReturn(IO.pure(None));
  }


  private def startLeo: IO[Http.ServerBinding] = {
    for {
      binding <- IO
        .fromFuture(IO(Http().newServerAt("localhost", 8080).bind(routes.route)))
        .onError { t: Throwable =>
          loggerIO.error(t.toString())
        }
      _ <- IO.fromFuture(IO(binding.whenTerminated))
      _ <- IO(system.terminate())
    } yield binding
  }

  private val providerStatesHandler: StateManagementFunction = StateManagementFunction {
    case ProviderState(States.AppExists, _) =>
      when(mockAppService.getApp(any[UserInfo],any[CloudContext.Gcp],AppName(anyString()))(any[Ask[IO, AppContext]])).thenReturn(IO {
        GetAppResponse(
          None,
          AppName("example"),
          CloudContext.Gcp(GoogleProject("exampleProject")),
          RegionName("exampleRegion"),
          KubernetesRuntimeConfig(NumNodes(8),MachineTypeName("exampleMachine"),true),
          List.empty[AppError],
          AppStatus.Unspecified,
          Map.empty[KubernetesSerializableName.ServiceName, URL],
          None,
          Map.empty[String,String],
          AuditInfo(WorkbenchEmail(""),Instant.now(),None,Instant.now()),
          AppType.CromwellRunnerApp,
          ChartName(""),
          None,
          Map.empty[String,String])})
    case _ =>
      loggerIO.debug("other state")
  }

  val provider: ProviderInfoBuilder = ProviderInfoBuilder(
    name = "y",
    pactSource = PactSource
      .FileSource(
        Map("x" -> new File("./pact4s/src/test/resources/x-y.json"))
      ))
    .withStateManagementFunction(
      providerStatesHandler
      .withBeforeEach(() => resetMocks())
    )

    .withHost("localhost")
    .withPort(8080)

  it should "Verify pacts" in {
    verifyPacts(
      publishVerificationResults = None,
      providerVerificationOptions = Nil,
      verificationTimeout = Some(1000.seconds)
    )
  }
}
