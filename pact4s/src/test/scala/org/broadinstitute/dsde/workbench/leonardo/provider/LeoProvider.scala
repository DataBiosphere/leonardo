package org.broadinstitute.dsde.workbench.leonardo.provider

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.google2.{DiskName, KubernetesSerializableName, MachineTypeName, RegionName}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.defaultUserInfo
import org.broadinstitute.dsde.workbench.leonardo.config.{ContentSecurityPolicyConfig, RefererConfig}
import org.broadinstitute.dsde.workbench.leonardo.http.GetAppResponse
import org.broadinstitute.dsde.workbench.leonardo.http.api.{HttpRoutes, MockUserInfoDirectives}
import org.broadinstitute.dsde.workbench.leonardo.http.service._
import org.broadinstitute.dsde.workbench.leonardo.{
  AppContext,
  AppError,
  AppName,
  AppStatus,
  AppType,
  AuditInfo,
  CloudContext,
  KubernetesRuntimeConfig,
  NumNodes
}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.oauth2.OpenIDConnectConfiguration
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.broadinstitute.dsp.ChartName
import org.mockito.ArgumentMatchers.{any, anyLong, anyString}
import org.mockito.Mockito.{reset, when}
import org.mockito.stubbing.OngoingStubbing
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import org.scalatestplus.mockito.MockitoSugar.mock
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pact4s.provider.Authentication.BasicAuth
import pact4s.provider.StateManagement.StateManagementFunction
import pact4s.provider._
import pact4s.scalatest.PactVerifier

import java.lang.Thread.sleep
import java.net.URL
import java.time.Instant
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

object States {
  val AppExists = "there is an app in a Google project"
}

class LeoProvider extends AnyFlatSpec with BeforeAndAfterAll with PactVerifier {

  implicit val metrics: OpenTelemetryMetrics[IO] = mock[OpenTelemetryMetrics[IO]]
  implicit val loggerIO: StructuredLogger[IO] = Slf4jLogger.getLogger[IO]
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global
  implicit val system: ActorSystem = ActorSystem("leotests")

  val mockOpenIDConnectConfiguration: OpenIDConnectConfiguration = mock[OpenIDConnectConfiguration]
  val mockStatusService: StatusService = mock[StatusService]
  val mockProxyService: ProxyService = mock[ProxyService]
  val mockRuntimeService: RuntimeService[IO] = mock[RuntimeService[IO]]
  val mockDiskService: DiskService[IO] = mock[DiskService[IO]]
  val mockDiskV2Service: DiskV2Service[IO] = mock[DiskV2Service[IO]]
  val mockAppService: AppService[IO] = mock[AppService[IO]]
  val mockRuntimeV2Service: RuntimeV2Service[IO] = mock[RuntimeV2Service[IO]]
  val mockAdminService: AdminService[IO] = mock[AdminService[IO]]
  val mockContentSecurityPolicyConfig: ContentSecurityPolicyConfig = mock[ContentSecurityPolicyConfig]
  val refererConfig: RefererConfig = RefererConfig(Set("*"), enabled = true)
  val mockUserInfoDirectives: MockUserInfoDirectives = new MockUserInfoDirectives {
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
      MockResourcesService,
      mockUserInfoDirectives,
      mockContentSecurityPolicyConfig,
      refererConfig
    )

  private val providerStatesHandler: StateManagementFunction = StateManagementFunction {
    case ProviderState(States.AppExists, _) =>
      when(mockAppService.getApp(any[UserInfo], any[CloudContext.Gcp], AppName(anyString()))(any[Ask[IO, AppContext]]))
        .thenReturn(IO {
          GetAppResponse(
            None,
            AppName("exampleApp"),
            CloudContext.Gcp(GoogleProject("exampleProject")),
            RegionName("exampleRegion"),
            KubernetesRuntimeConfig(NumNodes(8), MachineTypeName("exampleMachine"), autoscalingEnabled = true),
            List.empty[AppError],
            AppStatus.Unspecified,
            Map.empty[KubernetesSerializableName.ServiceName, URL],
            Some(DiskName("exampleDiskName")),
            Map.empty[String, String],
            AuditInfo(WorkbenchEmail(""), Instant.now(), None, Instant.now()),
            AppType.CromwellRunnerApp,
            ChartName(""),
            None,
            Map.empty[String, String]
          )
        })
    case _ =>
      loggerIO.debug("other state")
  }

  lazy val pactBrokerUrl: String = sys.env.getOrElse("PACT_BROKER_URL", "")
  lazy val pactBrokerUser: String = sys.env.getOrElse("PACT_BROKER_USERNAME", "")
  lazy val pactBrokerPass: String = sys.env.getOrElse("PACT_BROKER_PASSWORD", "")
  // Provider branch, semver
  lazy val providerBranch: String = sys.env.getOrElse("PROVIDER_BRANCH", "")
  lazy val providerVer: String = sys.env.getOrElse("PROVIDER_VERSION", "")
  // Consumer name, branch, semver (used for webhook events only)
  lazy val consumerName: Option[String] = sys.env.get("CONSUMER_NAME")
  lazy val consumerBranch: Option[String] = sys.env.get("CONSUMER_BRANCH")
  // This matches the latest commit of the consumer branch that triggered the webhook event
  lazy val consumerVer: Option[String] = sys.env.get("CONSUMER_VERSION")

  var consumerVersionSelectors: ConsumerVersionSelectors = ConsumerVersionSelectors()
  // consumerVersionSelectors = consumerVersionSelectors.mainBranch
  // The following match condition basically says
  // 1. If verification is triggered by consumer pact change, verify only the changed pact.
  // 2. For normal Sam PR, verify all consumer pacts in Pact Broker labelled with a deployed environment (alpha, dev, prod, staging).
  consumerBranch match {
    case Some(s) if !s.isBlank => consumerVersionSelectors = consumerVersionSelectors.branch(s, consumerName)
    case _                     => consumerVersionSelectors = consumerVersionSelectors.deployedOrReleased.mainBranch
  }

  val provider: ProviderInfoBuilder =
    ProviderInfoBuilder(name = "leonardo",
                        PactSource
                          .PactBrokerWithSelectors(pactBrokerUrl)
                          .withAuth(BasicAuth(pactBrokerUser, pactBrokerPass))
    )
      .withStateManagementFunction(
        providerStatesHandler
          .withBeforeEach(() => resetMocks())
      )
      .withHost("localhost")
      .withPort(8080)

  override def beforeAll(): Unit = {
    startLeo.unsafeToFuture()
    startLeo.start
    sleep(5000)

  }

  private def startLeo: IO[Http.ServerBinding] =
    for {
      binding <- IO
        .fromFuture(IO(Http().newServerAt("localhost", 8080).bind(routes.route)))
        .onError { t: Throwable =>
          loggerIO.error(t.toString)
        }
      _ <- IO.fromFuture(IO(binding.whenTerminated))
      _ <- IO(system.terminate())
    } yield binding

  def resetMocks(): OngoingStubbing[IO[Unit]] = {
    reset(mockOpenIDConnectConfiguration)
    reset(mockStatusService)
    reset(mockProxyService)
    reset(mockRuntimeService)
    reset(mockDiskService)
    reset(mockDiskV2Service)
    reset(mockAppService)
    reset(mockRuntimeV2Service)
    reset(mockAdminService)
    reset(mockContentSecurityPolicyConfig)
    when(metrics.incrementCounter(anyString(), anyLong(), any())).thenReturn(IO.pure(None))
  }

  it should "Verify pacts" in {
    val publishResults = sys.env.getOrElse("PACT_PUBLISH_RESULTS", "false").toBoolean
    verifyPacts(
      providerBranch = if (providerBranch.isEmpty) None else Some(Branch(providerBranch)),
      publishVerificationResults =
        if (publishResults)
          Some(
            PublishVerificationResults(providerVer, ProviderTags(providerBranch))
          )
        else None,
      providerVerificationOptions = Seq(
        ProviderVerificationOption.SHOW_STACKTRACE
      ).toList,
      verificationTimeout = Some(30.seconds)
    )
  }
}
