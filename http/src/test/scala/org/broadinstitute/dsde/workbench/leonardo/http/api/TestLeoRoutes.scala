package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import akka.http.scaladsl.model.HttpHeader
import akka.http.scaladsl.model.headers.{`Set-Cookie`, HttpCookiePair}
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import cats.effect.IO
import com.github.benmanes.caffeine.cache.Caffeine
import org.broadinstitute.dsde.workbench.google.GoogleStorageDAO
import org.broadinstitute.dsde.workbench.google.mock.{MockGoogleDirectoryDAO, MockGoogleIamDAO, MockGoogleStorageDAO}
import org.broadinstitute.dsde.workbench.google2.mock._
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.dao.google.MockGoogleOAuth2Service
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.broadinstitute.dsde.workbench.leonardo.dns.{
  KubernetesDnsCache,
  KubernetesDnsCacheKey,
  RuntimeDnsCache,
  RuntimeDnsCacheKey
}
import org.broadinstitute.dsde.workbench.leonardo.http.service._
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.scalactic.source.Position
import org.scalatest.concurrent.PatienceConfiguration.Interval
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.scalatestplus.mockito.MockitoSugar
import scalacache.Cache
import scalacache.caffeine.CaffeineCache

import java.io.ByteArrayInputStream
import java.time.Instant
import scala.concurrent.duration._
import scala.util.matching.Regex
trait TestLeoRoutes {
  this: ScalatestRouteTest
    with Matchers
    with ScalaFutures
    with LeonardoTestSuite
    with TestComponent
    with MockitoSugar =>
  implicit val timeout: RouteTestTimeout = RouteTestTimeout(20 seconds)

  // Set up the mock directoryDAO to have the Google group used to grant permission to users
  // to pull the custom dataproc image
  val mockGoogleDirectoryDAO = {
    val mockGoogleDirectoryDAOPatience = PatienceConfig(timeout = scaled(Span(30, Seconds)))
    val dao = new MockGoogleDirectoryDAO()
    dao
      .createGroup(Config.googleGroupsConfig.dataprocImageProjectGroupName,
                   Config.googleGroupsConfig.dataprocImageProjectGroupEmail,
                   Option(dao.lockedDownGroupSettings)
      )
      .futureValue(Interval(Span(30, Seconds)))(mockGoogleDirectoryDAOPatience, Position.here)
    dao
  }

  val mockGoogleIamDAO = new MockGoogleIamDAO
  val wsmDao = new MockWsmDAO
  val wsmClientProvider = mock[HttpWsmClientProvider[IO]]
  val mockPetGoogleStorageDAO: String => GoogleStorageDAO = _ => {
    val petMock = new MockGoogleStorageDAO
    petMock.buckets += userScriptBucketName -> Set(
      (userScriptObjectName, new ByteArrayInputStream("foo".getBytes()))
    )
    petMock.buckets += startUserScriptBucketName -> Set(
      (startUserScriptObjectName, new ByteArrayInputStream("foo".getBytes()))
    )
    petMock.buckets += jupyterExtensionBucket -> Set(
      (jupyterExtensionObject, new ByteArrayInputStream("foo".getBytes()))
    )
    petMock
  }
  // Route tests don't currently do cluster monitoring, so use NoopActor
  val bucketHelperConfig =
    BucketHelperConfig(imageConfig, welderConfig, proxyConfig, clusterFilesConfig)
  val bucketHelper =
    new BucketHelper[IO](bucketHelperConfig, mockGoogle2StorageDAO, serviceAccountProvider)
  val vpcInterp =
    new VPCInterpreter[IO](Config.vpcInterpreterConfig, FakeGoogleResourceService, FakeGoogleComputeService)
  val dataprocInterp =
    new DataprocInterpreter[IO](
      Config.dataprocInterpreterConfig,
      bucketHelper,
      vpcInterp,
      FakeGoogleDataprocService,
      FakeGoogleComputeService,
      MockGoogleDiskService,
      mockGoogleDirectoryDAO,
      mockGoogleIamDAO,
      FakeGoogleResourceService,
      MockWelderDAO
    )
  val gceInterp =
    new GceInterpreter[IO](Config.gceInterpreterConfig,
                           bucketHelper,
                           vpcInterp,
                           FakeGoogleComputeService,
                           MockGoogleDiskService,
                           MockWelderDAO
    )
  val runtimeInstances = new RuntimeInstances[IO](dataprocInterp, gceInterp)

  val leoKubernetesService: LeoAppServiceInterp[IO] = new LeoAppServiceInterp[IO](
    Config.appServiceConfig,
    allowListAuthProvider,
    serviceAccountProvider,
    QueueFactory.makePublisherQueue(),
    Some(FakeGoogleComputeService),
    Some(FakeGoogleResourceService),
    Config.gkeCustomAppConfig,
    wsmDao,
    wsmClientProvider
  )

  val serviceConfig = RuntimeServiceConfig(
    Config.proxyConfig.proxyUrlBase,
    imageConfig,
    autoFreezeConfig,
    dataprocConfig,
    Config.gceConfig,
    azureServiceConfig
  )

  val runtimev2Service =
    new RuntimeV2ServiceInterp[IO](
      serviceConfig,
      allowListAuthProvider,
      new MockWsmDAO,
      QueueFactory.makePublisherQueue(),
      QueueFactory.makeDateAccessedQueue(),
      wsmClientProvider
    )

  val underlyingRuntimeDnsCache =
    Caffeine.newBuilder().maximumSize(10000L).build[RuntimeDnsCacheKey, scalacache.Entry[HostStatus]]()
  val runtimeDnsCaffeineCache: Cache[IO, RuntimeDnsCacheKey, HostStatus] =
    CaffeineCache[IO, RuntimeDnsCacheKey, HostStatus](underlyingRuntimeDnsCache)
  val runtimeDnsCache =
    new RuntimeDnsCache[IO](proxyConfig, testDbRef, hostToIpMapping, runtimeDnsCaffeineCache)

  val underlyingKubernetesDnsCache =
    Caffeine.newBuilder().maximumSize(10000L).build[KubernetesDnsCacheKey, scalacache.Entry[HostStatus]]()
  val kubernetesDnsCaffeineCache: Cache[IO, KubernetesDnsCacheKey, HostStatus] =
    CaffeineCache[IO, KubernetesDnsCacheKey, HostStatus](underlyingKubernetesDnsCache)
  val kubernetesDnsCache =
    new KubernetesDnsCache[IO](proxyConfig, testDbRef, hostToIpMapping, kubernetesDnsCaffeineCache)

  val underlyingGoogleTokenCache =
    Caffeine.newBuilder().maximumSize(10000L).build[String, scalacache.Entry[(UserInfo, Instant)]]()
  val googleTokenCache: Cache[IO, String, (UserInfo, Instant)] =
    CaffeineCache[IO, String, (UserInfo, Instant)](underlyingGoogleTokenCache)
  val underlyingSamResourceCache =
    Caffeine
      .newBuilder()
      .maximumSize(10000L)
      .build[SamResourceCacheKey, scalacache.Entry[(Option[String], Option[AppAccessScope])]]()
  val samResourceCache: Cache[IO, SamResourceCacheKey, (Option[String], Option[AppAccessScope])] =
    CaffeineCache[IO, SamResourceCacheKey, (Option[String], Option[AppAccessScope])](underlyingSamResourceCache)

  val proxyService = new MockProxyService(proxyConfig,
                                          MockJupyterDAO,
                                          allowListAuthProvider,
                                          runtimeDnsCache,
                                          kubernetesDnsCache,
                                          googleTokenCache,
                                          samResourceCache,
                                          MockGoogleOAuth2Service
  )

  val statusService =
    new StatusService(mockSamDAO, testDbRef, pollInterval = 1.second)
  val timedUserInfo = defaultUserInfo.copy(tokenExpiresIn = tokenAge)
  val corsSupport = new CorsSupport(contentSecurityPolicy, refererConfig)
  val statusRoutes = new StatusRoutes(statusService)
  val userInfoDirectives = new MockUserInfoDirectives {
    override val userInfo: UserInfo = defaultUserInfo
  }
  val timedUserInfoDirectives = new MockUserInfoDirectives {
    override val userInfo: UserInfo = timedUserInfo
  }

  val runtimeService = RuntimeService(
    serviceConfig,
    ConfigReader.appConfig.persistentDisk,
    allowListAuthProvider,
    serviceAccountProvider,
    new MockDockerDAO,
    FakeGoogleStorageInterpreter,
    FakeGoogleComputeService,
    QueueFactory.makePublisherQueue()
  )

  val gcpOnlyServicesRegistry = {
    val registry = ServicesRegistry()
    registry.register[ProxyService](proxyService)
    registry.register[RuntimeService[IO]](runtimeService)
    registry.register[DiskService[IO]](MockDiskServiceInterp)
    registry.register[ResourcesService[IO]](MockResourcesService)
    registry
  }

  val httpRoutes =
    new HttpRoutes(
      openIdConnectionConfiguration,
      statusService,
      gcpOnlyServicesRegistry,
      MockDiskV2ServiceInterp,
      leoKubernetesService,
      runtimev2Service,
      MockAdminServiceInterp,
      userInfoDirectives,
      contentSecurityPolicy,
      refererConfig
    )

  val timedHttpRoutes =
    new HttpRoutes(
      openIdConnectionConfiguration,
      statusService,
      gcpOnlyServicesRegistry,
      MockDiskV2ServiceInterp,
      leoKubernetesService,
      runtimev2Service,
      MockAdminServiceInterp,
      timedUserInfoDirectives,
      contentSecurityPolicy,
      refererConfig
    )
  def roundUpToNearestTen(d: Long): Long = (Math.ceil(d / 10.0) * 10).toLong
  val cookieMaxAgeRegex: Regex = "Max-Age=(\\d+);".r

  protected def validateCookie(setCookie: Option[`Set-Cookie`],
                               expectedCookie: HttpCookiePair = tokenCookie,
                               age: Long = tokenAge
  ): Unit = {
    setCookie shouldBe defined
    val cookie = setCookie.get.cookie
    cookie.name shouldBe expectedCookie.name
    cookie.value shouldBe expectedCookie.value
    cookie.secure shouldBe true
    cookie.maxAge.map(roundUpToNearestTen) shouldBe Some(age) // test execution loses some milliseconds
    cookie.domain shouldBe None
    cookie.path shouldBe Some("/")
  }

  // TODO: remove when we upgrade to akka-http 10.2.0.
  // See comment in CookieHelper.setTokenCookie.
  protected def validateRawCookie(setCookie: Option[HttpHeader],
                                  expectedCookie: HttpCookiePair = tokenCookie,
                                  age: Long = tokenAge
  ): Unit = {
    setCookie shouldBe defined
    setCookie.get.name shouldBe "Set-Cookie"

    // test execution loses some milliseconds, so round Max-Age before validation
    val replaced =
      cookieMaxAgeRegex.replaceAllIn(setCookie.get.value, m => s"Max-Age=${roundUpToNearestTen(m.group(1).toLong)};")

    replaced shouldBe s"${expectedCookie.name}=${expectedCookie.value}; Max-Age=${age.toString}; Path=/; Secure; SameSite=None; HttpOnly; Partitioned;"
  }

  protected def validateUnsetRawCookie(setCookie: Option[HttpHeader],
                                       expectedCookie: HttpCookiePair = tokenCookie
  ): Unit = {
    setCookie shouldBe defined
    setCookie.get.name shouldBe "Set-Cookie"
    setCookie.get.value shouldBe s"${tokenName}=unset; expires=Thu, 01 Jan 1970 00:00:00 GMT; Path=/; Secure; SameSite=None; HttpOnly; Partitioned;"
  }
}
