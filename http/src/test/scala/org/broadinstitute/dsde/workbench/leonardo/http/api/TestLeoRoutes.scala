package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import java.io.ByteArrayInputStream
import akka.http.scaladsl.model.HttpHeader
import akka.http.scaladsl.model.headers.{`Set-Cookie`, HttpCookiePair}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import cats.effect.IO
import org.broadinstitute.dsde.workbench.google.GoogleStorageDAO
import org.broadinstitute.dsde.workbench.google.mock.{MockGoogleDirectoryDAO, MockGoogleIamDAO, MockGoogleStorageDAO}
import org.broadinstitute.dsde.workbench.google2.MockGoogleDiskService
import org.broadinstitute.dsde.workbench.google2.mock.{
  FakeGoogleComputeService,
  FakeGoogleDataprocService,
  FakeGoogleResourceService,
  FakeGoogleStorageInterpreter,
  MockComputePollOperation
}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.{leonaroBaseUrl => _, _}
import org.broadinstitute.dsde.workbench.leonardo.config.Config._
import org.broadinstitute.dsde.workbench.leonardo.dao.google.MockGoogleOAuth2Service
import org.broadinstitute.dsde.workbench.leonardo.dao.{MockDockerDAO, MockJupyterDAO, MockWelderDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.{DbReference, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.dns.{KubernetesDnsCache, RuntimeDnsCache}
import org.broadinstitute.dsde.workbench.leonardo.http.service._
import org.broadinstitute.dsde.workbench.leonardo.http.service.MockDiskServiceInterp
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.scalactic.source.Position
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.{serviceAccountProvider, whitelistAuthProvider}
import org.broadinstitute.dsde.workbench.leonardo.algebra.{MockSamDAO, VPCInterpreter}
import org.broadinstitute.dsde.workbench.leonardo.config.Config

import scala.concurrent.duration._
import scala.util.matching.Regex

trait TestLeoRoutes {
  this: ScalatestRouteTest with Matchers with ScalaFutures with LeonardoTestSuite with TestComponent =>
  // Set up the mock directoryDAO to have the Google group used to grant permission to users
  // to pull the custom dataproc image
  val mockGoogleDirectoryDAO = {
    val mockGoogleDirectoryDAOPatience = PatienceConfig(timeout = scaled(Span(30, Seconds)))
    val dao = new MockGoogleDirectoryDAO()
    dao
      .createGroup(googleGroupsConfig.dataprocImageProjectGroupName,
                   googleGroupsConfig.dataprocImageProjectGroupEmail,
                   Option(dao.lockedDownGroupSettings))
      .futureValue(mockGoogleDirectoryDAOPatience, Position.here)
    dao
  }

  val mockGoogleIamDAO = new MockGoogleIamDAO
  val mockPetGoogleStorageDAO: String => GoogleStorageDAO = _ => {
    val petMock = new MockGoogleStorageDAO
    petMock.buckets += jupyterUserScriptBucketName -> Set(
      (jupyterUserScriptObjectName, new ByteArrayInputStream("foo".getBytes()))
    )
    petMock.buckets += jupyterStartUserScriptBucketName -> Set(
      (jupyterStartUserScriptObjectName, new ByteArrayInputStream("foo".getBytes()))
    )
    petMock.buckets += jupyterExtensionBucket -> Set(
      (jupyterExtensionObject, new ByteArrayInputStream("foo".getBytes()))
    )
    petMock
  }
  // Route tests don't currently do cluster monitoring, so use NoopActor
  val bucketHelperConfig =
    BucketHelperConfig(imageConfig, welderConfig, proxyConfig, securityFilesConfig, clusterResourcesConfig)
  val bucketHelper =
    new BucketHelper[IO](bucketHelperConfig, mockGoogle2StorageDAO, serviceAccountProvider, blocker)
  val vpcInterp = new VPCInterpreter[IO](Config.vpcInterpreterConfig,
                                         FakeGoogleResourceService,
                                         FakeGoogleComputeService,
                                         new MockComputePollOperation)
  def dataprocInterp(implicit dbRef: DbReference[IO]) =
    new DataprocInterpreter[IO](dataprocInterpreterConfig,
                                bucketHelper,
                                vpcInterp,
                                FakeGoogleDataprocService,
                                FakeGoogleComputeService,
                                MockGoogleDiskService,
                                mockGoogleDirectoryDAO,
                                mockGoogleIamDAO,
                                FakeGoogleResourceService,
                                MockWelderDAO,
                                blocker)
  def gceInterp(implicit dbRef: DbReference[IO]) =
    new GceInterpreter[IO](gceInterpreterConfig,
                           bucketHelper,
                           vpcInterp,
                           FakeGoogleComputeService,
                           MockGoogleDiskService,
                           MockWelderDAO,
                           blocker)
  def runtimeInstances(implicit dbRef: DbReference[IO]) = new RuntimeInstances[IO](dataprocInterp, gceInterp)

  def leoKubernetesService(implicit dbRef: DbReference[IO]): LeoKubernetesServiceInterp[IO] =
    new LeoKubernetesServiceInterp[IO](
      whitelistAuthProvider,
      serviceAccountProvider,
      Config.leoKubernetesConfig,
      QueueFactory.makePublisherQueue()
    )

  def runtimeDnsCache(implicit dbRef: DbReference[IO]) =
    new RuntimeDnsCache[IO](proxyConfig, dbRef, Config.runtimeDnsCacheConfig, blocker)
  def kubernetesDnsCache(implicit dbRef: DbReference[IO]) =
    new KubernetesDnsCache[IO](proxyConfig, dbRef, Config.kubernetesDnsCacheConfig, blocker)

  def proxyService(implicit dbRef: DbReference[IO]) =
    new MockProxyService(proxyConfig,
                         MockJupyterDAO,
                         whitelistAuthProvider,
                         runtimeDnsCache,
                         kubernetesDnsCache,
                         MockGoogleOAuth2Service)
  def statusService(implicit dbRef: DbReference[IO]) =
    new StatusService(new MockSamDAO, dbRef, pollInterval = 1 second)
  val timedUserInfo = defaultUserInfo.copy(tokenExpiresIn = tokenAge)
  val corsSupport = new CorsSupport(contentSecurityPolicy)
  def statusRoutes(statusService: StatusService) = new StatusRoutes(statusService)
  val userInfoDirectives = new MockUserInfoDirectives {
    override val userInfo: UserInfo = defaultUserInfo
  }
  val timedUserInfoDirectives = new MockUserInfoDirectives {
    override val userInfo: UserInfo = timedUserInfo
  }

  def runtimeService(implicit dbRef: DbReference[IO]) = new RuntimeServiceInterp(
    RuntimeServiceConfig(Config.proxyConfig.proxyUrlBase.asString,
                         imageConfig,
                         autoFreezeConfig,
                         dataprocConfig,
                         Config.gceConfig),
    Config.persistentDiskConfig,
    whitelistAuthProvider,
    serviceAccountProvider,
    new MockDockerDAO,
    FakeGoogleStorageInterpreter,
    FakeGoogleComputeService,
    new MockComputePollOperation,
    QueueFactory.makePublisherQueue()
  )

  def httpRoutes(proxyService: ProxyService = null)(implicit dbRef: DbReference[IO]) = new HttpRoutes(
    swaggerConfig,
    statusService,
    proxyService,
    runtimeService,
    MockDiskServiceInterp,
    leoKubernetesService,
    userInfoDirectives,
    contentSecurityPolicy
  )
  def timedHttpRoutes(implicit dbRef: DbReference[IO]) =
    new HttpRoutes(swaggerConfig,
                   statusService,
                   proxyService,
                   runtimeService,
                   MockDiskServiceInterp,
                   leoKubernetesService,
                   timedUserInfoDirectives,
                   contentSecurityPolicy)

  def roundUpToNearestTen(d: Long): Long = (Math.ceil(d / 10.0) * 10).toLong
  val cookieMaxAgeRegex: Regex = "Max-Age=(\\d+);".r

  protected def validateCookie(setCookie: Option[`Set-Cookie`],
                               expectedCookie: HttpCookiePair = tokenCookie,
                               age: Long = tokenAge): Unit = {
    setCookie shouldBe 'defined
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
                                  age: Long = tokenAge): Unit = {
    setCookie shouldBe Symbol("defined")
    setCookie.get.name shouldBe "Set-Cookie"

    // test execution loses some milliseconds, so round Max-Age before validation
    val replaced =
      cookieMaxAgeRegex.replaceAllIn(setCookie.get.value, m => s"Max-Age=${roundUpToNearestTen(m.group(1).toLong)};")

    replaced shouldBe s"${expectedCookie.name}=${expectedCookie.value}; Max-Age=${age.toString}; Path=/; Secure; SameSite=None"
  }

  protected def validateUnsetRawCookie(setCookie: Option[HttpHeader],
                                       expectedCookie: HttpCookiePair = tokenCookie): Unit = {
    setCookie shouldBe Symbol("defined")
    setCookie.get.name shouldBe "Set-Cookie"
    setCookie.get.value shouldBe s"${tokenName}=unset; expires=Thu, 01 Jan 1970 00:00:00 GMT; Path=/; Secure; SameSite=None"
  }
}
