package org.broadinstitute.dsde.workbench.leonardo.api

import java.io.ByteArrayInputStream

import akka.http.scaladsl.model.HttpHeader
import akka.http.scaladsl.model.headers.{HttpCookiePair, `Set-Cookie`}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.broadinstitute.dsde.workbench.google.GoogleStorageDAO
import org.broadinstitute.dsde.workbench.google.mock.{MockGoogleDirectoryDAO, MockGoogleIamDAO, MockGoogleProjectDAO, MockGoogleStorageDAO}
import org.broadinstitute.dsde.workbench.leonardo.dao.{MockDockerDAO, MockWelderDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.DbSingleton
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache
import org.broadinstitute.dsde.workbench.leonardo.monitor.NoopActor
import org.broadinstitute.dsde.workbench.leonardo.service.{LeonardoService, MockProxyService, StatusService}
import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, LeonardoTestSuite}
import org.broadinstitute.dsde.workbench.leonardo.util.{BucketHelper, ClusterHelper, QueueFactory}
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.scalactic.source.Position
import org.scalatest.Matchers
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Span}
import CommonTestData._

import scala.concurrent.duration._
import scala.util.matching.Regex

trait TestLeoRoutes { this: ScalatestRouteTest with Matchers with ScalaFutures with LeonardoTestSuite =>
  implicit val db = DbSingleton.dbRef
  // Set up the mock directoryDAO to have the Google group used to grant permission to users
  // to pull the custom dataproc image
  val mockGoogleDirectoryDAO = {
    val mockGoogleDirectoryDAOPatience = PatienceConfig(timeout = scaled(Span(30, Seconds)))
    val dao = new MockGoogleDirectoryDAO()
    dao
      .createGroup(dataprocImageProjectGroupName, dataprocImageProjectGroupEmail, Option(dao.lockedDownGroupSettings))
      .futureValue(mockGoogleDirectoryDAOPatience, Position.here)
    dao
  }

  val mockGoogleIamDAO = new MockGoogleIamDAO
  val mockGoogleStorageDAO = new MockGoogleStorageDAO
  val mockGoogleProjectDAO = new MockGoogleProjectDAO
  mockGoogleStorageDAO.buckets += jupyterExtensionUri.bucketName -> Set(
    (jupyterExtensionUri.objectName, new ByteArrayInputStream("foo".getBytes()))
  )
  val mockPetGoogleStorageDAO: String => GoogleStorageDAO = _ => {
    val petMock = new MockGoogleStorageDAO
    petMock.buckets += jupyterUserScriptBucketName -> Set(
      (jupyterUserScriptObjectName, new ByteArrayInputStream("foo".getBytes()))
    )
    petMock.buckets += jupyterStartUserScriptBucketName -> Set(
      (jupyterStartUserScriptObjectName, new ByteArrayInputStream("foo".getBytes()))
    )
    petMock.buckets += jupyterExtensionUri.bucketName -> Set(
      (jupyterExtensionUri.objectName, new ByteArrayInputStream("foo".getBytes()))
    )
    petMock
  }
  // Route tests don't currently do cluster monitoring, so use NoopActor
  val clusterMonitorSupervisor = system.actorOf(NoopActor.props)
  val bucketHelper =
    new BucketHelper(mockGoogleComputeDAO, mockGoogleStorageDAO, mockGoogle2StorageDAO, serviceAccountProvider)(cs)
  val clusterHelper =
    new ClusterHelper(DbSingleton.dbRef,
                      dataprocConfig,
                      imageConfig,
                      googleGroupsConfig,
                      proxyConfig,
                      clusterResourcesConfig,
                      clusterFilesConfig,
                      monitorConfig,
                      welderConfig,
                      bucketHelper,
                      mockGoogleDataprocDAO,
                      mockGoogleComputeDAO,
                      mockGoogleDirectoryDAO,
                      mockGoogleIamDAO,
                      mockGoogleProjectDAO, MockWelderDAO,
                      blocker)

  val leonardoService = new LeonardoService(
    dataprocConfig,
    imageConfig,
    MockWelderDAO,
    clusterDefaultsConfig,
    proxyConfig,
    swaggerConfig,
    autoFreezeConfig,
    welderConfig,
    mockPetGoogleStorageDAO,
    whitelistAuthProvider,
    serviceAccountProvider,
    bucketHelper,
    clusterHelper,
    new MockDockerDAO,
    QueueFactory.makePublisherQueue()
  )(executor, system, loggerIO, cs, metrics, DbSingleton.dbRef, timer)

  val clusterDnsCache = new ClusterDnsCache(proxyConfig, DbSingleton.dbRef, dnsCacheConfig, blocker)

  val proxyService =
    new MockProxyService(proxyConfig, mockGoogleDataprocDAO, whitelistAuthProvider, clusterDnsCache)
  val statusService =
    new StatusService(mockGoogleDataprocDAO, mockSamDAO, DbSingleton.dbRef, dataprocConfig, pollInterval = 1.second)
  val timedUserInfo = defaultUserInfo.copy(tokenExpiresIn = tokenAge)
  val leoRoutes = new LeoRoutes(leonardoService, proxyService, statusService, swaggerConfig, contentSecurityPolicy)
  with MockUserInfoDirectives {
    override val userInfo: UserInfo = defaultUserInfo
  }
  val timedLeoRoutes = new LeoRoutes(leonardoService, proxyService, statusService, swaggerConfig, contentSecurityPolicy)
  with MockUserInfoDirectives {
    override val userInfo: UserInfo = timedUserInfo
  }

  def roundUpToNearestTen(d: Long): Long = (Math.ceil(d / 10.0) * 10).toLong
  val cookieMaxAgeRegex: Regex = "Max-Age=(\\d+);".r

  private[api] def validateCookie(setCookie: Option[`Set-Cookie`],
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
  private[api] def validateRawCookie(setCookie: Option[HttpHeader],
                                     expectedCookie: HttpCookiePair = tokenCookie,
                                     age: Long = tokenAge): Unit = {
    setCookie shouldBe 'defined
    setCookie.get.name shouldBe "Set-Cookie"

    // test execution loses some milliseconds, so round Max-Age before validation
    val replaced =
      cookieMaxAgeRegex.replaceAllIn(setCookie.get.value, m => s"Max-Age=${roundUpToNearestTen(m.group(1).toLong)};")

    replaced shouldBe s"${expectedCookie.name}=${expectedCookie.value}; Max-Age=${age.toString}; Path=/; Secure; SameSite=None"
  }
}
