package org.broadinstitute.dsde.workbench.leonardo
package dao

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.benmanes.caffeine.cache.Caffeine
import io.circe.parser._
import org.broadinstitute.dsde.workbench.azure.{AzureCloudContext, ManagedResourceGroupName, SubscriptionId, TenantId}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.dao.HttpCromwellDAO.statusDecoder
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.broadinstitute.dsde.workbench.leonardo.dns.{RuntimeDnsCache, RuntimeDnsCacheKey}
import org.broadinstitute.dsde.workbench.leonardo.http.ctxConversion
import org.http4s._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.client.Client
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.slf4j.Slf4jLogger
import scalacache.Cache
import scalacache.caffeine.CaffeineCache


class HttpCromwellDAOSpec extends AnyFlatSpec with Matchers with LeonardoTestSuite with TestComponent {
  val underlyingRuntimeDnsCache =
    Caffeine.newBuilder().maximumSize(10000L).build[RuntimeDnsCacheKey, scalacache.Entry[HostStatus]]()
  val runtimeDnsCaffeineCache: Cache[IO, RuntimeDnsCacheKey, HostStatus] =
    CaffeineCache[IO, RuntimeDnsCacheKey, HostStatus](underlyingRuntimeDnsCache)

  implicit def unsafeLogger = Slf4jLogger.getLogger[IO]

  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global


  override def beforeAll(): Unit = {
    super.beforeAll()
    runtimeDnsCaffeineCache.removeAll.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  override def afterAll(): Unit = {
    runtimeDnsCaffeineCache.close.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    super.afterAll()
  }

  "HttpCromwellDAO" should "decode cromwell status endpoint response successfully" in {
    val response =
      """
        |{
        |  "ok": true
        |}
      """.stripMargin

    val res = for {
      json <- parse(response)
      resp <- json.as[CromwellStatusCheckResponse]
    } yield resp

    res shouldBe (Right(CromwellStatusCheckResponse(true)))
  }

  "HttpCromwellDAO.getStatus" should "return false if status is not ok" in {
    val clusterDnsCache =
      new RuntimeDnsCache(proxyConfig, testDbRef, hostToIpMapping, runtimeDnsCaffeineCache)
    val azureCloudContext =
      AzureCloudContext(TenantId("testTenant"), SubscriptionId("testSubscription"), ManagedResourceGroupName("testMrg"))

    val response =
      """
        |{
        |  "ok": false
        |}
      """.stripMargin

    val okSam = Client.fromHttpApp[IO](
      HttpApp(_ => IO.fromEither(parse(response)).flatMap(r => IO(Response(status = Status.Ok).withEntity(r))))
    )

    val cromwellDAO = new HttpCromwellDAO(clusterDnsCache, okSam, new MockSamDAO())
    val res = cromwellDAO.getStatus(CloudContext.Azure(azureCloudContext), RuntimeName("rt"))
    res.map(r => r shouldBe CromwellStatusCheckResponse(false)).unsafeRunSync()
  }

  "HttpCromwellDAO.getStatus" should "return true if status is ok" in {
    val clusterDnsCache =
      new RuntimeDnsCache(proxyConfig, testDbRef, hostToIpMapping, runtimeDnsCaffeineCache)
    val azureCloudContext =
      AzureCloudContext(TenantId("testTenant"), SubscriptionId("testSubscription"), ManagedResourceGroupName("testMrg"))

    val response =
      """
        |{
        |  "ok": true
        |}
      """.stripMargin

    val okSam = Client.fromHttpApp[IO](
      HttpApp(_ => IO.fromEither(parse(response)).flatMap(r => IO(Response(status = Status.Ok).withEntity(r))))
    )

    val cromwellDAO = new HttpCromwellDAO(clusterDnsCache, okSam, new MockSamDAO())
    val res = cromwellDAO.getStatus(CloudContext.Azure(azureCloudContext), RuntimeName("rt"))
    res.map(r => r shouldBe CromwellStatusCheckResponse(true)).unsafeRunSync()
  }

