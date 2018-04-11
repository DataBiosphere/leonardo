package org.broadinstitute.dsde.workbench.leonardo.api

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.{HttpCookiePair, OAuth2BearerToken, `Set-Cookie`}
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.testkit.TestDuration
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.broadinstitute.dsde.workbench.leonardo.model.LeonardoJsonSupport._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.model.{WorkbenchEmail, WorkbenchUserId}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._
import spray.json._

class LeoRoutesSpec extends FlatSpec with Matchers with ScalatestRouteTest with TestLeoRoutes with TestComponent {

  // https://doc.akka.io/docs/akka-http/current/routing-dsl/testkit.html#increase-timeout
  implicit val timeout = RouteTestTimeout(5.seconds dilated)

  private val googleProject = GoogleProject("test-project")
  private val clusterName = ClusterName("test-cluster")
  private val tokenName = "LeoToken"
  private val tokenValue = "accessToken"
  private val tokenCookie = HttpCookiePair(tokenName, tokenValue)

  val invalidUserLeoRoutes = new LeoRoutes(leonardoService, proxyService, statusService, swaggerConfig) with MockUserInfoDirectives {
    override val userInfo: UserInfo =  UserInfo(OAuth2BearerToken(tokenValue), WorkbenchUserId("badUser"), WorkbenchEmail("badUser@example.com"), 0)
  }

  private def validateCookie(setCookie: Option[`Set-Cookie`],
                             expectedCookie: HttpCookiePair = tokenCookie,
                             age: Long = tokenAge / 1000): Unit = {
    def roundUpToNearestTen(d: Long) = Math.ceil(d / 10.0) * 10

    setCookie shouldBe 'defined
    val cookie = setCookie.get.cookie
    cookie.name shouldBe expectedCookie.name
    cookie.value shouldBe expectedCookie.value
    cookie.secure shouldBe true
    cookie.maxAge.map(roundUpToNearestTen) shouldBe Some(age) // test execution loses some milliseconds
    cookie.domain shouldBe None
    cookie.path shouldBe Some("/")
  }

  private def isTokenCached(cookie: HttpCookiePair = tokenCookie): Boolean =
    proxyService.googleTokenCache.asMap().containsKey(cookie.value)

  "LeoRoutes" should "200 on ping" in {
    Get("/ping") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
    }
  }

  it should "200 if you're on the whitelist" in isolatedDbTest {
    Get(s"/api/isWhitelisted") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
    }
  }

  it should "401 if you're not on the whitelist" in isolatedDbTest {
    Get(s"/api/isWhitelisted") ~> invalidUserLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.Unauthorized
    }
  }

  it should "200 when creating and getting cluster" in isolatedDbTest {
    isTokenCached() shouldBe false
    
    val newCluster = ClusterRequest(Map.empty, Some(extensionPath), Some(userScriptPath))

    Put(s"/api/cluster/${googleProject.value}/${clusterName.value}", newCluster.toJson) ~>
      timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK

      validateCookie { header[`Set-Cookie`] }
    }

    Get(s"/api/cluster/${googleProject.value}/${clusterName.value}") ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK

      val responseCluster = responseAs[Cluster]
      responseCluster.serviceAccountInfo.clusterServiceAccount shouldEqual serviceAccountProvider.getClusterServiceAccount(defaultUserInfo, googleProject).futureValue
      responseCluster.serviceAccountInfo.notebookServiceAccount shouldEqual serviceAccountProvider.getNotebookServiceAccount(defaultUserInfo, googleProject).futureValue
      responseCluster.jupyterExtensionUri shouldEqual Some(extensionPath)

      validateCookie { header[`Set-Cookie`] }
    }
  }

  it should "404 when getting a nonexistent cluster" in isolatedDbTest {
    Get(s"/api/cluster/nonexistent/cluster") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
  }

  it should "404 when getting a cluster as a non-white-listed user" in isolatedDbTest {
    val newCluster = ClusterRequest(Map.empty, None)

    Put(s"/api/cluster/${googleProject.value}/notyourcluster", newCluster.toJson) ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
    }

    Get(s"/api/cluster/${googleProject.value}/notyourcluster") ~> invalidUserLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
  }

  it should "202 when deleting a cluster" in isolatedDbTest{
    isTokenCached() shouldBe false

    val newCluster = ClusterRequest(Map.empty, None)

    Put(s"/api/cluster/${googleProject.value}/${clusterName.value}", newCluster.toJson) ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
    }
    Delete(s"/api/cluster/${googleProject.value}/${clusterName.value}") ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.Accepted

      validateCookie { header[`Set-Cookie`] }
    }
  }

  it should "404 when deleting a cluster that does not exist" in {
    Delete(s"/api/cluster/nonexistent/bestclustername") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
  }

  it should "200 when listing no clusters" in isolatedDbTest {
    isTokenCached() shouldBe false

    Get("/api/clusters") ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[List[Cluster]] shouldBe 'empty

      validateCookie { header[`Set-Cookie`] }
    }
  }

  it should "list clusters" in isolatedDbTest {
    isTokenCached() shouldBe false

    val newCluster = ClusterRequest(Map.empty, None)
    for (i <- 1 to 10) {
      Put(s"/api/cluster/${googleProject.value}/${clusterName.value}-$i", newCluster.toJson) ~> leoRoutes.route ~> check {
        status shouldEqual StatusCodes.OK
      }
    }

    Get("/api/clusters") ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK

      val responseClusters = responseAs[List[Cluster]]
      responseClusters should have size 10
      responseClusters foreach { cluster =>
        cluster.googleProject shouldEqual googleProject
        cluster.serviceAccountInfo.clusterServiceAccount shouldEqual clusterServiceAccount(googleProject)
        cluster.serviceAccountInfo.notebookServiceAccount shouldEqual notebookServiceAccount(googleProject)
        cluster.labels shouldEqual  Map(
          "clusterName" -> cluster.clusterName.value,
          "creator" -> "user1@example.com",
          "googleProject" -> googleProject.value) ++ serviceAccountLabels
      }

      validateCookie { header[`Set-Cookie`] }
    }
  }

  it should "list clusters with labels" in isolatedDbTest {
    isTokenCached() shouldBe false

    val newCluster = ClusterRequest(Map.empty, None)
    for (i <- 1 to 10) {
      Put(s"/api/cluster/${googleProject.value}/${clusterName.value}-$i", newCluster.copy(labels = Map(s"label$i" -> s"value$i")).toJson) ~> leoRoutes.route ~> check {
        status shouldEqual StatusCodes.OK
      }
    }

    Get("/api/clusters?label6=value6") ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK

      val responseClusters = responseAs[List[Cluster]]
      responseClusters should have size 1

      val cluster = responseClusters.head
      cluster.googleProject shouldEqual googleProject
      cluster.clusterName shouldEqual ClusterName("test-cluster-6")
      cluster.serviceAccountInfo.clusterServiceAccount shouldEqual clusterServiceAccount(googleProject)
      cluster.serviceAccountInfo.notebookServiceAccount shouldEqual notebookServiceAccount(googleProject)
      cluster.labels shouldEqual Map(
        "clusterName" -> "test-cluster-6",
        "creator" -> "user1@example.com",
        "googleProject" -> googleProject.value,
        "label6" -> "value6") ++ serviceAccountLabels

      validateCookie { header[`Set-Cookie`] }
    }

    Get("/api/clusters?_labels=label4%3Dvalue4") ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK

      val responseClusters = responseAs[List[Cluster]]
      responseClusters should have size 1

      val cluster = responseClusters.head
      cluster.googleProject shouldEqual googleProject
      cluster.clusterName shouldEqual ClusterName("test-cluster-4")
      cluster.serviceAccountInfo.clusterServiceAccount shouldEqual clusterServiceAccount(googleProject)
      cluster.serviceAccountInfo.notebookServiceAccount shouldEqual notebookServiceAccount(googleProject)
      cluster.labels shouldEqual Map(
        "clusterName" -> "test-cluster-4",
        "creator" -> "user1@example.com",
        "googleProject" -> googleProject.value,
        "label4" -> "value4") ++ serviceAccountLabels

      validateCookie { header[`Set-Cookie`] }
    }

    Get("/api/clusters?_labels=bad") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.BadRequest
    }
  }

  private def serviceAccountLabels: Map[String, String] = {
    (
      clusterServiceAccount(googleProject).map { sa => Map("clusterServiceAccount" -> sa.value) } getOrElse Map.empty
    ) ++ (
      notebookServiceAccount(googleProject).map { sa => Map("notebookServiceAccount" -> sa.value) } getOrElse Map.empty
    )
  }
}
