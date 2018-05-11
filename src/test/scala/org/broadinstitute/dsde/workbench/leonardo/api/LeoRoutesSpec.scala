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
import org.scalatest.FlatSpec

import scala.concurrent.duration._
import slick.dbio.DBIO
import spray.json._

class LeoRoutesSpec extends FlatSpec with ScalatestRouteTest with TestLeoRoutes with TestComponent {

  // https://doc.akka.io/docs/akka-http/current/routing-dsl/testkit.html#increase-timeout
  implicit val timeout = RouteTestTimeout(5.seconds dilated)

  private val googleProject = GoogleProject("test-project")
  private val clusterName = ClusterName("test-cluster")

  val invalidUserLeoRoutes = new LeoRoutes(leonardoService, proxyService, statusService, swaggerConfig) with MockUserInfoDirectives {
    override val userInfo: UserInfo =  UserInfo(OAuth2BearerToken(tokenValue), WorkbenchUserId("badUser"), WorkbenchEmail("badUser@example.com"), 0)
  }

  "LeoRoutes" should "200 on ping" in {
    Get("/ping") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
    }
  }

  it should "200 if you're on the whitelist" in isolatedDbTest {
    Get(s"/api/isWhitelisted") ~> timedLeoRoutes.route ~> check {
      validateCookie { header[`Set-Cookie`] }

      status shouldEqual StatusCodes.OK
    }
  }

  it should "401 if you're not on the whitelist" in isolatedDbTest {
    Get(s"/api/isWhitelisted") ~> invalidUserLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.Unauthorized
    }
  }

  it should "200 when creating and getting cluster" in isolatedDbTest {
    val newCluster = ClusterRequest(Map.empty, Some(extensionPath), Some(userScriptPath), None, None, Some(UserJupyterExtensionConfig(Map("abc" ->"def"))))

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
    Get("/api/clusters") ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[List[Cluster]] shouldBe 'empty

      validateCookie { header[`Set-Cookie`] }
    }
  }

  it should "list clusters" in isolatedDbTest {
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

  it should "202 when stopping and starting a cluster" in isolatedDbTest {
    val newCluster = ClusterRequest(Map.empty, None)

    Put(s"/api/cluster/${googleProject.value}/${clusterName.value}", newCluster.toJson) ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK

      validateCookie { header[`Set-Cookie`] }
    }

    // stopping a creating cluster should return 409
    Post(s"/api/cluster/${googleProject.value}/${clusterName.value}/stop") ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.Conflict
    }

    // simulate the cluster transitioning to Running
    dbFutureValue { dataAccess =>
      dataAccess.clusterQuery.getActiveClusterByName(googleProject, clusterName).flatMap {
        case Some(cluster) => dataAccess.clusterQuery.setToRunning(cluster.googleId, IP("1.2.3.4"))
        case None => DBIO.successful(0)
      }
    }

    // stop should now return 202
    Post(s"/api/cluster/${googleProject.value}/${clusterName.value}/stop") ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.Accepted

      validateCookie { header[`Set-Cookie`] }
    }

    // starting a stopping cluster should also return 202
    Post(s"/api/cluster/${googleProject.value}/${clusterName.value}/start") ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.Accepted

      validateCookie { header[`Set-Cookie`] }
    }
  }

  it should "404 when stopping a cluster that does not exist" in {
    Post(s"/api/cluster/nonexistent/bestclustername/stop") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
  }

  Seq(true, false).foreach { stopAfterCreation =>
    it should s"create a cluster with stopAfterCreation = $stopAfterCreation" in isolatedDbTest {
      val request = ClusterRequest(Map.empty, Some(extensionPath), Some(userScriptPath), stopAfterCreation = Some(stopAfterCreation))

      Put(s"/api/cluster/${googleProject.value}/${clusterName.value}", request.toJson) ~>
        timedLeoRoutes.route ~> check {
        status shouldEqual StatusCodes.OK

        validateCookie { header[`Set-Cookie`] }
      }
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
