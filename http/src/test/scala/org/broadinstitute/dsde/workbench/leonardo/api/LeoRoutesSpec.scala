package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import java.time.Instant

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.testkit.TestDuration
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import io.circe.syntax._
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.db.{TestComponent, clusterQuery}
import org.broadinstitute.dsde.workbench.leonardo.http.api.RoutesTestJsonSupport._
import org.broadinstitute.dsde.workbench.leonardo.http.service.{CreateRuntimeRequest, ListRuntimeResponse}
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail, WorkbenchUserId}
import org.scalatest.FlatSpec
import slick.dbio.DBIO

import scala.concurrent.duration._

class LeoRoutesSpec
    extends FlatSpec
    with ScalatestRouteTest
    with LeonardoTestSuite
    with TestComponent
    with TestLeoRoutes {
  // https://doc.akka.io/docs/akka-http/current/routing-dsl/testkit.html#increase-timeout
  implicit val timeout = RouteTestTimeout(5.seconds dilated)

  private val googleProject = GoogleProject("test-project")
  private val googleProject2 = GoogleProject("test-project2")
  private val clusterName = RuntimeName("test-cluster")
  val invalidUserLeoRoutes =
    new LeoRoutes(leonardoService, proxyService, statusService, swaggerConfig, contentSecurityPolicy)
    with MockUserInfoDirectives {
      override val userInfo: UserInfo =
        UserInfo(OAuth2BearerToken(tokenValue), WorkbenchUserId("badUser"), WorkbenchEmail("badUser@example.com"), 0)
    }

  val defaultClusterRequest = CreateRuntimeRequest(Map.empty, None, dataprocProperties = Map.empty)
  "LeoRoutes" should "200 on ping" in {
    Get("/ping") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
    }
  }

  it should "200 when creating and getting cluster" in isolatedDbTest {
    val newCluster = CreateRuntimeRequest(
      Map.empty,
      Some(jupyterExtensionUri),
      Some(jupyterUserScriptUri),
      Some(jupyterStartUserScriptUri),
      None,
      Map.empty,
      None,
      false,
      Some(UserJupyterExtensionConfig(Map("abc" -> "def"))),
      None
    )
    Put(s"/api/cluster/v2/${googleProject.value}/${clusterName.asString}", newCluster.asJson) ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.Accepted

      //validateCookie { header[`Set-Cookie`] }
      validateRawCookie(header("Set-Cookie"))
    }

    // GET endpoint has a single version
    Get(s"/api/cluster/${googleProject.value}/${clusterName.asString}") ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK

      val responseCluster = responseAs[GetClusterResponseTest]
      responseCluster.clusterName.asString shouldEqual clusterName.asString
      responseCluster.serviceAccountInfo.clusterServiceAccount shouldEqual serviceAccountProvider
        .getClusterServiceAccount(defaultUserInfo, googleProject)
        .unsafeRunSync()
      responseCluster.serviceAccountInfo.notebookServiceAccount shouldEqual serviceAccountProvider
        .getNotebookServiceAccount(defaultUserInfo, googleProject)
        .unsafeRunSync()
      responseCluster.jupyterExtensionUri shouldEqual Some(jupyterExtensionUri)

      //validateCookie { header[`Set-Cookie`] }
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "404 when getting a nonexistent cluster" in isolatedDbTest {
    Get(s"/api/cluster/nonexistent/cluster") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
  }

  it should "202 when deleting a cluster" in isolatedDbTest {
    val newCluster = defaultClusterRequest

    Put(s"/api/cluster/v2/${googleProject.value}/${clusterName.asString}", newCluster.asJson) ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.Accepted
    }

    // simulate the cluster transitioning to Running
    dbFutureValue {
      clusterQuery.getActiveClusterByName(googleProject, clusterName).flatMap {
        case Some(cluster) => clusterQuery.setToRunning(cluster.id, IP("1.2.3.4"), Instant.now)
        case None          => DBIO.successful(0)
      }
    }

    Delete(s"/api/cluster/${googleProject.value}/${clusterName.asString}") ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.Accepted

      //validateCookie { header[`Set-Cookie`] }
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "404 when deleting a cluster that does not exist" in {
    Delete(s"/api/cluster/nonexistent/bestclustername") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
  }

  it should "202 when resizing a running cluster" in isolatedDbTest {
    val newCluster = defaultClusterRequest
    val clusterName = "my-cluster"

    Put(s"/api/cluster/v2/${googleProject.value}/$clusterName", newCluster.asJson) ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.Accepted
    }

    // simulate the cluster transitioning to Running
    dbFutureValue {
      clusterQuery.getActiveClusterByName(googleProject, RuntimeName(clusterName)).flatMap {
        case Some(cluster) => clusterQuery.setToRunning(cluster.id, IP("1.2.3.4"), Instant.now)
        case None          => DBIO.successful(0)
      }
    }

    Patch(s"/api/cluster/${googleProject.value}/$clusterName", newCluster.asJson) ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.Accepted
    }
  }

  it should "409 when updating a non-running cluster" in isolatedDbTest {
    val newCluster = defaultClusterRequest
    val clusterName = "my-cluster"

    Put(s"/api/cluster/v2/${googleProject.value}/$clusterName", newCluster.asJson) ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.Accepted
    }

    //make sure to leave the cluster in Creating status for this next part

    Patch(s"/api/cluster/${googleProject.value}/$clusterName", newCluster.asJson) ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.Conflict
    }
  }

  it should "200 when listing no clusters" in isolatedDbTest {
    Get("/api/clusters") ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[List[ListRuntimeResponse]] shouldBe 'empty

      //validateCookie { header[`Set-Cookie`] }
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "list clusters" in isolatedDbTest {
    val newCluster = defaultClusterRequest

    for (i <- 1 to 5) {
      Put(s"/api/cluster/${googleProject.value}/${clusterName.asString}-$i", newCluster.asJson) ~> leoRoutes.route ~> check {
        status shouldEqual StatusCodes.OK
      }
    }

    for (i <- 6 to 10) {
      Put(s"/api/cluster/v2/${googleProject.value}/${clusterName.asString}-$i", newCluster.asJson) ~> leoRoutes.route ~> check {
        status shouldEqual StatusCodes.Accepted
      }
    }

    Get("/api/clusters") ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
      val responseClusters = responseAs[List[ListRuntimeResponse]]
      responseClusters should have size 10
      responseClusters foreach { cluster =>
        cluster.googleProject shouldEqual googleProject
        cluster.serviceAccountInfo.clusterServiceAccount shouldEqual clusterServiceAccountFromProject(googleProject)
        cluster.serviceAccountInfo.notebookServiceAccount shouldEqual notebookServiceAccountFromProject(googleProject)
        cluster.labels shouldEqual Map("clusterName" -> cluster.clusterName.asString,
                                       "creator" -> "user1@example.com",
                                       "googleProject" -> googleProject.value,
                                       "tool" -> "Jupyter") ++ serviceAccountLabels
      }

      //validateCookie { header[`Set-Cookie`] }
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "list clusters with labels" in isolatedDbTest {
    val newCluster = defaultClusterRequest
    def clusterWithLabels(i: Int) = newCluster.copy(labels = Map(s"label$i" -> s"value$i"))

    for (i <- 1 to 10) {
      Put(s"/api/cluster/v2/${googleProject.value}/${clusterName.asString}-$i", clusterWithLabels(i).asJson) ~> leoRoutes.route ~> check {
        status shouldEqual StatusCodes.Accepted
      }
    }

    Get("/api/clusters?label6=value6") ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK

      val responseClusters = responseAs[List[ListRuntimeResponse]]
      responseClusters should have size 1

      val cluster = responseClusters.head
      cluster.googleProject shouldEqual googleProject
      cluster.clusterName shouldEqual RuntimeName("test-cluster-6")
      cluster.serviceAccountInfo.clusterServiceAccount shouldEqual clusterServiceAccountFromProject(googleProject)
      cluster.serviceAccountInfo.notebookServiceAccount shouldEqual notebookServiceAccountFromProject(googleProject)
      cluster.labels shouldEqual Map("clusterName" -> "test-cluster-6",
                                     "creator" -> "user1@example.com",
                                     "googleProject" -> googleProject.value,
                                     "tool" -> "Jupyter",
                                     "label6" -> "value6") ++ serviceAccountLabels

      //validateCookie { header[`Set-Cookie`] }
      validateRawCookie(header("Set-Cookie"))
    }

    Get("/api/clusters?_labels=label4%3Dvalue4") ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK

      val responseClusters = responseAs[List[ListRuntimeResponse]]
      responseClusters should have size 1

      val cluster = responseClusters.head
      cluster.googleProject shouldEqual googleProject
      cluster.clusterName shouldEqual RuntimeName("test-cluster-4")
      cluster.serviceAccountInfo.clusterServiceAccount shouldEqual clusterServiceAccountFromProject(googleProject)
      cluster.serviceAccountInfo.notebookServiceAccount shouldEqual notebookServiceAccountFromProject(googleProject)
      cluster.labels shouldEqual Map("clusterName" -> "test-cluster-4",
                                     "creator" -> "user1@example.com",
                                     "googleProject" -> googleProject.value,
                                     "tool" -> "Jupyter",
                                     "label4" -> "value4") ++ serviceAccountLabels

      //validateCookie { header[`Set-Cookie`] }
      validateRawCookie(header("Set-Cookie"))
    }

    Get("/api/clusters?_labels=bad") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.BadRequest
    }
  }

  it should "list clusters by project" in isolatedDbTest {
    val newCluster = defaultClusterRequest

    // listClusters should return no clusters initially
    Get(s"/api/clusters/${googleProject.value}") ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
      val responseClusters = responseAs[List[ListRuntimeResponse]]
      responseClusters shouldBe List.empty[ListRuntimeResponse]
    }

    for (i <- 1 to 10) {
      Put(s"/api/cluster/v2/${googleProject.value}/${clusterName.asString}-$i", newCluster.asJson) ~> leoRoutes.route ~> check {
        status shouldEqual StatusCodes.Accepted
      }
    }

    Get(s"/api/clusters/${googleProject.value}") ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK

      val responseClusters = responseAs[List[ListRuntimeResponse]]
      responseClusters should have size 10
      responseClusters foreach { cluster =>
        cluster.googleProject shouldEqual googleProject
        cluster.serviceAccountInfo.clusterServiceAccount shouldEqual clusterServiceAccountFromProject(googleProject)
        cluster.serviceAccountInfo.notebookServiceAccount shouldEqual notebookServiceAccountFromProject(googleProject)
        cluster.labels shouldEqual Map("clusterName" -> cluster.clusterName.asString,
                                       "creator" -> "user1@example.com",
                                       "googleProject" -> googleProject.value,
                                       "tool" -> "Jupyter") ++ serviceAccountLabels
      }

      //validateCookie { header[`Set-Cookie`] }
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "202 when stopping and starting a cluster" in isolatedDbTest {
    val newCluster = defaultClusterRequest

    Put(s"/api/cluster/v2/${googleProject.value}/${clusterName.asString}", newCluster.asJson) ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.Accepted

      //validateCookie { header[`Set-Cookie`] }
      validateRawCookie(header("Set-Cookie"))
    }

    // stopping a creating cluster should return 409
    Post(s"/api/cluster/${googleProject.value}/${clusterName.asString}/stop") ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.Conflict
    }

    // simulate the cluster transitioning to Running
    dbFutureValue {
      clusterQuery.getActiveClusterByName(googleProject, clusterName).flatMap {
        case Some(cluster) => clusterQuery.setToRunning(cluster.id, IP("1.2.3.4"), Instant.now)
        case None          => DBIO.successful(0)
      }
    }

    // stop should now return 202
    Post(s"/api/cluster/${googleProject.value}/${clusterName.asString}/stop") ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.Accepted

      //validateCookie { header[`Set-Cookie`] }
      validateRawCookie(header("Set-Cookie"))
    }

    // starting a stopping cluster should also return 202
    Post(s"/api/cluster/${googleProject.value}/${clusterName.asString}/start") ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.Accepted

      //validateCookie { header[`Set-Cookie`] }
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "404 when stopping a cluster that does not exist" in {
    Post(s"/api/cluster/nonexistent/bestclustername/stop") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
  }

  Seq(true, false).foreach { stopAfterCreation =>
    it should s"create a cluster with stopAfterCreation = $stopAfterCreation" in isolatedDbTest {
      val request = CreateRuntimeRequest(
        Map.empty,
        Some(jupyterExtensionUri),
        Some(jupyterUserScriptUri),
        Some(jupyterStartUserScriptUri),
        stopAfterCreation = Some(stopAfterCreation),
        dataprocProperties = Map.empty
      )
      Put(s"/api/cluster/v2/${googleProject.value}/${clusterName.asString}", request.asJson) ~> timedLeoRoutes.route ~> check {
        status shouldEqual StatusCodes.Accepted
        //validateCookie { header[`Set-Cookie`] }
        validateRawCookie(header("Set-Cookie"))
      }
    }
  }

  it should s"reject create a cluster if cluster name is invalid" in isolatedDbTest {
    val invalidClusterName = "MyCluster"
    val request = CreateRuntimeRequest(Map.empty,
                                       Some(jupyterExtensionUri),
                                       Some(jupyterUserScriptUri),
                                       Some(jupyterStartUserScriptUri),
                                       stopAfterCreation = None,
                                       dataprocProperties = Map.empty)
    Put(s"/api/cluster/v2/${googleProject.value}/$invalidClusterName", request.asJson) ~> timedLeoRoutes.route ~> check {
      val expectedResponse =
        """invalid cluster name MyCluster. Only lowercase alphanumeric characters, numbers and dashes are allowed in cluster name"""
      responseAs[String] shouldBe expectedResponse
      status shouldEqual StatusCodes.BadRequest
    }
  }

  private def serviceAccountLabels: Map[String, String] =
    (
      clusterServiceAccountFromProject(googleProject).map { sa =>
        Map("clusterServiceAccount" -> sa.value)
      } getOrElse Map.empty
    ) ++ (
      notebookServiceAccountFromProject(googleProject).map { sa =>
        Map("notebookServiceAccount" -> sa.value)
      } getOrElse Map.empty
    )
}

final case class GetClusterResponseTest(
  id: Long,
  clusterName: ClusterName,
  serviceAccountInfo: ServiceAccountInfo,
  jupyterExtensionUri: Option[GcsPath]
)
