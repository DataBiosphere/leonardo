package org.broadinstitute.dsde.workbench.leonardo.api

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.broadinstitute.dsde.workbench.leonardo.model.LeonardoJsonSupport._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.model.{WorkbenchEmail, WorkbenchUserId}
import org.scalatest.{FlatSpec, Matchers}
import spray.json._

class LeoRoutesSpec extends FlatSpec with Matchers with ScalatestRouteTest with TestLeoRoutes with TestComponent {

  private val googleProject = GoogleProject("test-project")
  private val clusterName = ClusterName("test-cluster")

  val invalidUserLeoRoutes = new LeoRoutes(leonardoService, proxyService, statusService, swaggerConfig) with MockUserInfoDirectives {
    override val userInfo: UserInfo =  UserInfo(OAuth2BearerToken("accessToken"), WorkbenchUserId("badUser"), WorkbenchEmail("badUser@example.com"), 0)
  }

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
    val newCluster = ClusterRequest(Map.empty, Some(extensionPath))

    Put(s"/api/cluster/${googleProject.value}/${clusterName.value}", newCluster.toJson) ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
    }

    Get(s"/api/cluster/${googleProject.value}/${clusterName.value}") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
      val responseCluster = responseAs[Cluster]
      responseCluster.serviceAccountInfo.clusterServiceAccount shouldEqual serviceAccountProvider.getClusterServiceAccount(defaultUserInfo.userEmail, googleProject).futureValue
      responseCluster.serviceAccountInfo.notebookServiceAccount shouldEqual serviceAccountProvider.getNotebookServiceAccount(defaultUserInfo.userEmail, googleProject).futureValue
      responseCluster.jupyterExtensionUri shouldEqual Some(extensionPath)
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

    Put(s"/api/cluster/${googleProject.value}/${clusterName.value}", newCluster.toJson) ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
    }
    Delete(s"/api/cluster/${googleProject.value}/${clusterName.value}") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.Accepted
    }
  }

  it should "404 when deleting a cluster that does not exist" in {
    Delete(s"/api/cluster/nonexistent/bestclustername") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
  }

  it should "200 when listing no clusters" in isolatedDbTest {
    Get("/api/clusters") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[List[Cluster]] shouldBe 'empty
    }
  }

  it should "list clusters" in isolatedDbTest {
    val newCluster = ClusterRequest(Map.empty, None)
    for (i <- 1 to 10) {
      Put(s"/api/cluster/${googleProject.value}/${clusterName.value}-$i", newCluster.toJson) ~> leoRoutes.route ~> check {
        status shouldEqual StatusCodes.OK
      }
    }

    Get("/api/clusters") ~> leoRoutes.route ~> check {
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
    }
  }

  it should "list clusters with labels" in isolatedDbTest {
    val newCluster = ClusterRequest(Map.empty, None)
    for (i <- 1 to 10) {
      Put(s"/api/cluster/${googleProject.value}/${clusterName.value}-$i", newCluster.copy(labels = Map(s"label$i" -> s"value$i")).toJson) ~> leoRoutes.route ~> check {
        status shouldEqual StatusCodes.OK
      }
    }

    Get("/api/clusters?label6=value6") ~> leoRoutes.route ~> check {
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
    }

    Get("/api/clusters?_labels=label4%3Dvalue4") ~> leoRoutes.route ~> check {
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
    }

    Get("/api/clusters?_labels=bad") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.BadRequest
    }
  }

  it should "202 when stopping and starting a cluster" in isolatedDbTest {
    val newCluster = ClusterRequest(Map.empty, None)

    Put(s"/api/cluster/${googleProject.value}/${clusterName.value}", newCluster.toJson) ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
    }
    Post(s"/api/cluster/${googleProject.value}/${clusterName.value}/stop") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.Accepted
    }
    Post(s"/api/cluster/${googleProject.value}/${clusterName.value}/start") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.Accepted
    }
  }

  it should "404 when stopping a cluster that does not exist" in {
    Post(s"/api/cluster/nonexistent/bestclustername/stop") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.NotFound
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
