package org.broadinstitute.dsde.workbench.leonardo.api

import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import akka.http.scaladsl.model.headers.{OAuth2BearerToken, `Set-Cookie`}
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.testkit.TestDuration
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.broadinstitute.dsde.workbench.leonardo.model.LeonardoJsonSupport._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail, WorkbenchUserId}
import org.scalatest.FlatSpec
import slick.dbio.DBIO
import spray.json._
import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments._
import scala.concurrent.duration._

class LeoRoutesSpec extends FlatSpec with ScalatestRouteTest with CommonTestData with TestLeoRoutes with TestComponent {

  // https://doc.akka.io/docs/akka-http/current/routing-dsl/testkit.html#increase-timeout
  implicit val timeout = RouteTestTimeout(5.seconds dilated)

  private val googleProject = GoogleProject("test-project")
  private val googleProject2 = GoogleProject("test-project2")
  private val clusterName = ClusterName("test-cluster")

  val invalidUserLeoRoutes = new LeoRoutes(leonardoService, proxyService, statusService, swaggerConfig) with MockUserInfoDirectives {
    override val userInfo: UserInfo =  UserInfo(OAuth2BearerToken(tokenValue), WorkbenchUserId("badUser"), WorkbenchEmail("badUser@example.com"), 0)
  }

  val defaultClusterRequest = ClusterRequest(Map.empty, None, properties = Map.empty)
  "LeoRoutes" should "200 on ping" in {
    Get("/ping") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
    }
  }

  it should "200 when creating and getting cluster" in isolatedDbTest {
    val newCluster = ClusterRequest(Map.empty, Some(jupyterExtensionUri), Some(jupyterUserScriptUri), None, Map.empty, None, Some(UserJupyterExtensionConfig(Map("abc" ->"def"))))

    forallClusterCreationVersions(clusterName) { (version, clstrName, statusCode) =>
      Put(s"/api/cluster$version/${googleProject.value}/$clstrName", newCluster.toJson) ~> timedLeoRoutes.route ~> check {
        status shouldEqual statusCode

        validateCookie { header[`Set-Cookie`] }
      }
    }

    // GET endpoint has a single version
    Seq(s"${clusterName.value}", s"${clusterName.value}-v2") foreach { cn =>
      Get(s"/api/cluster/${googleProject.value}/$cn") ~> timedLeoRoutes.route ~> check {
        status shouldEqual StatusCodes.OK

        val responseCluster = responseAs[Cluster]
        responseCluster.clusterName.value shouldEqual cn
        responseCluster.serviceAccountInfo.clusterServiceAccount shouldEqual serviceAccountProvider.getClusterServiceAccount(defaultUserInfo, googleProject).futureValue
        responseCluster.serviceAccountInfo.notebookServiceAccount shouldEqual serviceAccountProvider.getNotebookServiceAccount(defaultUserInfo, googleProject).futureValue
        responseCluster.jupyterExtensionUri shouldEqual Some(jupyterExtensionUri)

        validateCookie { header[`Set-Cookie`] }
      }
    }
  }

  it should "404 when getting a nonexistent cluster" in isolatedDbTest {
    Get(s"/api/cluster/nonexistent/cluster") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
  }

  it should "202 when deleting a cluster" in isolatedDbTest {
    val newCluster = defaultClusterRequest

    forallClusterCreationVersions(clusterName) { (version, clstrName, statusCode) =>
      Put(s"/api/cluster$version/${googleProject.value}/$clstrName", newCluster.toJson) ~> timedLeoRoutes.route ~> check {
        status shouldEqual statusCode
      }

      // simulate the cluster transitioning to Running
      dbFutureValue { dataAccess =>
        dataAccess.clusterQuery.getActiveClusterByName(googleProject, ClusterName(clstrName)).flatMap {
          case Some(cluster) => dataAccess.clusterQuery.setToRunning(cluster.id, IP("1.2.3.4"))
          case None => DBIO.successful(0)
        }
      }

      Delete(s"/api/cluster/${googleProject.value}/$clstrName") ~> timedLeoRoutes.route ~> check {
        status shouldEqual StatusCodes.Accepted

        validateCookie { header[`Set-Cookie`] }
      }
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

    Put(s"/api/cluster/v2/${googleProject.value}/$clusterName", newCluster.toJson) ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.Accepted
    }

    // simulate the cluster transitioning to Running
    dbFutureValue { dataAccess =>
      dataAccess.clusterQuery.getActiveClusterByName(googleProject, ClusterName(clusterName)).flatMap {
        case Some(cluster) => dataAccess.clusterQuery.setToRunning(cluster.id, IP("1.2.3.4"))
        case None => DBIO.successful(0)
      }
    }

    Patch(s"/api/cluster/${googleProject.value}/$clusterName", newCluster.toJson) ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.Accepted
    }
  }

  it should "409 when updating a non-running cluster" in isolatedDbTest {
    val newCluster = defaultClusterRequest
    val clusterName = "my-cluster"

    Put(s"/api/cluster/v2/${googleProject.value}/$clusterName", newCluster.toJson) ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.Accepted
    }

    //make sure to leave the cluster in Creating status for this next part

    Patch(s"/api/cluster/${googleProject.value}/$clusterName", newCluster.toJson) ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.Conflict
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
    val newCluster = defaultClusterRequest

    for (i <- 1 to 5) {
      Put(s"/api/cluster/${googleProject.value}/${clusterName.value}-$i", newCluster.toJson) ~> leoRoutes.route ~> check {
        status shouldEqual StatusCodes.OK
      }
    }

    for (i <- 6 to 10) {
      Put(s"/api/cluster/v2/${googleProject.value}/${clusterName.value}-$i", newCluster.toJson) ~> leoRoutes.route ~> check {
        status shouldEqual StatusCodes.Accepted
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
    val newCluster = defaultClusterRequest
    def clusterWithLabels(i: Int) = newCluster.copy(labels = Map(s"label$i" -> s"value$i"))

    for (i <- 1 to 5) {
      Put(s"/api/cluster/${googleProject.value}/${clusterName.value}-$i", clusterWithLabels(i).toJson) ~> leoRoutes.route ~> check {
        status shouldEqual StatusCodes.OK
      }
    }

    for (i <- 6 to 10) {
      Put(s"/api/cluster/v2/${googleProject.value}/${clusterName.value}-$i", clusterWithLabels(i).toJson) ~> leoRoutes.route ~> check {
        status shouldEqual StatusCodes.Accepted
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

  it should "list clusters by project" in isolatedDbTest {
    val newCluster = defaultClusterRequest

    // listClusters should return no clusters initially
    Get(s"/api/clusters/${googleProject.value}") ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
      val responseClusters = responseAs[List[Cluster]]
      responseClusters shouldBe List.empty[Cluster]
    }

    Get(s"/api/clusters/${googleProject2.value}") ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
      val responseClusters = responseAs[List[Cluster]]
      responseClusters shouldBe List.empty[Cluster]
    }


    for (i <- 1 to 5) {
      Put(s"/api/cluster/${googleProject.value}/${clusterName.value}-$i", newCluster.toJson) ~> leoRoutes.route ~> check {
        status shouldEqual StatusCodes.OK
      }
    }

    for (i <- 6 to 10) {
      Put(s"/api/cluster/v2/${googleProject2.value}/${clusterName.value}-$i", newCluster.toJson) ~> leoRoutes.route ~> check {
        status shouldEqual StatusCodes.Accepted
      }
    }

    Get(s"/api/clusters/${googleProject.value}") ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK

      val responseClusters = responseAs[List[Cluster]]
      responseClusters should have size 5
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

    Get(s"/api/clusters/${googleProject2.value}") ~> timedLeoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK

      val responseClusters = responseAs[List[Cluster]]
      responseClusters should have size 5
      responseClusters foreach { cluster =>
        cluster.googleProject shouldEqual googleProject2
        cluster.serviceAccountInfo.clusterServiceAccount shouldEqual clusterServiceAccount(googleProject2)
        cluster.serviceAccountInfo.notebookServiceAccount shouldEqual notebookServiceAccount(googleProject2)
        cluster.labels shouldEqual  Map(
          "clusterName" -> cluster.clusterName.value,
          "creator" -> "user1@example.com",
          "googleProject" -> googleProject2.value) ++ serviceAccountLabels
      }

      validateCookie { header[`Set-Cookie`] }
    }
  }

  it should "202 when stopping and starting a cluster" in isolatedDbTest {
    val newCluster = defaultClusterRequest

    forallClusterCreationVersions(clusterName) { (version, clstrName, statusCode) =>
      Put(s"/api/cluster$version/${googleProject.value}/$clstrName", newCluster.toJson) ~> timedLeoRoutes.route ~> check {
        status shouldEqual statusCode

        validateCookie { header[`Set-Cookie`] }
      }

      // stopping a creating cluster should return 409
      Post(s"/api/cluster/${googleProject.value}/$clstrName/stop") ~> timedLeoRoutes.route ~> check {
        status shouldEqual StatusCodes.Conflict
      }

      // simulate the cluster transitioning to Running
      dbFutureValue { dataAccess =>
        dataAccess.clusterQuery.getActiveClusterByName(googleProject, ClusterName(clstrName)).flatMap {
          case Some(cluster) => dataAccess.clusterQuery.setToRunning(cluster.id, IP("1.2.3.4"))
          case None => DBIO.successful(0)
        }
      }

      // stop should now return 202
      Post(s"/api/cluster/${googleProject.value}/$clstrName/stop") ~> timedLeoRoutes.route ~> check {
        status shouldEqual StatusCodes.Accepted

        validateCookie { header[`Set-Cookie`] }
      }

      // starting a stopping cluster should also return 202
      Post(s"/api/cluster/${googleProject.value}/$clstrName/start") ~> timedLeoRoutes.route ~> check {
        status shouldEqual StatusCodes.Accepted

        validateCookie { header[`Set-Cookie`] }
      }
    }
  }

  it should "404 when stopping a cluster that does not exist" in {
    Post(s"/api/cluster/nonexistent/bestclustername/stop") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
  }

  Seq(true, false).foreach { stopAfterCreation =>
    it should s"create a cluster with stopAfterCreation = $stopAfterCreation" in isolatedDbTest {
      val request = ClusterRequest(Map.empty, Some(jupyterExtensionUri), Some(jupyterUserScriptUri), stopAfterCreation = Some(stopAfterCreation), properties = Map.empty)

      forallClusterCreationVersions(clusterName) { (version, clstrName, statusCode) =>
        Put(s"/api/cluster$version/${googleProject.value}/$clstrName", request.toJson) ~> timedLeoRoutes.route ~> check {
          status shouldEqual statusCode
          validateCookie {
            header[`Set-Cookie`]
          }
        }
      }
    }
  }

  it should s"reject create a cluster if cluster name is invalid" in isolatedDbTest {
    val invalidClusterName = "MyCluster"
    val request = ClusterRequest(Map.empty, Some(jupyterExtensionUri), Some(jupyterUserScriptUri), stopAfterCreation = None, properties = Map.empty)

    val pathPrefixes = List("/api/cluster/", "/api/cluster/v2/")
    pathPrefixes.map{
      prefix =>
        Put(s"$prefix${googleProject.value}/$invalidClusterName", request.toJson) ~> timedLeoRoutes.route ~> check {
          responseAs[String] shouldBe(s"invalid cluster name $invalidClusterName. Only lowercase alphanumeric characters, numbers and dashes are allowed in cluster name")
          status shouldEqual StatusCodes.BadRequest
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

  private def forallClusterCreationVersions(baseClusterName: ClusterName)
                                           (testCode: (String, String, StatusCode) => Any) = {
    val v1Params = ("", baseClusterName.value, StatusCodes.OK)
    val v2Params = ("/v2", s"${baseClusterName.value}-v2", StatusCodes.Accepted)

    Seq(v1Params, v2Params) foreach {
      case (version, clstrName, statusCode) => testCode(version, clstrName, statusCode)
    }
  }
}
