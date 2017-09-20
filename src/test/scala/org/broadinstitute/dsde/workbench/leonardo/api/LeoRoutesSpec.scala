package org.broadinstitute.dsde.workbench.leonardo.api
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, ClusterRequest}
import org.broadinstitute.dsde.workbench.leonardo.model.LeonardoJsonSupport._
import org.broadinstitute.dsde.workbench.leonardo.service.ClusterNotFoundException
import org.broadinstitute.dsde.workbench.model.ErrorReport
import org.scalatest.{FlatSpec, Matchers}
import spray.json._

class LeoRoutesSpec extends FlatSpec with Matchers with ScalatestRouteTest with TestLeoRoutes with TestComponent {

  "LeoRoutes" should "200 on ping" in {
    Get("/ping") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
    }
  }

  it should "200 when creating and getting cluster" in isolatedDbTest {
    val newCluster = ClusterRequest("test-bucket-path", "test-service-account", Map.empty, Some(mockGoogleDataprocDAO.extensionUri))
    val googleProject = "test-project"
    val clusterName = "test-cluster"

    Put(s"/api/cluster/$googleProject/$clusterName", newCluster.toJson) ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
    }

    Get(s"/api/cluster/$googleProject/$clusterName") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
      val responseCluster = responseAs[Cluster]
      responseCluster.googleBucket shouldEqual "test-bucket-path"
      responseCluster.googleServiceAccount shouldEqual "test-service-account"
      responseCluster.jupyterExtensionUri shouldEqual Some(mockGoogleDataprocDAO.extensionUri)
    }
  }

  it should "404 when getting a nonexistent cluster" in isolatedDbTest {
    Get(s"/api/cluster/nonexistent/cluster") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
  }

  it should "202 when deleting a cluster" in isolatedDbTest{
    val newCluster = ClusterRequest("test-bucket-path", "test-service-account", Map.empty, None)
    val googleProject = "test-project"
    val clusterName = "test-cluster"

    Put(s"/api/cluster/$googleProject/$clusterName", newCluster.toJson) ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
    }
    Delete(s"/api/cluster/$googleProject/$clusterName") ~> leoRoutes.route ~> check {
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
    val googleProject = "test-project"
    val clusterName = "test-cluster"
    val newCluster = ClusterRequest("test-bucket-path", "test-service-account", Map.empty, None)
    for (i <- 1 to 10) {
      Put(s"/api/cluster/$googleProject/$clusterName-$i", newCluster.toJson) ~> leoRoutes.route ~> check {
        status shouldEqual StatusCodes.OK
      }
    }

    Get("/api/clusters") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
      val responseClusters = responseAs[List[Cluster]]
      responseClusters should have size 10
      responseClusters foreach { cluster =>
        cluster.googleProject shouldEqual "test-project"
        cluster.googleBucket shouldEqual "test-bucket-path"
        cluster.googleServiceAccount shouldEqual "test-service-account"
      }
    }
  }

  it should "list clusters with labels" in isolatedDbTest {
    val googleProject = "test-project"
    val clusterName1 = "test-cluster-1"
    val newCluster1 = ClusterRequest("test-bucket-path", "test-service-account", Map("foo" -> "bar", "baz" -> "biz"), None)
    Put(s"/api/cluster/$googleProject/$clusterName1", newCluster1.toJson) ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
    }
    def validateCluster1(cluster: Cluster) = {
      cluster.googleProject shouldEqual "test-project"
      cluster.clusterName shouldEqual "test-cluster-1"
      cluster.googleBucket shouldEqual "test-bucket-path"
      cluster.googleServiceAccount shouldEqual "test-service-account"
      cluster.labels shouldEqual Map("foo" -> "bar", "baz" -> "biz")
    }

    val clusterName2 = "test-cluster-2"
    val newCluster2 = ClusterRequest("test-bucket-path", "test-service-account", Map("a" -> "b"), None)
    Put(s"/api/cluster/$googleProject/$clusterName2", newCluster2.toJson) ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
    }
    def validateCluster2(cluster: Cluster) = {
      cluster.googleProject shouldEqual "test-project"
      cluster.clusterName shouldEqual "test-cluster-2"
      cluster.googleBucket shouldEqual "test-bucket-path"
      cluster.googleServiceAccount shouldEqual "test-service-account"
      cluster.labels shouldEqual Map("a" -> "b")
    }

    Get("/api/clusters?foo=bar") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
      val responseClusters = responseAs[List[Cluster]]
      responseClusters should have size 1
      validateCluster1(responseClusters.head)
    }

    Get("/api/clusters?foo=bar&baz=biz") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
      val responseClusters = responseAs[List[Cluster]]
      responseClusters should have size 1
      validateCluster1(responseClusters.head)
    }

    Get("/api/clusters?a=b") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
      val responseClusters = responseAs[List[Cluster]]
      responseClusters should have size 1
      validateCluster2(responseClusters.head)
    }

    Get("/api/clusters?foo=bar&baz=biz&a=b&extra=bogus") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[List[Cluster]] should have size 0
    }
  }

  it should "list clusters with swagger-style labels" in isolatedDbTest {
    val googleProject = "test-project"
    val clusterName1 = "test-cluster-1"
    val newCluster1 = ClusterRequest("test-bucket-path", "test-service-account", Map("foo" -> "bar", "baz" -> "biz"), None)
    Put(s"/api/cluster/$googleProject/$clusterName1", newCluster1.toJson) ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
    }
    def validateCluster1(cluster: Cluster) = {
      cluster.googleProject shouldEqual "test-project"
      cluster.clusterName shouldEqual "test-cluster-1"
      cluster.googleBucket shouldEqual "test-bucket-path"
      cluster.googleServiceAccount shouldEqual "test-service-account"
      cluster.labels shouldEqual Map("foo" -> "bar", "baz" -> "biz")
    }

    val clusterName2 = "test-cluster-2"
    val newCluster2 = ClusterRequest("test-bucket-path", "test-service-account", Map("a" -> "b"), None)
    Put(s"/api/cluster/$googleProject/$clusterName2", newCluster2.toJson) ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
    }
    def validateCluster2(cluster: Cluster) = {
      cluster.googleProject shouldEqual "test-project"
      cluster.clusterName shouldEqual "test-cluster-2"
      cluster.googleBucket shouldEqual "test-bucket-path"
      cluster.googleServiceAccount shouldEqual "test-service-account"
      cluster.labels shouldEqual Map("a" -> "b")
    }

    Get("/api/clusters?_labels=foo%3Dbar") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
      val responseClusters = responseAs[List[Cluster]]
      responseClusters should have size 1
      validateCluster1(responseClusters.head)
    }

    Get("/api/clusters?_labels=a%3Db") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
      val responseClusters = responseAs[List[Cluster]]
      responseClusters should have size 1
      validateCluster2(responseClusters.head)
    }

    Get("/api/clusters?_labels=foo%3Dbar,baz%3Dbiz") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
      val responseClusters = responseAs[List[Cluster]]
      responseClusters should have size 1
      validateCluster1(responseClusters.head)
    }

    Get("/api/clusters?foo=bar&_labels=baz%3Dbiz") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
      val responseClusters = responseAs[List[Cluster]]
      responseClusters should have size 1
      validateCluster1(responseClusters.head)
    }

    Get("/api/clusters?foo=bar&_labels=extra%3Dbogus") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[List[Cluster]] should have size 0
    }

    Get("/api/clusters?_labels=foo%3Dbar,baz%3Dbiz,a%3Db,extra%3Dbogus") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[List[Cluster]] should have size 0
    }
  }

}
