package org.broadinstitute.dsde.workbench.leonardo.api

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.broadinstitute.dsde.workbench.google.gcs.GcsBucketName
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.broadinstitute.dsde.workbench.leonardo.model.LeonardoJsonSupport._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.scalatest.{FlatSpec, Matchers}
import spray.json._

class LeoRoutesSpec extends FlatSpec with Matchers with ScalatestRouteTest with TestLeoRoutes with TestComponent {

  private val googleProject = GoogleProject("test-project")
  private val clusterName = ClusterName("test-cluster")
  private val bucketPath = GcsBucketName("test-bucket-path")
  private val googleServiceAccount = GoogleServiceAccount("test-service-account")


  "LeoRoutes" should "200 on ping" in {
    Get("/ping") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
    }
  }

  it should "200 when creating and getting cluster" in isolatedDbTest {
    val newCluster = ClusterRequest(bucketPath, googleServiceAccount, Map.empty, Some(mockGoogleDataprocDAO.extensionPath))

    Put(s"/api/cluster/${googleProject.string}/${clusterName.string}", newCluster.toJson) ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
    }

    Get(s"/api/cluster/${googleProject.string}/${clusterName.string}") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
      val responseCluster = responseAs[Cluster]
      responseCluster.googleBucket shouldEqual bucketPath
      responseCluster.googleServiceAccount shouldEqual googleServiceAccount
      responseCluster.jupyterExtensionUri shouldEqual Some(mockGoogleDataprocDAO.extensionPath)
    }
  }

  it should "404 when getting a nonexistent cluster" in isolatedDbTest {
    Get(s"/api/cluster/nonexistent/cluster") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
  }

  it should "202 when deleting a cluster" in isolatedDbTest{
    val newCluster = ClusterRequest(bucketPath, googleServiceAccount, Map.empty, None)

    Put(s"/api/cluster/${googleProject.string}/${clusterName.string}", newCluster.toJson) ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
    }
    Delete(s"/api/cluster/${googleProject.string}/${clusterName.string}") ~> leoRoutes.route ~> check {
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
    val newCluster = ClusterRequest(bucketPath, googleServiceAccount, Map.empty, None)
    for (i <- 1 to 10) {
      Put(s"/api/cluster/${googleProject.string}/${clusterName.string}-$i", newCluster.toJson) ~> leoRoutes.route ~> check {
        status shouldEqual StatusCodes.OK
      }
    }

    Get("/api/clusters") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
      val responseClusters = responseAs[List[Cluster]]
      responseClusters should have size 10
      responseClusters foreach { cluster =>
        cluster.googleProject shouldEqual googleProject
        cluster.googleBucket shouldEqual bucketPath
        cluster.googleServiceAccount shouldEqual googleServiceAccount
        cluster.labels shouldEqual Map(
          "googleBucket" -> bucketPath.name,
          "clusterName" -> cluster.clusterName.string,
          "googleProject" -> googleProject.string,
          "serviceAccount" -> googleServiceAccount.string)
      }
    }
  }

  it should "list clusters with labels" in isolatedDbTest {
    val newCluster = ClusterRequest(bucketPath, googleServiceAccount, Map.empty, None)
    for (i <- 1 to 10) {
      Put(s"/api/cluster/${googleProject.string}/${clusterName.string}-$i", newCluster.copy(labels = Map(s"label$i" -> s"value$i")).toJson) ~> leoRoutes.route ~> check {
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
      cluster.googleBucket shouldEqual bucketPath
      cluster.googleServiceAccount shouldEqual googleServiceAccount
      cluster.labels shouldEqual Map(
        "googleBucket" -> bucketPath.name,
        "clusterName" -> "test-cluster-6",
        "googleProject" -> googleProject.string,
        "serviceAccount" -> googleServiceAccount.string,
        "label6" -> "value6")
    }

    Get("/api/clusters?_labels=label4%3Dvalue4") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
      val responseClusters = responseAs[List[Cluster]]
      responseClusters should have size 1
      val cluster = responseClusters.head
      cluster.googleProject shouldEqual googleProject
      cluster.clusterName shouldEqual ClusterName("test-cluster-4")
      cluster.googleBucket shouldEqual bucketPath
      cluster.googleServiceAccount shouldEqual googleServiceAccount
      cluster.labels shouldEqual Map(
        "googleBucket" -> bucketPath.name,
        "clusterName" -> "test-cluster-4",
        "googleProject" -> googleProject.string,
        "serviceAccount" -> googleServiceAccount.string,
        "label4" -> "value4")
    }

    Get("/api/clusters?_labels=bad") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.BadRequest
    }
  }

}
