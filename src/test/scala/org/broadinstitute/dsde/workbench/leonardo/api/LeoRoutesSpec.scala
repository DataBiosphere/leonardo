package org.broadinstitute.dsde.workbench.leonardo.api
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterRequest
import org.broadinstitute.dsde.workbench.leonardo.model.LeonardoJsonSupport._
import org.scalatest.{FlatSpec, Matchers}
import spray.json._

class LeoRoutesSpec extends FlatSpec with Matchers with ScalatestRouteTest with TestLeoRoutes with TestComponent {

  "LeoRoutes" should "200 on ping" in {
    Get("/api/ping") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
    }
  }

  it should "200 when creating a cluster" in isolatedDbTest {
    val newCluster = ClusterRequest("test-bucket-path", "test-service-account", Map[String,String]())
    val googleProject = "test-project"
    val clusterName = "test-cluster"

    Put(s"/api/cluster/$googleProject/$clusterName", newCluster.toJson) ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
    }
  }
}
