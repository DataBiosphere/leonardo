package org.broadinstitute.dsde.workbench.leonardo.api
import spray.json.DefaultJsonProtocol._
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.broadinstitute.dsde.workbench.leonardo.config.SwaggerConfig
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterRequest
import org.scalatest.{FlatSpec, Matchers}
import org.broadinstitute.dsde.workbench.leonardo.model.LeonardoJsonSupport._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import spray.json._


class LeoRoutesSpec extends FlatSpec with Matchers with ScalatestRouteTest {

  class TestLeoRoutes
    extends LeoRoutes(SwaggerConfig())
  val leoRoutes = new TestLeoRoutes()

  "LeoRoutes" should "200 on ping" in {
    Get("/api/ping") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
    }
  }

  "Cluster" should "200" in {

    val newCluster = ClusterRequest("", "", ""/*Map[String,String]()*/)
    val googleProject = ""
    val clusterName = ""

    Put(s"/api/cluster/$googleProject/$clusterName", newCluster.toJson) ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
    }
  }
}
