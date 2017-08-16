package org.broadinstitute.dsde.workbench.leonardo.api
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.broadinstitute.dsde.workbench.leonardo.config.SwaggerConfig
import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterRequest, GoogleBucket, GoogleServiceAccount}
import org.scalatest.{FlatSpec, Matchers}
import org.broadinstitute.dsde.workbench.leonardo.model.LeonardoJsonSupport._
import org.broadinstitute.dsde.workbench.leonardo.dao.MockGoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.DbSingleton
import org.broadinstitute.dsde.workbench.leonardo.service.LeonardoService
import spray.json._

class LeoRoutesSpec extends FlatSpec with Matchers with ScalatestRouteTest {

  class TestLeoRoutes(leonardoService: LeonardoService) extends LeoRoutes(leonardoService, SwaggerConfig())

  val mockGoogleDataprocDAO = new MockGoogleDataprocDAO
  val leonardoService = new LeonardoService(mockGoogleDataprocDAO, DbSingleton.ref)
  val leoRoutes = new TestLeoRoutes(leonardoService)

  "LeoRoutes" should "200 on ping" in {
    Get("/api/ping") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
    }
  }

  it should "200 when creating a cluster" in {

    val newCluster = ClusterRequest(GoogleBucket("test-bucket-path"), GoogleServiceAccount("test-service-account"), Map.empty)
    val googleProject = "test-project"
    val clusterName = "test-cluster"

    Put(s"/api/cluster/$googleProject/$clusterName", newCluster.toJson) ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
    }
  }
}
