package org.broadinstitute.dsde.workbench.leonardo.service

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.broadinstitute.dsde.workbench.leonardo.TestSupport
import org.scalatest.{FlatSpec, Matchers}
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.config.{DataprocConfig, SwaggerConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.GoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterRequest


class LeonardoServiceSpec extends FlatSpec with Matchers with TestSupport {
  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val system = ActorSystem("leonardotest")
  implicit val materializer = ActorMaterializer()

  val config = ConfigFactory.parseResources("references.conf").withFallback(ConfigFactory.load())
  val dataprocConfig = config.as[DataprocConfig]("dataproc")

  val gdDAO = new GoogleDataprocDAO(dataprocConfig)
  val service = new LeonardoService(gdDAO)

  //ToDo: Commenting out this test right now, but we need to figure out how to properly implement integration testing later
  /*"LeonardoService" should "create a cluster" in {
    val clusterRequest = new ClusterRequest("bucketPath", "serviceAccount", Map[String, String]())
    service.createCluster("googleProject", "clusterName", clusterRequest)
    //Once the DELETE and GET APIs are written, we can test the existence of the cluster and then clean up
  }*/

}
