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

  implicit val system = ActorSystem("samtest")
  implicit val materializer = ActorMaterializer()
  val dataprocConfig = ConfigFactory.load().as[DataprocConfig]("dataproc")
  val gdDAO = new GoogleDataprocDAO(dataprocConfig)

  val service = new LeonardoService(gdDAO)

  "LeonardoService" should "create a cluster" in {
    val clusterRequest = new ClusterRequest("bucketPath", "serviceAccount", Map[String, String]())
    val result = service.createCluster("googleProject", "clusterName", clusterRequest).map{operation => operation.getDone}
    // Unsure what to do here - are we going to create actual clusters to test LeonardoService?
  }

}
