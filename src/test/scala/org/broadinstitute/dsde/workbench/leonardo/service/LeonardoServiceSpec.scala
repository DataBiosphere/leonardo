package org.broadinstitute.dsde.workbench.leonardo.service

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.config.DataprocConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.GoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.DbSingleton
import org.scalatest.{FlatSpec, Matchers}


class LeonardoServiceSpec extends FlatSpec with Matchers {
  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val system = ActorSystem("leonardotest")

  val dataprocConfig = ConfigFactory.load().as[DataprocConfig]("dataproc")

  val gdDAO = new GoogleDataprocDAO(dataprocConfig)
  val service = new LeonardoService(gdDAO, DbSingleton.ref)

  //ToDo: Commenting out this test right now, but we need to figure out how to properly implement integration testing later
  /*"LeonardoService" should "create a cluster" in {
    val clusterRequest = new ClusterRequest("bucketPath", "serviceAccount", Map[String, String]())
    service.createCluster("googleProject", "clusterName", clusterRequest)
    //Once the DELETE and GET APIs are written, we can test the existence of the cluster and then clean up
  }*/

}
