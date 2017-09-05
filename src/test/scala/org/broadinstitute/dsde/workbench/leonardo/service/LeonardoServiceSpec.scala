package org.broadinstitute.dsde.workbench.leonardo.service

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.config.DataprocConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.MockGoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterRequest
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

class LeonardoServiceSpec extends TestKit(ActorSystem("leonardotest")) with FlatSpecLike with Matchers with BeforeAndAfterAll with TestComponent with ScalaFutures with OptionValues {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  val dataprocConfig = ConfigFactory.load().as[DataprocConfig]("dataproc")

  val gdDAO = new MockGoogleDataprocDAO
  val leo = new LeonardoService(gdDAO, DbSingleton.ref)

  "LeonardoService" should "create and get a cluster" in isolatedDbTest {
    val clusterRequest = ClusterRequest("bucketPath", "serviceAccount", Map[String, String]())

    val clusterCreateResponse = leo.createCluster("googleProject", "clusterName", clusterRequest).futureValue
    val clusterGetResponse = leo.getClusterDetails("googleProject", "clusterName").futureValue

    clusterCreateResponse shouldEqual clusterGetResponse
    clusterCreateResponse.googleBucket shouldEqual "bucketPath"
    clusterCreateResponse.googleServiceAccount shouldEqual "serviceAccount"
  }

  it should "throw ClusterNotFoundException for nonexistent clusters" in isolatedDbTest {
    whenReady( leo.getClusterDetails("nonexistent", "cluster").failed ) { exc =>
      exc shouldBe a [ClusterNotFoundException]
    }
  }

  "LeonardoService" should "throw ClusterAlreadyExistsException when creating a cluster with same name and project as an existing cluster" in isolatedDbTest {
    val clusterRequest = ClusterRequest("bucketPath", "serviceAccount", Map[String, String]())
    val clusterCreateResponse = leo.createCluster("googleProject1", "clusterName1", clusterRequest).futureValue

    whenReady( leo.createCluster("googleProject1", "clusterName1", clusterRequest)) { exc =>
      exc shouldBe a [ClusterAlreadyExistsException]
    }
  }

  "LeonardoService" should "delete a cluster" in isolatedDbTest {
    val clusterRequest = ClusterRequest("bucketPath", "serviceAccount", Map[String, String]())

    val clusterCreateResponse = leo.createCluster("googleProject", "clusterName", clusterRequest).futureValue
    val clusterGetResponse = leo.deleteCluster("googleProject", "clusterName").futureValue
    clusterGetResponse shouldEqual 1
  }

  "LeonardoService" should "throw ClusterNotFoundException when deleting non existent clusters" in isolatedDbTest {

    whenReady( leo.deleteCluster("nonexistent", "cluster").failed ) { exc =>
      exc shouldBe a [ClusterNotFoundException]
    }
  }


}