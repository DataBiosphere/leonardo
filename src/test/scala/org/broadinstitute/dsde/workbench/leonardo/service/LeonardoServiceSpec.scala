package org.broadinstitute.dsde.workbench.leonardo.service

import java.util.UUID

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.config.DataprocConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.MockGoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterInitValues, ClusterRequest}
import org.broadinstitute.dsde.workbench.leonardo.model.LeonardoJsonSupport._
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import spray.json._

class LeonardoServiceSpec extends TestKit(ActorSystem("leonardotest")) with FlatSpecLike with Matchers with BeforeAndAfterAll with TestComponent with ScalaFutures with OptionValues {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  private val dataprocConfig = ConfigFactory.load().as[DataprocConfig]("dataproc")

  private val gdDAO = new MockGoogleDataprocDAO(dataprocConfig)
  private val leo = new LeonardoService(dataprocConfig, gdDAO, DbSingleton.ref)

  private val bucketPath = "bucket-path"
  private val serviceAccount = "service-account"
  private val googleProject = "test-google-project"
  private val testClusterRequest = ClusterRequest(bucketPath, serviceAccount, Map.empty)

  val initFiles = Array( dataprocConfig.clusterDockerComposeName, dataprocConfig.initActionsScriptName, dataprocConfig.jupyterServerCrtName,
    dataprocConfig.jupyterServerKeyName, dataprocConfig.jupyterRootCaPemName, dataprocConfig.jupyterProxySiteConfName)

  "LeonardoService" should "create a cluster" in isolatedDbTest {
    // set unique names for our cluster
    val clusterName = s"cluster-${UUID.randomUUID.toString}"
    val googleProject = s"project-${UUID.randomUUID.toString}"

    // create the cluster
    val clusterCreateResponse = leo.createCluster(googleProject, clusterName, testClusterRequest).futureValue

    // check the create response has the correct info
    clusterCreateResponse.googleBucket shouldEqual bucketPath
    clusterCreateResponse.googleServiceAccount shouldEqual serviceAccount

    // check the firewall rule was created for the project
    assert(gdDAO.firewallRules.exists(_  == (googleProject, dataprocConfig.clusterFirewallRuleName)))

    // check the bucket was created for the cluster
    assert(gdDAO.buckets.contains(clusterCreateResponse.googleBucket))

    // check the init files were added to the bucket
    initFiles.map(initFile => assert(gdDAO.bucketObjects.exists(_ == (clusterCreateResponse.googleBucket, initFile))))
  }

  "LeonardoService" should "create and get a cluster" in isolatedDbTest {
    // set unique names for our cluster
    val clusterName = s"cluster-${UUID.randomUUID.toString}"
    val googleProject = s"project-${UUID.randomUUID.toString}"

    // create the cluster
    val clusterCreateResponse = leo.createCluster(googleProject, clusterName, testClusterRequest).futureValue

    // get the cluster detail
    val clusterGetResponse = leo.getClusterDetails(googleProject, clusterName).futureValue

    // check the create response and get response are the same
    clusterCreateResponse shouldEqual clusterGetResponse
  }


  it should "throw ClusterNotFoundException for nonexistent clusters" in isolatedDbTest {
    whenReady( leo.getClusterDetails("nonexistent", "cluster").failed ) { exc =>
      exc shouldBe a [ClusterNotFoundException]
    }
  }

  "LeonardoService" should "throw ClusterAlreadyExistsException when creating a cluster with same name and project as an existing cluster" in isolatedDbTest {
    val clusterCreateResponse = leo.createCluster("googleProject1", "clusterName1", testClusterRequest).futureValue

    whenReady( leo.createCluster("googleProject1", "clusterName1", testClusterRequest).failed ) { exc =>
      exc shouldBe a [ClusterAlreadyExistsException]
    }
  }

  "LeonardoService" should "delete a cluster" in isolatedDbTest {

    val clusterCreateResponse = leo.createCluster("googleProject", "clusterName", testClusterRequest).futureValue
    val clusterGetResponse = leo.deleteCluster("googleProject", "clusterName").futureValue
    clusterGetResponse shouldEqual 1
  }

  "LeonardoService" should "throw ClusterNotFoundException when deleting non existent clusters" in isolatedDbTest {

    whenReady( leo.deleteCluster("nonexistent", "cluster").failed ) { exc =>
      exc shouldBe a [ClusterNotFoundException]
    }
  }

  "LeonardoService" should "initialize bucket with correct files" in isolatedDbTest {
    // set unique names for our cluster name and bucket name
    val clusterName = s"cluster-${UUID.randomUUID.toString}"
    val bucketName = s"bucket-${clusterName}"

    // assert that our bucket does not exist
    assert(!gdDAO.buckets.contains(bucketName))

    // create the bucket and add files
    leo.initializeBucket(googleProject, clusterName, bucketName)

    // assert that our bucket now exists
    assert(gdDAO.buckets.contains(bucketName))

    // assert that the init files are in the bucket
    initFiles.map(initFile => assert(gdDAO.bucketObjects.exists(_ == (bucketName, initFile))))
  }

  "LeonardoService" should "add bucket objects" in isolatedDbTest {
    val clusterName = s"cluster-${UUID.randomUUID.toString}"
    val bucketName = s"bucket-${UUID.randomUUID.toString}"
    val googleProject = s"project-${UUID.randomUUID.toString}"

    leo.initializeBucketObjects(googleProject, clusterName, bucketName)

    assert(gdDAO.buckets.contains(bucketName))

    initFiles.map(initFile => assert(gdDAO.bucketObjects.exists(_ == (bucketName, initFile))))
  }

  "LeonardoService" should "create a firewall rule in a project only once when the first cluster is added" in isolatedDbTest {
    // set unique names for the google project and the two clusters we'll be creating
    val cluster1 = s"cluster-${UUID.randomUUID.toString}"
    val cluster2 = s"cluster-${UUID.randomUUID.toString}"
    val googleProject = s"project-${UUID.randomUUID.toString}"

    // assert that our google project has no firewall rules
    assert(!gdDAO.firewallRules.exists(_ == (googleProject, dataprocConfig.clusterFirewallRuleName)))

    // create the first cluster, this should create a firewall rule in our project
    leo.createCluster(googleProject, cluster1, testClusterRequest).futureValue

    // check that there is exactly 1 firewall rule for our project
    assert(gdDAO.firewallRules.filterKeys(_ == googleProject).size == 1)

    // create the second cluster. This should check that our project has a firewall rule and not try to add it again
    leo.createCluster(googleProject, cluster2, testClusterRequest).futureValue

    // check that there is still exactly 1 firewall rule in our project
    assert(gdDAO.firewallRules.filterKeys(_ == googleProject).size == 1)
  }

  "LeonardoService" should "template a script using config values" in isolatedDbTest {
    val clusterName = s"cluster-${UUID.randomUUID.toString}"
    val bucketName = s"bucket-${clusterName}"
    val filePath = dataprocConfig.configFolderPath + dataprocConfig.initActionsScriptName
    val replacements = ClusterInitValues(googleProject, clusterName, bucketName, dataprocConfig).toJson.asJsObject.fields

    assert(filePath == "src/test/resources/test-init-actions.sh")

    val result = leo.template("src/test/resources/test-init-actions.sh", replacements).futureValue

    assert(result == "")

  }


}