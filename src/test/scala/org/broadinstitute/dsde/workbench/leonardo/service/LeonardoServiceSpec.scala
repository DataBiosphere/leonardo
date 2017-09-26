package org.broadinstitute.dsde.workbench.leonardo.service

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory

import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.config.DataprocConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.MockGoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterInitValues, ClusterRequest}
import org.broadinstitute.dsde.workbench.leonardo.model.LeonardoJsonSupport._
import org.broadinstitute.dsde.workbench.leonardo.monitor.NoopActor
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import spray.json._

class LeonardoServiceSpec extends TestKit(ActorSystem("leonardotest")) with FlatSpecLike with Matchers with BeforeAndAfter with BeforeAndAfterAll with TestComponent with ScalaFutures with OptionValues {
  private val dataprocConfig = ConfigFactory.load().as[DataprocConfig]("dataproc")
  private val bucketPath = "bucket-path"
  private val serviceAccount = "service-account"
  private val googleProject = "test-google-project"
  private val clusterName = "test-cluster"
  private val bucketName = "bucket-name"
  private lazy val testClusterRequest = ClusterRequest(bucketPath, serviceAccount, Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"), Some(gdDAO.extensionUri))

  private var gdDAO: MockGoogleDataprocDAO = _
  private var leo: LeonardoService = _

  before {
    gdDAO = new MockGoogleDataprocDAO(dataprocConfig)
    leo = new LeonardoService(dataprocConfig, gdDAO, DbSingleton.ref, system.actorOf(NoopActor.props))
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  val initFiles = Array( dataprocConfig.clusterDockerComposeName, dataprocConfig.initActionsScriptName, dataprocConfig.jupyterServerCrtName,
    dataprocConfig.jupyterServerKeyName, dataprocConfig.jupyterRootCaPemName, dataprocConfig.jupyterProxySiteConfName, dataprocConfig.jupyterInstallExtensionScript,
    dataprocConfig.userServiceAccountCredentials)

  "LeonardoService" should "create a cluster" in isolatedDbTest {
    // create the cluster
    val clusterCreateResponse = leo.createCluster(googleProject, clusterName, testClusterRequest).futureValue

    // check the create response has the correct info
    clusterCreateResponse.googleBucket shouldEqual bucketPath
    clusterCreateResponse.googleServiceAccount shouldEqual serviceAccount

    // check the firewall rule was created for the project
    gdDAO.firewallRules should contain (googleProject, dataprocConfig.clusterFirewallRuleName)

    val bucketArray = gdDAO.buckets.filter(str => str.startsWith(clusterName))

    // check the bucket was created for the cluster
    bucketArray.size shouldEqual 1

    // check the init files were added to the bucket
    initFiles.map(initFile => gdDAO.bucketObjects should contain (bucketArray.head, initFile))
    gdDAO.bucketObjects should contain theSameElementsAs (initFiles.map(bucketArray.head -> _))

  }

  it should "create and get a cluster" in isolatedDbTest {
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

  it should "throw ClusterAlreadyExistsException when creating a cluster with same name and project as an existing cluster" in isolatedDbTest {
    // create the first cluster
    leo.createCluster(googleProject, clusterName, testClusterRequest).futureValue

    // creating the same cluster again should throw a ClusterAlreadyExistsException
    whenReady( leo.createCluster(googleProject, clusterName, testClusterRequest).failed ) { exc =>
      exc shouldBe a [ClusterAlreadyExistsException]
    }
  }

  it should "delete a cluster" in isolatedDbTest {
    // check that the cluster does not exist
    gdDAO.clusters should not contain key (clusterName)

    // create the cluster
    val clusterCreateResponse = leo.createCluster(googleProject, clusterName, testClusterRequest).futureValue

    // check that the cluster was created
    gdDAO.clusters should contain key (clusterName)

    // delete the cluster
    val clusterDeleteResponse = leo.deleteCluster(googleProject, clusterName).futureValue

    // the delete response should indicate 1 cluster was deleted
    clusterDeleteResponse shouldEqual 1

    // check that the cluster no longer exists
    gdDAO.clusters should not contain key (clusterName)
  }

  it should "throw ClusterNotFoundException when deleting non existent clusters" in isolatedDbTest {
    whenReady( leo.deleteCluster("nonexistent", "cluster").failed ) { exc =>
      exc shouldBe a [ClusterNotFoundException]
    }
  }

  it should "initialize bucket with correct files" in isolatedDbTest {
    // Our bucket should not exist
    gdDAO.buckets should not contain (bucketName)

    // create the bucket and add files
    leo.initializeBucket(googleProject, clusterName, bucketName, testClusterRequest).futureValue

    // our bucket should now exist
    gdDAO.buckets should contain (bucketName)

    // check the init files were added to the bucket
    initFiles.map(initFile => gdDAO.bucketObjects should contain (bucketName, initFile))
  }

  it should "add bucket objects" in isolatedDbTest {
    // since we're only testing adding the objects, we're directly creating the bucket in the mock DAO
    gdDAO.buckets += bucketName

    // add all the bucket objects
    leo.initializeBucketObjects(googleProject, clusterName, bucketName, testClusterRequest).futureValue

    // check that the bucket exists
    gdDAO.buckets should contain (bucketName)

    // check the init files were added to the bucket
    initFiles.map(initFile => gdDAO.bucketObjects should contain (bucketName, initFile))
  }

  it should "create a firewall rule in a project only once when the first cluster is added" in isolatedDbTest {
    val clusterName2 = "test-cluster-2"

    // Our google project should have no firewall rules
    gdDAO.firewallRules should not contain (googleProject, dataprocConfig.clusterFirewallRuleName)

    // create the first cluster, this should create a firewall rule in our project
    leo.createCluster(googleProject, clusterName, testClusterRequest).futureValue

    // check that there is exactly 1 firewall rule for our project
    gdDAO.firewallRules.filterKeys(_ == googleProject) should have size 1

    // create the second cluster. This should check that our project has a firewall rule and not try to add it again
    leo.createCluster(googleProject, clusterName2, testClusterRequest).futureValue

    // check that there is still exactly 1 firewall rule in our project
    gdDAO.firewallRules.filterKeys(_ == googleProject) should have size 1
  }

  it should "template a script using config values" in isolatedDbTest {
    // create the file path for our init actions script
    val filePath = dataprocConfig.configFolderPath + dataprocConfig.initActionsScriptName

    // Create replacements map
    val replacements = ClusterInitValues(googleProject, clusterName, bucketName, dataprocConfig, testClusterRequest).toJson.asJsObject.fields

    // Each value in the replacement map will replace it's key in the file being processed
    val result = leo.template(filePath, replacements).futureValue

    // Check that the values in the bash script file were correctly replaced
    val expected =
      s"""|#!/usr/bin/env bash
          |
          |"$clusterName"
          |"$googleProject"
          |"${dataprocConfig.jupyterProxyDockerImage}"
          |"${gdDAO.extensionUri}"""".stripMargin

    result shouldEqual expected
  }

  it should "throw a JupyterExtensionException when the extensionUri is too long" in isolatedDbTest {
    val jupyterExtensionUri = "gs://aBucket/" + Stream.continually('a').take(1025).mkString

    // create the cluster
    val response = leo.createCluster(googleProject, clusterName, testClusterRequest.copy(jupyterExtensionUri = Some(jupyterExtensionUri))).failed.futureValue

    response shouldBe a [JupyterExtensionException]
  }

  it should "throw a JupyterExtensionException when the jupyterExtensionUri is not a valid URI" in isolatedDbTest {
    val jupyterExtensionUri = Stream.continually('a').take(100).mkString

    // create the cluster
    val response = leo.createCluster(googleProject, clusterName, testClusterRequest.copy(jupyterExtensionUri = Some(jupyterExtensionUri))).failed.futureValue

    response shouldBe a [JupyterExtensionException]
  }

  it should "throw a JupyterExtensionException when the jupyterExtensionUri does not point to a GCS object" in isolatedDbTest {
    val jupyterExtensionUri = "gs://bogus/object.tar.gz"

    // create the cluster
    val response = leo.createCluster(googleProject, clusterName, testClusterRequest.copy(jupyterExtensionUri = Some(jupyterExtensionUri))).failed.futureValue

    response shouldBe a [JupyterExtensionException]
  }

  it should "list no clusters" in isolatedDbTest {
    leo.listClusters(Map.empty).futureValue shouldBe 'empty
    leo.listClusters(Map("foo" -> "bar", "baz" -> "biz")).futureValue shouldBe 'empty
  }

  it should "list clusters" in isolatedDbTest {
    // create a couple clusters
    val cluster1 = leo.createCluster(googleProject, clusterName, testClusterRequest).futureValue

    val clusterName2 = "test-cluster-2"
    val cluster2 = leo.createCluster(googleProject, clusterName2, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar"))).futureValue

    leo.listClusters(Map.empty).futureValue.toSet shouldBe Set(cluster1, cluster2)
  }

  it should "list clusters with labels" in isolatedDbTest {
    // create a couple clusters
    val cluster1 = leo.createCluster(googleProject, clusterName, testClusterRequest).futureValue

    val clusterName2 = s"test-cluster-2"
    val cluster2 = leo.createCluster(googleProject, clusterName2, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar"))).futureValue

    leo.listClusters(Map("foo" -> "bar")).futureValue.toSet shouldBe Set(cluster1, cluster2)
    leo.listClusters(Map("foo" -> "bar", "bam" -> "yes")).futureValue.toSet shouldBe Set(cluster1)
    leo.listClusters(Map("foo" -> "bar", "bam" -> "yes", "vcf" -> "no")).futureValue.toSet shouldBe Set(cluster1)
    leo.listClusters(Map("a" -> "b")).futureValue.toSet shouldBe Set(cluster2)
    leo.listClusters(Map("foo" -> "bar", "baz" -> "biz")).futureValue.toSet shouldBe Set.empty
    leo.listClusters(Map("A" -> "B")).futureValue.toSet shouldBe Set(cluster2)  // labels are not case sensitive because MySQL
  }

  it should "list clusters with swagger-style labels" in isolatedDbTest {
    // create a couple clusters
    val cluster1 = leo.createCluster(googleProject, clusterName, testClusterRequest).futureValue

    val clusterName2 = "test-cluster-2"
    val cluster2 = leo.createCluster(googleProject, clusterName2, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar"))).futureValue

    leo.listClusters(Map("_labels" -> "foo=bar")).futureValue.toSet shouldBe Set(cluster1, cluster2)
    leo.listClusters(Map("_labels" -> "foo=bar,bam=yes")).futureValue.toSet shouldBe Set(cluster1)
    leo.listClusters(Map("_labels" -> "foo=bar,bam=yes,vcf=no")).futureValue.toSet shouldBe Set(cluster1)
    leo.listClusters(Map("_labels" -> "a=b")).futureValue.toSet shouldBe Set(cluster2)
    leo.listClusters(Map("_labels" -> "baz=biz")).futureValue.toSet shouldBe Set.empty
    leo.listClusters(Map("_labels" -> "A=B")).futureValue.toSet shouldBe Set(cluster2)   // labels are not case sensitive because MySQL
    leo.listClusters(Map("_labels" -> "foo%3Dbar")).failed.futureValue shouldBe a [ParseLabelsException]
    leo.listClusters(Map("_labels" -> "foo=bar;bam=yes")).failed.futureValue shouldBe a [ParseLabelsException]
    leo.listClusters(Map("_labels" -> "foo=bar,bam")).failed.futureValue shouldBe a [ParseLabelsException]
    leo.listClusters(Map("_labels" -> "bogus")).failed.futureValue shouldBe a [ParseLabelsException]
    leo.listClusters(Map("_labels" -> "a,b")).failed.futureValue shouldBe a [ParseLabelsException]
  }

  it should "generate valid bucket names" in {
    leo.generateBucketName("myCluster") should startWith ("mycluster-")
    leo.generateBucketName("MyCluster") should startWith ("mycluster-")
    leo.generateBucketName("My_Cluster") should startWith ("my_cluster-")
    leo.generateBucketName("MY_CLUSTER") should startWith ("my_cluster-")
    leo.generateBucketName("MYCLUSTER") should startWith ("mycluster-")
    leo.generateBucketName("_myCluster_") should startWith ("0mycluster0-")
    leo.generateBucketName("googMyCluster") should startWith ("g00gmycluster-")
    leo.generateBucketName("my_Google_clUsTer") should startWith("my_g00gle_cluster-")
    leo.generateBucketName("myClusterWhichHasAVeryLongNameBecauseIAmExtremelyVerboseInMyDescriptions") should startWith ("myclusterwhichhasaverylong-")
    leo.generateBucketName("myClusterWhichHasAVeryLongNameBecauseIAmExtremelyVerboseInMyDescriptions").length shouldBe 63
  }
}
