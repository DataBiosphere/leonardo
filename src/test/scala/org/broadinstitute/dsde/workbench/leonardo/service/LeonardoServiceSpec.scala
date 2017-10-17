package org.broadinstitute.dsde.workbench.leonardo.service

import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.google.gcs.{GcsBucketName, GcsPath, GcsRelativePath}
import org.broadinstitute.dsde.workbench.leonardo.config.{ClusterResourcesConfig, DataprocConfig, ProxyConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.{CallToGoogleApiFailedException, MockGoogleDataprocDAO, MockSamDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.{DataAccess, DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.LeonardoJsonSupport._
import org.broadinstitute.dsde.workbench.leonardo.monitor.NoopActor
import org.broadinstitute.dsde.workbench.model.{WorkbenchUserEmail, WorkbenchUserId}
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import spray.json._

class LeonardoServiceSpec extends TestKit(ActorSystem("leonardotest")) with FlatSpecLike with Matchers with BeforeAndAfter with BeforeAndAfterAll with TestComponent with ScalaFutures with OptionValues {
  private val dataprocConfig = ConfigFactory.load().as[DataprocConfig]("dataproc")
  private val clusterResourcesConfig = ConfigFactory.load().as[ClusterResourcesConfig]("clusterResources")
  private val proxyConfig = ConfigFactory.load().as[ProxyConfig]("proxy")
  private val bucketPath = GcsBucketName("bucket-path")
  private val googleProject = GoogleProject("test-google-project")
  private val clusterName = ClusterName("test-cluster")
  private val defaultUserInfo = UserInfo(OAuth2BearerToken("accessToken"), WorkbenchUserId("user1"), WorkbenchUserEmail("user1@example.com"), 0)
  private lazy val testClusterRequest = ClusterRequest(bucketPath, Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"), Some(gdDAO.extensionPath))

  private var gdDAO: MockGoogleDataprocDAO = _
  private var samDAO: MockSamDAO = _
  private var leo: LeonardoService = _

  before {
    gdDAO = new MockGoogleDataprocDAO(dataprocConfig, proxyConfig)
    samDAO = new MockSamDAO
    leo = new LeonardoService(dataprocConfig, clusterResourcesConfig, proxyConfig, gdDAO, DbSingleton.ref, system.actorOf(NoopActor.props), samDAO)
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  val initFiles = Array(clusterResourcesConfig.clusterDockerCompose, clusterResourcesConfig.initActionsScript, clusterResourcesConfig.jupyterServerCrt,
    clusterResourcesConfig.jupyterServerKey, clusterResourcesConfig.jupyterRootCaPem, clusterResourcesConfig.jupyterProxySiteConf, clusterResourcesConfig.jupyterInstallExtensionScript
  ) map GcsRelativePath

  "LeonardoService" should "create a cluster" in isolatedDbTest {
    // create the cluster
    val clusterCreateResponse = leo.createCluster(defaultUserInfo, googleProject, clusterName, testClusterRequest).futureValue

    // check the create response has the correct info
    clusterCreateResponse.googleBucket shouldEqual bucketPath
    clusterCreateResponse.googleServiceAccount shouldEqual samDAO.serviceAccount

    // check the firewall rule was created for the project
    gdDAO.firewallRules should contain (googleProject, proxyConfig.firewallRuleName)

    val bucketArray = gdDAO.buckets.filter(bucket => bucket.name.startsWith(clusterName.string))

    // check the bucket was created for the cluster
    bucketArray.size shouldEqual 1

    // check the init files were added to the bucket
    initFiles.foreach(initFile => gdDAO.bucketObjects should contain (GcsPath(bucketArray.head, initFile)))
    gdDAO.bucketObjects should contain theSameElementsAs (initFiles.map(GcsPath(bucketArray.head, _)))

    val initBucket = dbFutureValue { dataAccess =>
      dataAccess.clusterQuery.getInitBucket(googleProject, clusterName)
    }
    initBucket shouldBe 'defined
  }

  it should "create and get a cluster" in isolatedDbTest {
    // create the cluster
    val clusterCreateResponse = leo.createCluster(defaultUserInfo, googleProject, clusterName, testClusterRequest).futureValue

    // get the cluster detail
    val clusterGetResponse = leo.getActiveClusterDetails(googleProject, clusterName).futureValue

    // check the create response and get response are the same
    clusterCreateResponse shouldEqual clusterGetResponse
  }

  it should "throw ClusterNotFoundException for nonexistent clusters" in isolatedDbTest {
    whenReady( leo.getActiveClusterDetails(GoogleProject("nonexistent"), ClusterName("cluster")).failed ) { exc =>
      exc shouldBe a [ClusterNotFoundException]
    }
  }

  it should "throw ClusterAlreadyExistsException when creating a cluster with same name and project as an existing cluster" in isolatedDbTest {
    // create the first cluster
    leo.createCluster(defaultUserInfo, googleProject, clusterName, testClusterRequest).futureValue

    // creating the same cluster again should throw a ClusterAlreadyExistsException
    whenReady( leo.createCluster(defaultUserInfo, googleProject, clusterName, testClusterRequest).failed ) { exc =>
      exc shouldBe a [ClusterAlreadyExistsException]
    }
  }

  it should "create two clusters with same name with only one active" in isolatedDbTest {
    //create first cluster
    leo.createCluster(defaultUserInfo, googleProject, clusterName, testClusterRequest).futureValue

    // check that the cluster was created
    gdDAO.clusters should contain key (clusterName)

    // delete the cluster
    val clusterDeleteResponse = leo.deleteCluster(googleProject, clusterName).futureValue

    // the delete response should indicate 1 cluster was deleted
    clusterDeleteResponse shouldEqual 1

    //recreate cluster with same project and cluster name
    leo.createCluster(defaultUserInfo, googleProject, clusterName, testClusterRequest).futureValue

    //confirm cluster was created
    gdDAO.clusters should contain key (clusterName)
  }

  it should "delete a cluster" in isolatedDbTest {
    // check that the cluster does not exist
    gdDAO.clusters should not contain key (clusterName)

    // create the cluster
    val clusterCreateResponse = leo.createCluster(defaultUserInfo, googleProject, clusterName, testClusterRequest).futureValue

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
    whenReady( leo.deleteCluster(GoogleProject("nonexistent"), ClusterName("cluster")).failed ) { exc =>
      exc shouldBe a [ClusterNotFoundException]
    }
  }

  it should "initialize bucket with correct files" in isolatedDbTest {
    // Our bucket should not exist
    gdDAO.buckets should not contain (bucketPath)

    // create the bucket and add files
    leo.initializeBucket(googleProject, clusterName, bucketPath, testClusterRequest).futureValue

    // our bucket should now exist
    gdDAO.buckets should contain (bucketPath)

    // check the init files were added to the bucket
    initFiles.map(initFile => gdDAO.bucketObjects should contain (GcsPath(bucketPath, initFile)))
  }

  it should "add bucket objects" in isolatedDbTest {
    // since we're only testing adding the objects, we're directly creating the bucket in the mock DAO
    gdDAO.buckets += bucketPath

    // add all the bucket objects
    leo.initializeBucketObjects(googleProject, clusterName, bucketPath, testClusterRequest).futureValue

    // check that the bucket exists
    gdDAO.buckets should contain (bucketPath)

    // check the init files were added to the bucket
    initFiles.map(initFile => gdDAO.bucketObjects should contain (GcsPath(bucketPath, initFile)))
  }

  it should "create a firewall rule in a project only once when the first cluster is added" in isolatedDbTest {
    val clusterName2 = ClusterName("test-cluster-2")

    // Our google project should have no firewall rules
    gdDAO.firewallRules should not contain (googleProject, proxyConfig.firewallRuleName)

    // create the first cluster, this should create a firewall rule in our project
    leo.createCluster(defaultUserInfo, googleProject, clusterName, testClusterRequest).futureValue

    // check that there is exactly 1 firewall rule for our project
    gdDAO.firewallRules.filterKeys(_ == googleProject) should have size 1

    // create the second cluster. This should check that our project has a firewall rule and not try to add it again
    leo.createCluster(defaultUserInfo, googleProject, clusterName2, testClusterRequest).futureValue

    // check that there is still exactly 1 firewall rule in our project
    gdDAO.firewallRules.filterKeys(_ == googleProject) should have size 1
  }

  it should "template a script using config values" in isolatedDbTest {
    // Create replacements map
    val replacements = ClusterInitValues(googleProject, clusterName, bucketPath, testClusterRequest, dataprocConfig, clusterResourcesConfig, proxyConfig).toJson.asJsObject.fields

    // Each value in the replacement map will replace it's key in the file being processed
    val result = leo.template(clusterResourcesConfig.configFolderPath + clusterResourcesConfig.initActionsScript, replacements).futureValue

    // Check that the values in the bash script file were correctly replaced
    val expected =
      s"""|#!/usr/bin/env bash
          |
          |"${clusterName.string}"
          |"${googleProject.string}"
          |"${proxyConfig.jupyterProxyDockerImage}"
          |"${gdDAO.extensionPath.toUri}"""".stripMargin

    result shouldEqual expected
  }

  it should "throw a JupyterExtensionException when the extensionUri is too long" in isolatedDbTest {
    val jupyterExtensionUri = GcsPath(GcsBucketName("bucket"), GcsRelativePath(Stream.continually('a').take(1025).mkString))

    // create the cluster
    val response = leo.createCluster(defaultUserInfo, googleProject, clusterName, testClusterRequest.copy(jupyterExtensionUri = Some(jupyterExtensionUri))).failed.futureValue

    response shouldBe a [JupyterExtensionException]
  }

  it should "throw a JupyterExtensionException when the jupyterExtensionUri does not point to a GCS object" in isolatedDbTest {
    val jupyterExtensionUri = GcsPath.parse("gs://bogus/object.tar.gz").right.get

    // create the cluster
    val response = leo.createCluster(defaultUserInfo, googleProject, clusterName, testClusterRequest.copy(jupyterExtensionUri = Some(jupyterExtensionUri))).failed.futureValue

    response shouldBe a [JupyterExtensionException]
  }

  it should "list no clusters" in isolatedDbTest {
    leo.listClusters(Map.empty).futureValue shouldBe 'empty
    leo.listClusters(Map("foo" -> "bar", "baz" -> "biz")).futureValue shouldBe 'empty
  }

  it should "list all clusters" in isolatedDbTest {
    // create a couple clusters
    val clusterName1 = ClusterName(s"cluster-${UUID.randomUUID.toString}")
    val cluster1 = leo.createCluster(defaultUserInfo, googleProject, clusterName1, testClusterRequest).futureValue

    val clusterName2 = ClusterName(s"cluster-${UUID.randomUUID.toString}")
    val cluster2 = leo.createCluster(defaultUserInfo, googleProject, clusterName2, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar"))).futureValue

    leo.listClusters(Map.empty).futureValue.toSet shouldBe Set(cluster1, cluster2)
  }

  it should "list all active clusters" in isolatedDbTest {
    // create a couple clusters
    val cluster1 = leo.createCluster(defaultUserInfo, googleProject, clusterName, testClusterRequest).futureValue

    val clusterName2 = ClusterName("test-cluster-2")
    val cluster2 = leo.createCluster(defaultUserInfo, googleProject, clusterName2, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar"))).futureValue

    leo.listClusters(Map("includeDeleted" -> "false")).futureValue.toSet shouldBe Set(cluster1, cluster2)
    leo.listClusters(Map.empty).futureValue.toSet shouldBe Set(cluster1, cluster2)
    leo.listClusters(Map.empty).futureValue.toSet shouldBe Set(cluster1, cluster2)

    val clusterName3 = ClusterName("test-cluster-3")
    val cluster3 = leo.createCluster(defaultUserInfo, googleProject, clusterName3, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar"))).futureValue

    dbFutureValue(dataAccess =>
      dataAccess.clusterQuery.completeDeletion(cluster3.googleId, clusterName3)
    )

    leo.listClusters(Map.empty).futureValue.toSet shouldBe Set(cluster1, cluster2)
    leo.listClusters(Map("includeDeleted" -> "false")).futureValue.toSet shouldBe Set(cluster1, cluster2)
    leo.listClusters(Map("includeDeleted" -> "true")).futureValue.toSet.size shouldBe 3
  }


  it should "list clusters with labels" in isolatedDbTest {
    // create a couple clusters
    val clusterName1 = ClusterName(s"cluster-${UUID.randomUUID.toString}")
    val cluster1 = leo.createCluster(defaultUserInfo, googleProject, clusterName1, testClusterRequest).futureValue

    val clusterName2 = ClusterName(s"test-cluster-2")
    val cluster2 = leo.createCluster(defaultUserInfo, googleProject, clusterName2, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar"))).futureValue

    leo.listClusters(Map("foo" -> "bar")).futureValue.toSet shouldBe Set(cluster1, cluster2)
    leo.listClusters(Map("foo" -> "bar", "bam" -> "yes")).futureValue.toSet shouldBe Set(cluster1)
    leo.listClusters(Map("foo" -> "bar", "bam" -> "yes", "vcf" -> "no")).futureValue.toSet shouldBe Set(cluster1)
    leo.listClusters(Map("a" -> "b")).futureValue.toSet shouldBe Set(cluster2)
    leo.listClusters(Map("foo" -> "bar", "baz" -> "biz")).futureValue.toSet shouldBe Set.empty
    leo.listClusters(Map("A" -> "B")).futureValue.toSet shouldBe Set(cluster2)  // labels are not case sensitive because MySQL
  }

  it should "throw IllegalLabelKeyException when using a forbidden label" in isolatedDbTest {
    // cluster should not be allowed to have a label with key of "includeDeleted"
    val includeDeletedResponse = leo.createCluster(defaultUserInfo, googleProject, clusterName, testClusterRequest.copy(labels = Map("includeDeleted" -> "val"))).failed.futureValue
    includeDeletedResponse shouldBe a [IllegalLabelKeyException]
  }

  it should "list clusters with swagger-style labels" in isolatedDbTest {
    // create a couple clusters
    val clusterName1 = ClusterName(s"cluster-${UUID.randomUUID.toString}")
    val cluster1 = leo.createCluster(defaultUserInfo, googleProject, clusterName1, testClusterRequest).futureValue

    val clusterName2 = ClusterName(s"cluster-${UUID.randomUUID.toString}")
    val cluster2 = leo.createCluster(defaultUserInfo, googleProject, clusterName2, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar"))).futureValue

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

  it should "delete the init bucket if cluster creation fails" in isolatedDbTest {
    // create the cluster
    val clusterCreateResponse = leo.createCluster(defaultUserInfo, googleProject, gdDAO.badClusterName, testClusterRequest).failed.futureValue

    clusterCreateResponse shouldBe a [CallToGoogleApiFailedException]

    // check the firewall rule was created for the project
    gdDAO.firewallRules should contain (googleProject, proxyConfig.firewallRuleName)

    gdDAO.buckets shouldBe 'empty
  }
}
