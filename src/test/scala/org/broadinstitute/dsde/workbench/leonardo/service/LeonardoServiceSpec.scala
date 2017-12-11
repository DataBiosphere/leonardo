package org.broadinstitute.dsde.workbench.leonardo.service

import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.google.gcs.{GcsBucketName, GcsPath, GcsRelativePath}
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleIamDAO
import org.broadinstitute.dsde.workbench.leonardo.auth.{MockServiceAccountProvider, WhitelistAuthProvider}
import org.broadinstitute.dsde.workbench.leonardo.config.{ClusterDefaultsConfig, ClusterFilesConfig, ClusterResourcesConfig, DataprocConfig, ProxyConfig, SwaggerConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.{CallToGoogleApiFailedException, MockGoogleDataprocDAO, MockSamDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.LeonardoJsonSupport._
import org.broadinstitute.dsde.workbench.leonardo.monitor.NoopActor
import org.broadinstitute.dsde.workbench.model._
import org.broadinstitute.dsde.workbench.model.google.{GoogleProject, ServiceAccountKey, ServiceAccountKeyId, ServiceAccountPrivateKeyData}
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import spray.json._

class LeonardoServiceSpec extends TestKit(ActorSystem("leonardotest")) with FlatSpecLike with Matchers with BeforeAndAfter with BeforeAndAfterAll with TestComponent with ScalaFutures with OptionValues {
  private val configFactory = ConfigFactory.load()
  private val dataprocConfig = configFactory.as[DataprocConfig]("dataproc")
  private val clusterFilesConfig = configFactory.as[ClusterFilesConfig]("clusterFiles")
  private val clusterResourcesConfig = configFactory.as[ClusterResourcesConfig]("clusterResources")
  private val clusterDefaultsConfig = configFactory.as[ClusterDefaultsConfig]("clusterDefaults")
  private val proxyConfig = configFactory.as[ProxyConfig]("proxy")
  private val swaggerConfig = configFactory.as[SwaggerConfig]("swagger")
  private val bucketPath = GcsBucketName("bucket-path")
  private val googleProject = GoogleProject("test-google-project")
  private val petServiceAccount = WorkbenchEmail("petSA@test-domain.iam.gserviceaccount.com")
  private val clusterName = ClusterName("test-cluster")
  private val defaultUserInfo = UserInfo(OAuth2BearerToken("accessToken"), WorkbenchUserId("user1"), WorkbenchEmail("user1@example.com"), 0)
  private lazy val testClusterRequest = ClusterRequest(bucketPath, Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"), Some(gdDAO.extensionPath))
  private lazy val singleNodeDefaultMachineConfig = MachineConfig(Some(clusterDefaultsConfig.numberOfWorkers), Some(clusterDefaultsConfig.masterMachineType), Some(clusterDefaultsConfig.masterDiskSize))
  private val serviceAccountKey = ServiceAccountKey(ServiceAccountKeyId("123"), ServiceAccountPrivateKeyData("abcdefg"), Some(Instant.now), Some(Instant.now.plusSeconds(300)))

  private var gdDAO: MockGoogleDataprocDAO = _
  private var iamDAO: MockGoogleIamDAO = _
  private var samDAO: MockSamDAO = _
  private var leo: LeonardoService = _
  private var authProvider: LeoAuthProvider = _
  private var serviceAccountProvider: ServiceAccountProvider = _

  before {
    gdDAO = new MockGoogleDataprocDAO(dataprocConfig, proxyConfig, clusterDefaultsConfig)
    iamDAO = new MockGoogleIamDAO
    samDAO = new MockSamDAO
    authProvider = new WhitelistAuthProvider(configFactory.getConfig("auth.providerConfig"))
    serviceAccountProvider = new MockServiceAccountProvider(configFactory.getConfig("serviceAccounts.config"))
    leo = new LeonardoService(dataprocConfig, clusterFilesConfig, clusterResourcesConfig, proxyConfig, swaggerConfig, gdDAO, iamDAO, DbSingleton.ref, system.actorOf(NoopActor.props), authProvider, serviceAccountProvider)
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  val initFiles = List(
    clusterResourcesConfig.clusterDockerCompose.string,
    clusterResourcesConfig.initActionsScript.string,
    clusterFilesConfig.jupyterServerCrt.getName,
    clusterFilesConfig.jupyterServerKey.getName,
    clusterFilesConfig.jupyterRootCaPem.getName,
    clusterResourcesConfig.jupyterProxySiteConf.string,
    clusterResourcesConfig.jupyterInstallExtensionScript.string,
    clusterResourcesConfig.jupyterCustomJs.string,
    clusterResourcesConfig.jupyterGoogleSignInJs.string,
    ClusterInitValues.serviceAccountCredentialsFilename
  ) map GcsRelativePath


  "LeonardoService" should "create a single node cluster with default machine configs" in isolatedDbTest {
    // create the cluster
    val clusterCreateResponse = leo.createCluster(defaultUserInfo, googleProject, clusterName, testClusterRequest).futureValue

    // check the create response has the correct info
    clusterCreateResponse.googleBucket shouldEqual bucketPath
    clusterCreateResponse.serviceAccountInfo.clusterServiceAccount shouldEqual None
    clusterCreateResponse.serviceAccountInfo.notebookServiceAccount shouldEqual Some(samDAO.serviceAccount)

    // check the cluster has the correct machine configs
    clusterCreateResponse.machineConfig shouldEqual singleNodeDefaultMachineConfig

    // check the firewall rule was created for the project
    gdDAO.firewallRules should contain (googleProject, proxyConfig.firewallRuleName)

    val bucketArray = gdDAO.buckets.filter(bucket => bucket.name.startsWith(clusterName.string))

    // check the bucket was created for the cluster
    bucketArray.size shouldEqual 1

    // check the init files were added to the bucket
    initFiles.foreach(initFile => gdDAO.bucketObjects should contain (GcsPath(bucketArray.head, initFile)))
    gdDAO.bucketObjects should contain theSameElementsAs (initFiles.map(GcsPath(bucketArray.head, _)))
    iamDAO.serviceAccountKeys should contain key (samDAO.serviceAccount)

    val initBucket = dbFutureValue { dataAccess =>
      dataAccess.clusterQuery.getInitBucket(googleProject, clusterName)
    }
    initBucket shouldBe 'defined
  }

  it should "create and get a cluster" in isolatedDbTest {
    // create the cluster
    val clusterCreateResponse = leo.createCluster(defaultUserInfo, googleProject, clusterName, testClusterRequest).futureValue

    // get the cluster detail
    val clusterGetResponse = leo.getActiveClusterDetails(defaultUserInfo, googleProject, clusterName).futureValue

    // check the create response and get response are the same
    clusterCreateResponse shouldEqual clusterGetResponse
  }

  it should "create a single node cluster with an empty machine config" in isolatedDbTest {
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = Some(MachineConfig()))
    val clusterCreateResponse = leo.createCluster(defaultUserInfo, googleProject, clusterName, clusterRequestWithMachineConfig).futureValue
    clusterCreateResponse.machineConfig shouldEqual singleNodeDefaultMachineConfig
  }

  it should "create a single node cluster with zero workers explicitly defined in machine config" in isolatedDbTest {
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = Some(MachineConfig(Some(0))))
    val clusterCreateResponse = leo.createCluster(defaultUserInfo, googleProject, clusterName, clusterRequestWithMachineConfig).futureValue
    clusterCreateResponse.machineConfig shouldEqual singleNodeDefaultMachineConfig
  }

  it should "create a single node cluster with master configs defined" in isolatedDbTest {
    val singleNodeDefinedMachineConfig = MachineConfig(Some(0), Some("test-master-machine-type2"), Some(200))
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = Some(singleNodeDefinedMachineConfig))
    val clusterCreateResponse = leo.createCluster(defaultUserInfo, googleProject, clusterName, clusterRequestWithMachineConfig).futureValue
    clusterCreateResponse.machineConfig shouldEqual singleNodeDefinedMachineConfig
  }

  it should "create a single node cluster and override worker configs" in isolatedDbTest {
    // machine config is creating a single node cluster, but has worker configs defined
    val machineConfig = Some(MachineConfig(Some(0), Some("test-master-machine-type3"), Some(200), Some("test-worker-machine-type"), Some(10), Some(3), Some(4)))
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = machineConfig)

    val clusterCreateResponse = leo.createCluster(defaultUserInfo, googleProject, clusterName, clusterRequestWithMachineConfig).futureValue
    clusterCreateResponse.machineConfig shouldEqual MachineConfig(Some(0), Some("test-master-machine-type3"), Some(200))
  }

  it should "create a standard cluster with 2 workers with default worker configs" in isolatedDbTest {
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = Some(MachineConfig(Some(2))))

    val clusterCreateResponse = leo.createCluster(defaultUserInfo, googleProject, clusterName, clusterRequestWithMachineConfig).futureValue
    val machineConfigResponse = MachineConfig(Some(2),
      Some(clusterDefaultsConfig.masterMachineType),
      Some(clusterDefaultsConfig.masterDiskSize),
      Some(clusterDefaultsConfig.workerMachineType),
      Some(clusterDefaultsConfig.workerDiskSize),
      Some(clusterDefaultsConfig.numberOfWorkerLocalSSDs),
      Some(clusterDefaultsConfig.numberOfPreemptibleWorkers))

    clusterCreateResponse.machineConfig shouldEqual machineConfigResponse
  }

  it should "create a standard cluster with 10 workers with defined config" in isolatedDbTest {
    val machineConfig = MachineConfig(Some(10), Some("test-master-machine-type"), Some(200), Some("test-worker-machine-type"), Some(300), Some(3), Some(4))
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = Some(machineConfig))

    val clusterCreateResponse = leo.createCluster(defaultUserInfo, googleProject, clusterName, clusterRequestWithMachineConfig).futureValue
    clusterCreateResponse.machineConfig shouldEqual machineConfig

  }

  it should "create a standard cluster with 2 workers and override too-small disk sizes with minimum disk size" in isolatedDbTest {
    val machineConfig = MachineConfig(Some(2), Some("test-master-machine-type"), Some(50), Some("test-worker-machine-type"), Some(10), Some(3), Some(4))
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = Some(machineConfig))

    val clusterCreateResponse = leo.createCluster(defaultUserInfo, googleProject, clusterName, clusterRequestWithMachineConfig).futureValue
    clusterCreateResponse.machineConfig shouldEqual MachineConfig(Some(2), Some("test-master-machine-type"), Some(100), Some("test-worker-machine-type"), Some(100), Some(3), Some(4))
  }

  it should "throw OneWorkerSpecifiedInClusterRequestException when create a 1 worker cluster" in isolatedDbTest {
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = Some(MachineConfig(Some(1))))

    whenReady(leo.createCluster(defaultUserInfo, googleProject, clusterName, clusterRequestWithMachineConfig).failed) { exc =>
      exc shouldBe a[OneWorkerSpecifiedInClusterRequestException]
    }
  }

  it should "throw NegativeIntegerArgumentInClusterRequestException when master disk size in single node cluster request is a negative integer" in isolatedDbTest {
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = Some(MachineConfig(Some(0), Some("test-worker-machine-type"), Some(-30))))

    whenReady(leo.createCluster(defaultUserInfo, googleProject, clusterName, clusterRequestWithMachineConfig).failed) { exc =>
      exc shouldBe a[NegativeIntegerArgumentInClusterRequestException]
    }
  }

  it should "throw NegativeIntegerArgumentInClusterRequestException when number of preemptible workers in a 2 worker cluster request is a negative integer" in isolatedDbTest {
    val machineConfig = MachineConfig(Some(10), Some("test-master-machine-type"), Some(200), Some("test-worker-machine-type"), Some(300), Some(3), Some(-1))
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = Option(machineConfig))

    whenReady(leo.createCluster(defaultUserInfo, googleProject, clusterName, clusterRequestWithMachineConfig).failed) { exc =>
      exc shouldBe a[NegativeIntegerArgumentInClusterRequestException]
    }
  }

  it should "throw ClusterNotFoundException for nonexistent clusters" in isolatedDbTest {
    whenReady( leo.getActiveClusterDetails(defaultUserInfo, GoogleProject("nonexistent"), ClusterName("cluster")).failed ) { exc =>
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
    leo.deleteCluster(defaultUserInfo, googleProject, clusterName).futureValue

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
    iamDAO.serviceAccountKeys should contain key (samDAO.serviceAccount)

    // delete the cluster
    leo.deleteCluster(defaultUserInfo, googleProject, clusterName).futureValue

    // check that the cluster no longer exists
    gdDAO.clusters should not contain key (clusterName)
    iamDAO.serviceAccountKeys should not contain key (samDAO.serviceAccount)
  }

  it should "delete a cluster that has status Error" in isolatedDbTest {
    // check that the cluster does not exist
    gdDAO.clusters should not contain key (gdDAO.errorClusterName)

    // create the cluster
    val clusterCreateResponse = leo.createCluster(defaultUserInfo, googleProject, gdDAO.errorClusterName, testClusterRequest).futureValue

    // check that the cluster was created
    gdDAO.clusters should contain key gdDAO.errorClusterName

    // delete the cluster
    leo.deleteCluster(defaultUserInfo, googleProject, gdDAO.errorClusterName).futureValue

    // check that the cluster no longer exists
    gdDAO.clusters should not contain key (gdDAO.errorClusterName)
  }

  it should "throw ClusterNotFoundException when deleting non existent clusters" in isolatedDbTest {
    whenReady( leo.deleteCluster(defaultUserInfo, GoogleProject("nonexistent"), ClusterName("cluster")).failed ) { exc =>
      exc shouldBe a [ClusterNotFoundException]
    }
  }

  it should "initialize bucket with correct files" in isolatedDbTest {
    // Our bucket should not exist
    gdDAO.buckets should not contain (bucketPath)

    // create the bucket and add files
    leo.initializeBucket(googleProject, clusterName, bucketPath, testClusterRequest, ServiceAccountInfo(None, Some(petServiceAccount)), Some(serviceAccountKey)).futureValue

    // our bucket should now exist
    gdDAO.buckets should contain (bucketPath)

    // check the init files were added to the bucket
    initFiles.map(initFile => gdDAO.bucketObjects should contain (GcsPath(bucketPath, initFile)))

    // check that the service account key was added to the bucket
    gdDAO.bucketObjects should contain (GcsPath(bucketPath, GcsRelativePath(ClusterInitValues.serviceAccountCredentialsFilename)))
  }

  it should "add bucket objects" in isolatedDbTest {
    // since we're only testing adding the objects, we're directly creating the bucket in the mock DAO
    gdDAO.buckets += bucketPath

    // add all the bucket objects
    leo.initializeBucketObjects(googleProject, clusterName, bucketPath, testClusterRequest, Some(serviceAccountKey)).futureValue

    // check that the bucket exists
    gdDAO.buckets should contain (bucketPath)

    // check the init files were added to the bucket
    initFiles.map(initFile => gdDAO.bucketObjects should contain (GcsPath(bucketPath, initFile)))

    // check that the service account key was added to the bucket
    gdDAO.bucketObjects should contain (GcsPath(bucketPath, GcsRelativePath(ClusterInitValues.serviceAccountCredentialsFilename)))
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
    val replacements = ClusterInitValues(googleProject, clusterName, bucketPath, testClusterRequest, dataprocConfig, clusterFilesConfig, clusterResourcesConfig, proxyConfig, swaggerConfig, Some(serviceAccountKey)).toJson.asJsObject.fields

    // Each value in the replacement map will replace it's key in the file being processed
    val result = leo.templateResource(clusterResourcesConfig.initActionsScript, replacements)

    // Check that the values in the bash script file were correctly replaced
    val expected =
      s"""|#!/usr/bin/env bash
          |
          |"${clusterName.string}"
          |"${googleProject.value}"
          |"${proxyConfig.jupyterProxyDockerImage}"
          |"${gdDAO.extensionPath.toUri}"
          |"${GcsPath(bucketPath, GcsRelativePath(ClusterInitValues.serviceAccountCredentialsFilename)).toUri}"""".stripMargin

    result shouldEqual expected
  }

  it should "template google_sign_in.js with config values" in isolatedDbTest {
    // Create replacements map
    val replacements = ClusterInitValues(googleProject, clusterName, bucketPath, testClusterRequest, dataprocConfig, clusterFilesConfig, clusterResourcesConfig, proxyConfig, swaggerConfig, Some(serviceAccountKey)).toJson.asJsObject.fields

    // Each value in the replacement map will replace it's key in the file being processed
    val result = leo.templateResource(clusterResourcesConfig.jupyterGoogleSignInJs, replacements)

    // Check that the values in the bash script file were correctly replaced
    val expected = s""""${swaggerConfig.googleClientId}""""

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
    leo.listClusters(defaultUserInfo, Map.empty).futureValue shouldBe 'empty
    leo.listClusters(defaultUserInfo, Map("foo" -> "bar", "baz" -> "biz")).futureValue shouldBe 'empty
  }

  it should "list all clusters" in isolatedDbTest {
    // create a couple clusters
    val clusterName1 = ClusterName(s"cluster-${UUID.randomUUID.toString}")
    val cluster1 = leo.createCluster(defaultUserInfo, googleProject, clusterName1, testClusterRequest).futureValue

    val clusterName2 = ClusterName(s"cluster-${UUID.randomUUID.toString}")
    val cluster2 = leo.createCluster(defaultUserInfo, googleProject, clusterName2, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar"))).futureValue

    leo.listClusters(defaultUserInfo, Map.empty).futureValue.toSet shouldBe Set(cluster1, cluster2)
  }

  it should "list all active clusters" in isolatedDbTest {
    // create a couple clusters
    val cluster1 = leo.createCluster(defaultUserInfo, googleProject, clusterName, testClusterRequest).futureValue

    val clusterName2 = ClusterName("test-cluster-2")
    val cluster2 = leo.createCluster(defaultUserInfo, googleProject, clusterName2, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar"))).futureValue

    leo.listClusters(defaultUserInfo, Map("includeDeleted" -> "false")).futureValue.toSet shouldBe Set(cluster1, cluster2)
    leo.listClusters(defaultUserInfo, Map.empty).futureValue.toSet shouldBe Set(cluster1, cluster2)
    leo.listClusters(defaultUserInfo, Map.empty).futureValue.toSet shouldBe Set(cluster1, cluster2)

    val clusterName3 = ClusterName("test-cluster-3")
    val cluster3 = leo.createCluster(defaultUserInfo, googleProject, clusterName3, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar"))).futureValue

    dbFutureValue(dataAccess =>
      dataAccess.clusterQuery.completeDeletion(cluster3.googleId, clusterName3)
    )

    leo.listClusters(defaultUserInfo, Map.empty).futureValue.toSet shouldBe Set(cluster1, cluster2)
    leo.listClusters(defaultUserInfo, Map("includeDeleted" -> "false")).futureValue.toSet shouldBe Set(cluster1, cluster2)
    leo.listClusters(defaultUserInfo, Map("includeDeleted" -> "true")).futureValue.toSet.size shouldBe 3
  }


  it should "list clusters with labels" in isolatedDbTest {
    // create a couple clusters
    val clusterName1 = ClusterName(s"cluster-${UUID.randomUUID.toString}")
    val cluster1 = leo.createCluster(defaultUserInfo, googleProject, clusterName1, testClusterRequest).futureValue

    val clusterName2 = ClusterName(s"test-cluster-2")
    val cluster2 = leo.createCluster(defaultUserInfo, googleProject, clusterName2, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar"))).futureValue

    leo.listClusters(defaultUserInfo, Map("foo" -> "bar")).futureValue.toSet shouldBe Set(cluster1, cluster2)
    leo.listClusters(defaultUserInfo, Map("foo" -> "bar", "bam" -> "yes")).futureValue.toSet shouldBe Set(cluster1)
    leo.listClusters(defaultUserInfo, Map("foo" -> "bar", "bam" -> "yes", "vcf" -> "no")).futureValue.toSet shouldBe Set(cluster1)
    leo.listClusters(defaultUserInfo, Map("a" -> "b")).futureValue.toSet shouldBe Set(cluster2)
    leo.listClusters(defaultUserInfo, Map("foo" -> "bar", "baz" -> "biz")).futureValue.toSet shouldBe Set.empty
    leo.listClusters(defaultUserInfo, Map("A" -> "B")).futureValue.toSet shouldBe Set(cluster2)  // labels are not case sensitive because MySQL
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

    leo.listClusters(defaultUserInfo, Map("_labels" -> "foo=bar")).futureValue.toSet shouldBe Set(cluster1, cluster2)
    leo.listClusters(defaultUserInfo, Map("_labels" -> "foo=bar,bam=yes")).futureValue.toSet shouldBe Set(cluster1)
    leo.listClusters(defaultUserInfo, Map("_labels" -> "foo=bar,bam=yes,vcf=no")).futureValue.toSet shouldBe Set(cluster1)
    leo.listClusters(defaultUserInfo, Map("_labels" -> "a=b")).futureValue.toSet shouldBe Set(cluster2)
    leo.listClusters(defaultUserInfo, Map("_labels" -> "baz=biz")).futureValue.toSet shouldBe Set.empty
    leo.listClusters(defaultUserInfo, Map("_labels" -> "A=B")).futureValue.toSet shouldBe Set(cluster2)   // labels are not case sensitive because MySQL
    leo.listClusters(defaultUserInfo, Map("_labels" -> "foo%3Dbar")).failed.futureValue shouldBe a [ParseLabelsException]
    leo.listClusters(defaultUserInfo, Map("_labels" -> "foo=bar;bam=yes")).failed.futureValue shouldBe a [ParseLabelsException]
    leo.listClusters(defaultUserInfo, Map("_labels" -> "foo=bar,bam")).failed.futureValue shouldBe a [ParseLabelsException]
    leo.listClusters(defaultUserInfo, Map("_labels" -> "bogus")).failed.futureValue shouldBe a [ParseLabelsException]
    leo.listClusters(defaultUserInfo, Map("_labels" -> "a,b")).failed.futureValue shouldBe a [ParseLabelsException]
  }

  it should "delete the init bucket if cluster creation fails" in isolatedDbTest {
    // create the cluster
    val clusterCreateResponse = leo.createCluster(defaultUserInfo, googleProject, gdDAO.badClusterName, testClusterRequest).failed.futureValue

    clusterCreateResponse shouldBe a [CallToGoogleApiFailedException]

    // check the firewall rule was created for the project
    gdDAO.firewallRules should contain (googleProject, proxyConfig.firewallRuleName)

    gdDAO.buckets shouldBe 'empty
  }

  it should "tell you if you're whitelisted" in isolatedDbTest {
    leo.isWhitelisted(defaultUserInfo).futureValue
  }

  it should "complain if you're not whitelisted" in isolatedDbTest {
    val badUserInfo = UserInfo(OAuth2BearerToken("accessToken"), WorkbenchUserId("badguy"), WorkbenchEmail("dont@whitelist.me"), 0)
    val authExc = leo.isWhitelisted(badUserInfo).failed.futureValue
    authExc shouldBe a [AuthorizationError]
  }
}
