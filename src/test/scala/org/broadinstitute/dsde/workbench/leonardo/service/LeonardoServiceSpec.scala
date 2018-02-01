package org.broadinstitute.dsde.workbench.leonardo.service

import java.io.ByteArrayInputStream
import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.google.mock.{MockGoogleDataprocDAO, MockGoogleIamDAO, MockGoogleStorageDAO}
import org.broadinstitute.dsde.workbench.leonardo.auth.{MockPetsPerProjectServiceAccountProvider, WhitelistAuthProvider}
import org.broadinstitute.dsde.workbench.leonardo.config.{ClusterDefaultsConfig, ClusterFilesConfig, ClusterResourcesConfig, DataprocConfig, ProxyConfig, SwaggerConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.MockSamDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleExceptionSupport.CallToGoogleApiFailedException
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.leonardo.model.LeonardoJsonSupport._
import org.broadinstitute.dsde.workbench.leonardo.model.MachineConfigOps.{NegativeIntegerArgumentInClusterRequestException, OneWorkerSpecifiedInClusterRequestException}
import org.broadinstitute.dsde.workbench.leonardo.monitor.NoopActor
import org.broadinstitute.dsde.workbench.model._
import org.broadinstitute.dsde.workbench.model.google._
import org.mockito.Mockito.{never, verify}
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import spray.json._
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers.{any, eq => mockitoEq}

import scala.concurrent.{ExecutionContext, Future}

class LeonardoServiceSpec extends TestKit(ActorSystem("leonardotest")) with FlatSpecLike with Matchers with BeforeAndAfter with BeforeAndAfterAll with TestComponent with ScalaFutures with OptionValues {
  private val configFactory = ConfigFactory.load()
  private val whitelist = configFactory.as[Set[String]]("auth.whitelistProviderConfig.whitelist").map(_.toLowerCase)
  private val dataprocConfig = configFactory.as[DataprocConfig]("dataproc")
  private val clusterFilesConfig = configFactory.as[ClusterFilesConfig]("clusterFiles")
  private val clusterResourcesConfig = configFactory.as[ClusterResourcesConfig]("clusterResources")
  private val clusterDefaultsConfig = configFactory.as[ClusterDefaultsConfig]("clusterDefaults")
  private val proxyConfig = configFactory.as[ProxyConfig]("proxy")
  private val swaggerConfig = configFactory.as[SwaggerConfig]("swagger")
  private val initBucketPath = GcsBucketName("bucket-path")
  private val googleProject = GoogleProject("test-google-project")
  private val petServiceAccount = WorkbenchEmail("petSA@test-domain.iam.gserviceaccount.com")
  private val clusterName = ClusterName("test-cluster")
  private val defaultUserInfo = UserInfo(OAuth2BearerToken("accessToken"), WorkbenchUserId("user1"), WorkbenchEmail("user1@example.com"), 0)
  private val jupyterExtensionPath = GcsPath(GcsBucketName("extension_bucket"), GcsObjectName("extension_path"))
  private lazy val testClusterRequest = ClusterRequest(Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"), Some(jupyterExtensionPath))
  private lazy val singleNodeDefaultMachineConfig = MachineConfig(Some(clusterDefaultsConfig.numberOfWorkers), Some(clusterDefaultsConfig.masterMachineType), Some(clusterDefaultsConfig.masterDiskSize))
  private val serviceAccountKey = ServiceAccountKey(ServiceAccountKeyId("123"), ServiceAccountPrivateKeyData("abcdefg"), Some(Instant.now), Some(Instant.now.plusSeconds(300)))

  private var gdDAO: MockGoogleDataprocDAO = _
  private var iamDAO: MockGoogleIamDAO = _
  private var storageDAO: MockGoogleStorageDAO = _
  private var samDAO: MockSamDAO = _
  private var leo: LeonardoService = _
  private var authProvider: LeoAuthProvider = _
  private var serviceAccountProvider: ServiceAccountProvider = _

  before {
    gdDAO = new MockGoogleDataprocDAO
    iamDAO = new MockGoogleIamDAO
    storageDAO = new MockGoogleStorageDAO
    storageDAO.buckets += jupyterExtensionPath.bucketName -> Set((jupyterExtensionPath.objectName, new ByteArrayInputStream("foo".getBytes())))

    samDAO = new MockSamDAO
    // TODO look into parameterized tests so both provider impls can both be tested
    //serviceAccountProvider = new MockPetServiceAccountProvider(configFactory.getConfig("serviceAccounts.config"))
    serviceAccountProvider = new MockPetsPerProjectServiceAccountProvider(configFactory.getConfig("serviceAccounts.config"))
    authProvider = new WhitelistAuthProvider(configFactory.getConfig("auth.whitelistProviderConfig"),serviceAccountProvider)

    leo = new LeonardoService(dataprocConfig, clusterFilesConfig, clusterResourcesConfig, clusterDefaultsConfig, proxyConfig, swaggerConfig, gdDAO, iamDAO, storageDAO, DbSingleton.ref, system.actorOf(NoopActor.props), authProvider, serviceAccountProvider, WorkbenchEmail("leo"), whitelist)
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  lazy val initFiles = List(
    clusterResourcesConfig.clusterDockerCompose.value,
    clusterResourcesConfig.initActionsScript.value,
    clusterFilesConfig.jupyterServerCrt.getName,
    clusterFilesConfig.jupyterServerKey.getName,
    clusterFilesConfig.jupyterRootCaPem.getName,
    clusterResourcesConfig.jupyterProxySiteConf.value,
    clusterResourcesConfig.jupyterCustomJs.value,
    clusterResourcesConfig.jupyterGoogleSignInJs.value
  ) ++ (
    notebookServiceAccount(googleProject).map(_ => List(ClusterInitValues.serviceAccountCredentialsFilename)).getOrElse(List.empty)
  ) map(name => GcsObjectName(name))

  private def clusterServiceAccount(googleProject: GoogleProject): Option[WorkbenchEmail] = {
    serviceAccountProvider.getClusterServiceAccount(defaultUserInfo, googleProject).futureValue
  }

  private def notebookServiceAccount(googleProject: GoogleProject): Option[WorkbenchEmail] = {
    serviceAccountProvider.getNotebookServiceAccount(defaultUserInfo, googleProject).futureValue
  }

  "LeonardoService" should "create a single node cluster with default machine configs" in isolatedDbTest {
    // create the cluster
    val clusterCreateResponse = leo.createCluster(defaultUserInfo, googleProject, clusterName, testClusterRequest).futureValue

    // check the create response has the correct info
    clusterCreateResponse.serviceAccountInfo.clusterServiceAccount shouldEqual clusterServiceAccount(googleProject)
    clusterCreateResponse.serviceAccountInfo.notebookServiceAccount shouldEqual notebookServiceAccount(googleProject)

    // check the cluster has the correct machine configs
    clusterCreateResponse.machineConfig shouldEqual singleNodeDefaultMachineConfig

    // check the firewall rule was created for the project
    gdDAO.firewallRules should contain key (googleProject)
    gdDAO.firewallRules(googleProject).name.value shouldBe proxyConfig.firewallRuleName

    val bucketArray = storageDAO.buckets.keys.filter(_.value.startsWith(clusterName.value))

    // check the bucket was created for the cluster
    bucketArray.size shouldEqual 1

    val bucketObjects = storageDAO.buckets(bucketArray.head).map(_._1).map(objName => GcsPath(bucketArray.head, objName))

    // check the init files were added to the bucket
    initFiles.foreach(initFile => bucketObjects should contain (GcsPath(bucketArray.head, initFile)))
    bucketObjects should contain theSameElementsAs (initFiles.map(GcsPath(bucketArray.head, _)))
    // a service account key should only have been created if using a notebook service account
    if (notebookServiceAccount(googleProject).isDefined) {
      iamDAO.serviceAccountKeys should contain key(samDAO.serviceAccount)
    } else {
      iamDAO.serviceAccountKeys should not contain key(samDAO.serviceAccount)
    }

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
    // need a specialized LeonardoService for this test, so we can spy on its authProvider
    val spyProvider: LeoAuthProvider = spy(authProvider)
    val leoForTest = new LeonardoService(dataprocConfig, clusterFilesConfig, clusterResourcesConfig, clusterDefaultsConfig, proxyConfig, swaggerConfig, gdDAO, iamDAO, storageDAO, DbSingleton.ref, system.actorOf(NoopActor.props), spyProvider, serviceAccountProvider, WorkbenchEmail("leo"), whitelist)

    // check that the cluster does not exist
    gdDAO.clusters should not contain key (clusterName)

    // create the cluster
    val clusterCreateResponse = leoForTest.createCluster(defaultUserInfo, googleProject, clusterName, testClusterRequest).futureValue

    // check that the cluster was created
    gdDAO.clusters should contain key (clusterName)
    // a service account key should only have been created if using a notebook service account
    if (notebookServiceAccount(googleProject).isDefined) {
      iamDAO.serviceAccountKeys should contain key(samDAO.serviceAccount)
    } else {
      iamDAO.serviceAccountKeys should not contain key(samDAO.serviceAccount)
    }

    // delete the cluster
    leoForTest.deleteCluster(defaultUserInfo, googleProject, clusterName).futureValue

    // check that  the cluster no longer exists
    gdDAO.clusters should not contain key (clusterName)
    iamDAO.serviceAccountKeys should not contain key (samDAO.serviceAccount)

    // the cluster has transitioned to the Deleting state (Cluster Monitor will later transition it to Deleted)

    dbFutureValue { _.clusterQuery.getClusterByName(googleProject, clusterName) }.map(_.status) shouldBe Some(ClusterStatus.Deleting)

    // the auth provider should have not yet been notified of deletion
    verify(spyProvider, never).notifyClusterDeleted(mockitoEq(defaultUserInfo.userEmail), mockitoEq(defaultUserInfo.userEmail), mockitoEq(googleProject), mockitoEq(clusterName))(any[ExecutionContext])
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
    storageDAO.buckets should not contain (initBucketPath)

    // create the bucket and add files
    leo.initializeBucket(defaultUserInfo.userEmail, googleProject, clusterName, initBucketPath, testClusterRequest, ServiceAccountInfo(None, Some(petServiceAccount)), Some(serviceAccountKey)).futureValue

    // our bucket should now exist
    storageDAO.buckets should contain key (initBucketPath)

    val bucketObjects = storageDAO.buckets(initBucketPath).map(_._1).map(objName => GcsPath(initBucketPath, objName))

    // check the init files were added to the bucket
    initFiles.map(initFile => bucketObjects should contain (GcsPath(initBucketPath, initFile)))

    // check that the service account key was added to the bucket
    bucketObjects should contain (GcsPath(initBucketPath, GcsObjectName(ClusterInitValues.serviceAccountCredentialsFilename)))
  }

  it should "add bucket objects" in isolatedDbTest {
    // since we're only testing adding the objects, we're directly creating the bucket in the mock DAO
    storageDAO.buckets += initBucketPath -> Set.empty

    // add all the bucket objects
    leo.initializeBucketObjects(defaultUserInfo.userEmail, googleProject, clusterName, initBucketPath, testClusterRequest, Some(serviceAccountKey)).futureValue

    // check that the bucket exists
    storageDAO.buckets should contain key (initBucketPath)

    val bucketObjects = storageDAO.buckets(initBucketPath).map(_._1).map(objName => GcsPath(initBucketPath, objName))

    // check the init files were added to the bucket
    initFiles.map(initFile => bucketObjects should contain (GcsPath(initBucketPath, initFile)))

    // check that the service account key was added to the bucket
    bucketObjects should contain (GcsPath(initBucketPath, GcsObjectName(ClusterInitValues.serviceAccountCredentialsFilename)))
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
    val replacements = ClusterInitValues(googleProject, clusterName, initBucketPath, testClusterRequest, dataprocConfig, clusterFilesConfig, clusterResourcesConfig, proxyConfig, Some(serviceAccountKey), defaultUserInfo.userEmail).toJson.asJsObject.fields

    // Each value in the replacement map will replace it's key in the file being processed
    val result = leo.templateResource(clusterResourcesConfig.initActionsScript, replacements)

    // Check that the values in the bash script file were correctly replaced
    val expected =
      s"""|#!/usr/bin/env bash
          |
          |"${clusterName.value}"
          |"${googleProject.value}"
          |"${proxyConfig.jupyterProxyDockerImage}"
          |"${jupyterExtensionPath.toUri}"
          |"${GcsPath(initBucketPath, GcsObjectName(ClusterInitValues.serviceAccountCredentialsFilename)).toUri}"""".stripMargin

    result shouldEqual expected
  }

  it should "template google_sign_in.js with config values" in isolatedDbTest {
    // Create replacements map
    val replacements = ClusterInitValues(googleProject, clusterName, initBucketPath, testClusterRequest, dataprocConfig, clusterFilesConfig, clusterResourcesConfig, proxyConfig, Some(serviceAccountKey), defaultUserInfo.userEmail).toJson.asJsObject.fields

    // Each value in the replacement map will replace it's key in the file being processed
    val result = leo.templateResource(clusterResourcesConfig.jupyterGoogleSignInJs, replacements)

    // Check that the values in the bash script file were correctly replaced
    val expected =
      s""""${defaultUserInfo.userEmail.value}""""

    result shouldEqual expected
  }

  it should "throw a JupyterExtensionException when the extensionUri is too long" in isolatedDbTest {
    val jupyterExtensionUri = GcsPath(GcsBucketName("bucket"), GcsObjectName(Stream.continually('a').take(1025).mkString))

    // create the cluster
    val response = leo.createCluster(defaultUserInfo, googleProject, clusterName, testClusterRequest.copy(jupyterExtensionUri = Some(jupyterExtensionUri))).failed.futureValue

    response shouldBe a [JupyterExtensionException]
  }

  it should "throw a JupyterExtensionException when the jupyterExtensionUri does not point to a GCS object" in isolatedDbTest {
    val jupyterExtensionUri = parseGcsPath("gs://bogus/object.tar.gz").right.get

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

  it should "filter out auth provider exceptions from list clusters" in isolatedDbTest {
    // create a couple clusters
    val clusterName1 = ClusterName(s"cluster-${UUID.randomUUID.toString}")
    val cluster1 = leo.createCluster(defaultUserInfo, googleProject, clusterName1, testClusterRequest).futureValue

    val clusterName2 = ClusterName(s"cluster-${UUID.randomUUID.toString}")
    val cluster2 = leo.createCluster(defaultUserInfo, googleProject, clusterName2, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar"))).futureValue

    // provider fails on cluster2, succeeds on cluster1
    val newAuthProvider = new WhitelistAuthProvider(configFactory.getConfig("auth.whitelistProviderConfig"),serviceAccountProvider) {
      override def hasNotebookClusterPermission(userEmail: WorkbenchEmail, action: NotebookClusterActions.NotebookClusterAction, googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Boolean] = {
        if (clusterName == clusterName1) {
          super.hasNotebookClusterPermission(userEmail, action, googleProject, clusterName)
        } else {
          Future.failed(new RuntimeException)
        }
      }
    }

    // make a new LeoService
    val newLeo = new LeonardoService(dataprocConfig, clusterFilesConfig, clusterResourcesConfig, clusterDefaultsConfig, proxyConfig, swaggerConfig, gdDAO, iamDAO, storageDAO, DbSingleton.ref, system.actorOf(NoopActor.props), newAuthProvider, serviceAccountProvider, WorkbenchEmail("leo"), whitelist)

    // list clusters should only return cluster1
    newLeo.listClusters(defaultUserInfo, Map.empty).futureValue shouldBe Seq(cluster1)
  }

  it should "delete the init bucket if cluster creation fails" in isolatedDbTest {
    // create the cluster
    val clusterCreateResponse = leo.createCluster(defaultUserInfo, googleProject, gdDAO.badClusterName, testClusterRequest).failed.futureValue

    clusterCreateResponse shouldBe a [Exception] // thrown by MockGoogleDataprocDAO

    // check the firewall rule was created for the project
    gdDAO.firewallRules should contain key (googleProject)
    gdDAO.firewallRules(googleProject).name.value shouldBe proxyConfig.firewallRuleName

    (storageDAO.buckets - jupyterExtensionPath.bucketName) shouldBe 'empty
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
