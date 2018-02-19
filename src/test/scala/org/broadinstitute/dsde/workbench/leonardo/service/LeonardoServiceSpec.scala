package org.broadinstitute.dsde.workbench.leonardo.service

import java.io.ByteArrayInputStream
import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.testkit.TestKit
import org.broadinstitute.dsde.workbench.google.mock.{MockGoogleDataprocDAO, MockGoogleIamDAO, MockGoogleStorageDAO}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData
import org.broadinstitute.dsde.workbench.leonardo.auth.{MockPetClusterServiceAccountProvider, MockSwaggerSamClient, WhitelistAuthProvider}
import org.broadinstitute.dsde.workbench.leonardo.dao.google.MockGoogleComputeDAO
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.model.LeonardoJsonSupport._
import org.broadinstitute.dsde.workbench.leonardo.model.MachineConfigOps.{NegativeIntegerArgumentInClusterRequestException, OneWorkerSpecifiedInClusterRequestException}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.leonardo.monitor.NoopActor
import org.broadinstitute.dsde.workbench.model._
import org.broadinstitute.dsde.workbench.model.google._
import org.mockito.ArgumentMatchers.{any, eq => mockitoEq}
import org.mockito.Mockito.{never, verify, _}
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import spray.json._

import scala.concurrent.{ExecutionContext, Future}

class LeonardoServiceSpec extends TestKit(ActorSystem("leonardotest")) with FlatSpecLike with Matchers with BeforeAndAfter with BeforeAndAfterAll with TestComponent with ScalaFutures with OptionValues with CommonTestData {
  private var gdDAO: MockGoogleDataprocDAO = _
  private var computeDAO: MockGoogleComputeDAO = _
  private var iamDAO: MockGoogleIamDAO = _
  private var storageDAO: MockGoogleStorageDAO = _
  private var samClient: MockSwaggerSamClient = _
  private var bucketHelper: BucketHelper = _
  private var leo: LeonardoService = _
  private var authProvider: LeoAuthProvider = _

  before {
    gdDAO = new MockGoogleDataprocDAO
    computeDAO = new MockGoogleComputeDAO
    iamDAO = new MockGoogleIamDAO
    storageDAO = new MockGoogleStorageDAO
    // Pre-populate the juptyer extenion bucket in the mock storage DAO, as it is passed in some requests
    storageDAO.buckets += jupyterExtensionUri.bucketName -> Set((jupyterExtensionUri.objectName, new ByteArrayInputStream("foo".getBytes())))

    samClient = serviceAccountProvider.asInstanceOf[MockPetClusterServiceAccountProvider].mockSwaggerSamClient
    authProvider = new WhitelistAuthProvider(whitelistAuthConfig, serviceAccountProvider)

    bucketHelper = new BucketHelper(dataprocConfig, gdDAO, computeDAO, storageDAO, serviceAccountProvider)
    leo = new LeonardoService(dataprocConfig, clusterFilesConfig, clusterResourcesConfig, clusterDefaultsConfig, proxyConfig, swaggerConfig, gdDAO, computeDAO, iamDAO, storageDAO, DbSingleton.ref, system.actorOf(NoopActor.props), authProvider, serviceAccountProvider, whitelist, bucketHelper)
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
    notebookServiceAccount(project).map(_ => List(ClusterInitValues.serviceAccountCredentialsFilename)).getOrElse(List.empty)
  ) map(name => GcsObjectName(name))

  "LeonardoService" should "create a single node cluster with default machine configs" in isolatedDbTest {
    // create the cluster
    val clusterCreateResponse = leo.createCluster(userInfo, project, name1, testClusterRequest).futureValue

    // check the create response has the correct info
    clusterCreateResponse.serviceAccountInfo.clusterServiceAccount shouldEqual clusterServiceAccount(project)
    clusterCreateResponse.serviceAccountInfo.notebookServiceAccount shouldEqual notebookServiceAccount(project)

    // check the cluster has the correct machine configs
    clusterCreateResponse.machineConfig shouldEqual singleNodeDefaultMachineConfig

    // check the firewall rule was created for the project
    computeDAO.firewallRules should contain key (project)
    computeDAO.firewallRules(project).name.value shouldBe proxyConfig.firewallRuleName

    // should have created init and staging buckets
    val initBucketOpt = storageDAO.buckets.keys.find(_.value.startsWith(name1.value+"-init"))
    initBucketOpt shouldBe 'defined

    val stagingBucketOpt = storageDAO.buckets.keys.find(_.value.startsWith(name1.value+"-staging"))
    stagingBucketOpt shouldBe 'defined

    // check the init files were added to the init bucket
    val initBucketObjects = storageDAO.buckets(initBucketOpt.get).map(_._1).map(objName => GcsPath(initBucketOpt.get, objName))
    initFiles.foreach(initFile => initBucketObjects should contain (GcsPath(initBucketOpt.get, initFile)))
    initBucketObjects should contain theSameElementsAs (initFiles.map(GcsPath(initBucketOpt.get, _)))

    // a service account key should only have been created if using a notebook service account
    if (notebookServiceAccount(project).isDefined) {
      iamDAO.serviceAccountKeys should contain key(samClient.serviceAccount)
    } else {
      iamDAO.serviceAccountKeys should not contain key(samClient.serviceAccount)
    }

    val dbInitBucketOpt = dbFutureValue { dataAccess =>
      dataAccess.clusterQuery.getInitBucket(project, name1)
    }
    dbInitBucketOpt shouldBe 'defined
  }

  it should "create and get a cluster" in isolatedDbTest {
    // create the cluster
    val clusterCreateResponse = leo.createCluster(userInfo, project, name1, testClusterRequest).futureValue

    // get the cluster detail
    val clusterGetResponse = leo.getActiveClusterDetails(userInfo, project, name1).futureValue

    // check the create response and get response are the same
    clusterCreateResponse shouldEqual clusterGetResponse
  }

  it should "create a single node cluster with an empty machine config" in isolatedDbTest {
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = Some(MachineConfig()))
    val clusterCreateResponse = leo.createCluster(userInfo, project, name1, clusterRequestWithMachineConfig).futureValue
    clusterCreateResponse.machineConfig shouldEqual singleNodeDefaultMachineConfig
  }

  it should "create a single node cluster with zero workers explicitly defined in machine config" in isolatedDbTest {
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = Some(MachineConfig(Some(0))))
    val clusterCreateResponse = leo.createCluster(userInfo, project, name1, clusterRequestWithMachineConfig).futureValue
    clusterCreateResponse.machineConfig shouldEqual singleNodeDefaultMachineConfig
  }

  it should "create a single node cluster with master configs defined" in isolatedDbTest {
    val singleNodeDefinedMachineConfig = MachineConfig(Some(0), Some("test-master-machine-type2"), Some(200))
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = Some(singleNodeDefinedMachineConfig))
    val clusterCreateResponse = leo.createCluster(userInfo, project, name1, clusterRequestWithMachineConfig).futureValue
    clusterCreateResponse.machineConfig shouldEqual singleNodeDefinedMachineConfig
  }

  it should "create a single node cluster and override worker configs" in isolatedDbTest {
    // machine config is creating a single node cluster, but has worker configs defined
    val machineConfig = Some(MachineConfig(Some(0), Some("test-master-machine-type3"), Some(200), Some("test-worker-machine-type"), Some(10), Some(3), Some(4)))
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = machineConfig)

    val clusterCreateResponse = leo.createCluster(userInfo, project, name1, clusterRequestWithMachineConfig).futureValue
    clusterCreateResponse.machineConfig shouldEqual MachineConfig(Some(0), Some("test-master-machine-type3"), Some(200))
  }

  it should "create a standard cluster with 2 workers with default worker configs" in isolatedDbTest {
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = Some(MachineConfig(Some(2))))

    val clusterCreateResponse = leo.createCluster(userInfo, project, name1, clusterRequestWithMachineConfig).futureValue
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

    val clusterCreateResponse = leo.createCluster(userInfo, project, name1, clusterRequestWithMachineConfig).futureValue
    clusterCreateResponse.machineConfig shouldEqual machineConfig

  }

  it should "create a standard cluster with 2 workers and override too-small disk sizes with minimum disk size" in isolatedDbTest {
    val machineConfig = MachineConfig(Some(2), Some("test-master-machine-type"), Some(50), Some("test-worker-machine-type"), Some(10), Some(3), Some(4))
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = Some(machineConfig))

    val clusterCreateResponse = leo.createCluster(userInfo, project, name1, clusterRequestWithMachineConfig).futureValue
    clusterCreateResponse.machineConfig shouldEqual MachineConfig(Some(2), Some("test-master-machine-type"), Some(100), Some("test-worker-machine-type"), Some(100), Some(3), Some(4))
  }

  it should "throw OneWorkerSpecifiedInClusterRequestException when create a 1 worker cluster" in isolatedDbTest {
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = Some(MachineConfig(Some(1))))

    whenReady(leo.createCluster(userInfo, project, name1, clusterRequestWithMachineConfig).failed) { exc =>
      exc shouldBe a[OneWorkerSpecifiedInClusterRequestException]
    }
  }

  it should "throw NegativeIntegerArgumentInClusterRequestException when master disk size in single node cluster request is a negative integer" in isolatedDbTest {
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = Some(MachineConfig(Some(0), Some("test-worker-machine-type"), Some(-30))))

    whenReady(leo.createCluster(userInfo, project, name1, clusterRequestWithMachineConfig).failed) { exc =>
      exc shouldBe a[NegativeIntegerArgumentInClusterRequestException]
    }
  }

  it should "throw NegativeIntegerArgumentInClusterRequestException when number of preemptible workers in a 2 worker cluster request is a negative integer" in isolatedDbTest {
    val machineConfig = MachineConfig(Some(10), Some("test-master-machine-type"), Some(200), Some("test-worker-machine-type"), Some(300), Some(3), Some(-1))
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = Option(machineConfig))

    whenReady(leo.createCluster(userInfo, project, name1, clusterRequestWithMachineConfig).failed) { exc =>
      exc shouldBe a[NegativeIntegerArgumentInClusterRequestException]
    }
  }

  it should "throw ClusterNotFoundException for nonexistent clusters" in isolatedDbTest {
    whenReady( leo.getActiveClusterDetails(userInfo, GoogleProject("nonexistent"), ClusterName("cluster")).failed ) { exc =>
      exc shouldBe a [ClusterNotFoundException]
    }
  }

  it should "throw ClusterAlreadyExistsException when creating a cluster with same name and project as an existing cluster" in isolatedDbTest {
    // create the first cluster
    leo.createCluster(userInfo, project, name1, testClusterRequest).futureValue

    // creating the same cluster again should throw a ClusterAlreadyExistsException
    whenReady( leo.createCluster(userInfo, project, name1, testClusterRequest).failed ) { exc =>
      exc shouldBe a [ClusterAlreadyExistsException]
    }
  }

  it should "create two clusters with same name with only one active" in isolatedDbTest {
    //create first cluster
    leo.createCluster(userInfo, project, name1, testClusterRequest).futureValue

    // check that the cluster was created
    gdDAO.clusters should contain key (name1)

    // delete the cluster
    leo.deleteCluster(userInfo, project, name1).futureValue

    //recreate cluster with same project and cluster name
    leo.createCluster(userInfo, project, name1, testClusterRequest).futureValue

    //confirm cluster was created
    gdDAO.clusters should contain key (name1)
  }

  it should "delete a cluster" in isolatedDbTest {
    // need a specialized LeonardoService for this test, so we can spy on its authProvider
    val spyProvider: LeoAuthProvider = spy(authProvider)
    val leoForTest = new LeonardoService(dataprocConfig, clusterFilesConfig, clusterResourcesConfig, clusterDefaultsConfig, proxyConfig, swaggerConfig, gdDAO, computeDAO, iamDAO, storageDAO, DbSingleton.ref, system.actorOf(NoopActor.props), spyProvider, serviceAccountProvider, whitelist, bucketHelper)

    // check that the cluster does not exist
    gdDAO.clusters should not contain key (name1)

    // create the cluster
    val clusterCreateResponse = leoForTest.createCluster(userInfo, project, name1, testClusterRequest).futureValue

    // check that the cluster was created
    gdDAO.clusters should contain key (name1)
    // a service account key should only have been created if using a notebook service account
    if (notebookServiceAccount(project).isDefined) {
      iamDAO.serviceAccountKeys should contain key(samClient.serviceAccount)
    } else {
      iamDAO.serviceAccountKeys should not contain key(samClient.serviceAccount)
    }

    // delete the cluster
    leoForTest.deleteCluster(userInfo, project, name1).futureValue

    // check that  the cluster no longer exists
    gdDAO.clusters should not contain key (name1)
    iamDAO.serviceAccountKeys should not contain key (samClient.serviceAccount)

    // the cluster has transitioned to the Deleting state (Cluster Monitor will later transition it to Deleted)

    dbFutureValue { _.clusterQuery.getClusterByName(project, name1) }.map(_.status) shouldBe Some(ClusterStatus.Deleting)

    // the auth provider should have not yet been notified of deletion
    verify(spyProvider, never).notifyClusterDeleted(mockitoEq(userInfo.userEmail), mockitoEq(userInfo.userEmail), mockitoEq(project), mockitoEq(name1))(any[ExecutionContext])
  }

  it should "delete a cluster that has status Error" in isolatedDbTest {
    // check that the cluster does not exist
    gdDAO.clusters should not contain key (gdDAO.errorClusterName)

    // create the cluster
    val clusterCreateResponse = leo.createCluster(userInfo, project, gdDAO.errorClusterName, testClusterRequest).futureValue

    // check that the cluster was created
    gdDAO.clusters should contain key gdDAO.errorClusterName

    // delete the cluster
    leo.deleteCluster(userInfo, project, gdDAO.errorClusterName).futureValue

    // check that the cluster no longer exists
    gdDAO.clusters should not contain key (gdDAO.errorClusterName)
  }

  it should "delete a cluster's instances" in isolatedDbTest {
    // check that the cluster does not exist
    gdDAO.clusters should not contain key (name1)

    // create the cluster
    val clusterCreateResponse = leo.createCluster(userInfo, project, name1, testClusterRequest).futureValue

    // check that the cluster was created
    gdDAO.clusters should contain key name1

    // populate some instances for the cluster
    dbFutureValue { _.instanceQuery.saveAllForCluster(getClusterId(clusterCreateResponse.googleId), Seq(masterInstance, workerInstance1, workerInstance2)) }

    // delete the cluster
    leo.deleteCluster(userInfo, project, name1).futureValue

    // check that the cluster no longer exists
    gdDAO.clusters should not contain key (name1)

    // check that the instances' status are Deleting in the DB
    val instances = dbFutureValue { _.instanceQuery.getAllForCluster(getClusterId(clusterCreateResponse.googleId)) }
    instances.size shouldBe 3
    instances.map(_.status).toSet shouldBe Set(InstanceStatus.Deleting)
  }

  it should "throw ClusterNotFoundException when deleting non existent clusters" in isolatedDbTest {
    whenReady( leo.deleteCluster(userInfo, GoogleProject("nonexistent"), ClusterName("cluster")).failed ) { exc =>
      exc shouldBe a [ClusterNotFoundException]
    }
  }

  it should "initialize bucket with correct files" in isolatedDbTest {
    // create the bucket and add files
    val bucket = bucketHelper.createInitBucket(project, name1, ServiceAccountInfo(None, Some(serviceAccountEmail))).futureValue
    leo.initializeBucketObjects(userInfo.userEmail, project, name1, bucket, testClusterRequest, Some(serviceAccountKey)).futureValue

    // our bucket should now exist
    storageDAO.buckets should contain key (bucket)

    val bucketObjects = storageDAO.buckets(bucket).map(_._1).map(objName => GcsPath(bucket, objName))

    // check the init files were added to the bucket
    initFiles.map(initFile => bucketObjects should contain (GcsPath(bucket, initFile)))

    // check that the service account key was added to the bucket
    bucketObjects should contain (GcsPath(bucket, GcsObjectName(ClusterInitValues.serviceAccountCredentialsFilename)))
  }

  it should "create a firewall rule in a project only once when the first cluster is added" in isolatedDbTest {
    val clusterName2 = ClusterName("test-cluster-2")

    // Our google project should have no firewall rules
    computeDAO.firewallRules should not contain (project, proxyConfig.firewallRuleName)

    // create the first cluster, this should create a firewall rule in our project
    leo.createCluster(userInfo, project, name1, testClusterRequest).futureValue

    // check that there is exactly 1 firewall rule for our project
    computeDAO.firewallRules.filterKeys(_ == project) should have size 1

    // create the second cluster. This should check that our project has a firewall rule and not try to add it again
    leo.createCluster(userInfo, project, clusterName2, testClusterRequest).futureValue

    // check that there is still exactly 1 firewall rule in our project
    computeDAO.firewallRules.filterKeys(_ == project) should have size 1
  }

  it should "template a script using config values" in isolatedDbTest {
    // Create replacements map
    val replacements = ClusterInitValues(project, name1, initBucketPath, testClusterRequestWithExtensionAndScript, dataprocConfig, clusterFilesConfig, clusterResourcesConfig, proxyConfig, Some(serviceAccountKey), userInfo.userEmail).toJson.asJsObject.fields

    // Each value in the replacement map will replace it's key in the file being processed
    val result = leo.templateResource(clusterResourcesConfig.initActionsScript, replacements)

    // Check that the values in the bash script file were correctly replaced
    val expected =
      s"""|#!/usr/bin/env bash
          |
          |"${name1.value}"
          |"${project.value}"
          |"${proxyConfig.jupyterProxyDockerImage}"
          |"${jupyterExtensionUri.toUri}"
          |"${jupyterUserScriptUri.toUri}"
          |"${GcsPath(initBucketPath, GcsObjectName(ClusterInitValues.serviceAccountCredentialsFilename)).toUri}"""".stripMargin

    result shouldEqual expected
  }

  it should "template google_sign_in.js with config values" in isolatedDbTest {
    // Create replacements map
    val replacements = ClusterInitValues(project, name1, initBucketPath, testClusterRequest, dataprocConfig, clusterFilesConfig, clusterResourcesConfig, proxyConfig, Some(serviceAccountKey), userInfo.userEmail).toJson.asJsObject.fields

    // Each value in the replacement map will replace it's key in the file being processed
    val result = leo.templateResource(clusterResourcesConfig.jupyterGoogleSignInJs, replacements)

    // Check that the values in the bash script file were correctly replaced
    val expected =
      s""""${userInfo.userEmail.value}""""

    result shouldEqual expected
  }

  it should "throw a JupyterExtensionException when the extensionUri is too long" in isolatedDbTest {
    val jupyterExtensionUri = GcsPath(GcsBucketName("bucket"), GcsObjectName(Stream.continually('a').take(1025).mkString))

    // create the cluster
    val response = leo.createCluster(userInfo, project, name1, testClusterRequest.copy(jupyterExtensionUri = Some(jupyterExtensionUri))).failed.futureValue

    response shouldBe a [BucketObjectException]
  }

  it should "throw a JupyterExtensionException when the jupyterExtensionUri does not point to a GCS object" in isolatedDbTest {
    val jupyterExtensionUri = parseGcsPath("gs://bogus/object.tar.gz").right.get

    // create the cluster
    val response = leo.createCluster(userInfo, project, name1, testClusterRequest.copy(jupyterExtensionUri = Some(jupyterExtensionUri))).failed.futureValue

    response shouldBe a [BucketObjectException]
  }

  it should "list no clusters" in isolatedDbTest {
    leo.listClusters(userInfo, Map.empty).futureValue shouldBe 'empty
    leo.listClusters(userInfo, Map("foo" -> "bar", "baz" -> "biz")).futureValue shouldBe 'empty
  }

  it should "list all clusters" in isolatedDbTest {
    // create a couple clusters
    val clusterName1 = ClusterName(s"cluster-${UUID.randomUUID.toString}")
    val cluster1 = leo.createCluster(userInfo, project, clusterName1, testClusterRequest).futureValue

    val clusterName2 = ClusterName(s"cluster-${UUID.randomUUID.toString}")
    val cluster2 = leo.createCluster(userInfo, project, clusterName2, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar"))).futureValue

    leo.listClusters(userInfo, Map.empty).futureValue.toSet shouldBe Set(cluster1, cluster2)
  }

  it should "list all active clusters" in isolatedDbTest {
    // create a couple clusters
    val cluster1 = leo.createCluster(userInfo, project, name1, testClusterRequest).futureValue

    val clusterName2 = ClusterName("test-cluster-2")
    val cluster2 = leo.createCluster(userInfo, project, clusterName2, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar"))).futureValue

    leo.listClusters(userInfo, Map("includeDeleted" -> "false")).futureValue.toSet shouldBe Set(cluster1, cluster2)
    leo.listClusters(userInfo, Map.empty).futureValue.toSet shouldBe Set(cluster1, cluster2)
    leo.listClusters(userInfo, Map.empty).futureValue.toSet shouldBe Set(cluster1, cluster2)

    val clusterName3 = ClusterName("test-cluster-3")
    val cluster3 = leo.createCluster(userInfo, project, clusterName3, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar"))).futureValue

    dbFutureValue(dataAccess =>
      dataAccess.clusterQuery.completeDeletion(cluster3.googleId)
    )

    leo.listClusters(userInfo, Map.empty).futureValue.toSet shouldBe Set(cluster1, cluster2)
    leo.listClusters(userInfo, Map("includeDeleted" -> "false")).futureValue.toSet shouldBe Set(cluster1, cluster2)
    leo.listClusters(userInfo, Map("includeDeleted" -> "true")).futureValue.toSet.size shouldBe 3
  }


  it should "list clusters with labels" in isolatedDbTest {
    // create a couple clusters
    val clusterName1 = ClusterName(s"cluster-${UUID.randomUUID.toString}")
    val cluster1 = leo.createCluster(userInfo, project, clusterName1, testClusterRequest).futureValue

    val clusterName2 = ClusterName(s"test-cluster-2")
    val cluster2 = leo.createCluster(userInfo, project, clusterName2, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar"))).futureValue

    leo.listClusters(userInfo, Map("foo" -> "bar")).futureValue.toSet shouldBe Set(cluster1, cluster2)
    leo.listClusters(userInfo, Map("foo" -> "bar", "bam" -> "yes")).futureValue.toSet shouldBe Set(cluster1)
    leo.listClusters(userInfo, Map("foo" -> "bar", "bam" -> "yes", "vcf" -> "no")).futureValue.toSet shouldBe Set(cluster1)
    leo.listClusters(userInfo, Map("a" -> "b")).futureValue.toSet shouldBe Set(cluster2)
    leo.listClusters(userInfo, Map("foo" -> "bar", "baz" -> "biz")).futureValue.toSet shouldBe Set.empty
    leo.listClusters(userInfo, Map("A" -> "B")).futureValue.toSet shouldBe Set(cluster2)  // labels are not case sensitive because MySQL
  }

  it should "throw IllegalLabelKeyException when using a forbidden label" in isolatedDbTest {
    // cluster should not be allowed to have a label with key of "includeDeleted"
    val includeDeletedResponse = leo.createCluster(userInfo, project, name1, testClusterRequest.copy(labels = Map("includeDeleted" -> "val"))).failed.futureValue
    includeDeletedResponse shouldBe a [IllegalLabelKeyException]
  }

  it should "list clusters with swagger-style labels" in isolatedDbTest {
    // create a couple clusters
    val clusterName1 = ClusterName(s"cluster-${UUID.randomUUID.toString}")
    val cluster1 = leo.createCluster(userInfo, project, clusterName1, testClusterRequest).futureValue

    val clusterName2 = ClusterName(s"cluster-${UUID.randomUUID.toString}")
    val cluster2 = leo.createCluster(userInfo, project, clusterName2, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar"))).futureValue

    leo.listClusters(userInfo, Map("_labels" -> "foo=bar")).futureValue.toSet shouldBe Set(cluster1, cluster2)
    leo.listClusters(userInfo, Map("_labels" -> "foo=bar,bam=yes")).futureValue.toSet shouldBe Set(cluster1)
    leo.listClusters(userInfo, Map("_labels" -> "foo=bar,bam=yes,vcf=no")).futureValue.toSet shouldBe Set(cluster1)
    leo.listClusters(userInfo, Map("_labels" -> "a=b")).futureValue.toSet shouldBe Set(cluster2)
    leo.listClusters(userInfo, Map("_labels" -> "baz=biz")).futureValue.toSet shouldBe Set.empty
    leo.listClusters(userInfo, Map("_labels" -> "A=B")).futureValue.toSet shouldBe Set(cluster2)   // labels are not case sensitive because MySQL
    leo.listClusters(userInfo, Map("_labels" -> "foo%3Dbar")).failed.futureValue shouldBe a [ParseLabelsException]
    leo.listClusters(userInfo, Map("_labels" -> "foo=bar;bam=yes")).failed.futureValue shouldBe a [ParseLabelsException]
    leo.listClusters(userInfo, Map("_labels" -> "foo=bar,bam")).failed.futureValue shouldBe a [ParseLabelsException]
    leo.listClusters(userInfo, Map("_labels" -> "bogus")).failed.futureValue shouldBe a [ParseLabelsException]
    leo.listClusters(userInfo, Map("_labels" -> "a,b")).failed.futureValue shouldBe a [ParseLabelsException]
  }

  it should "filter out auth provider exceptions from list clusters" in isolatedDbTest {
    // create a couple clusters
    val clusterName1 = ClusterName(s"cluster-${UUID.randomUUID.toString}")
    val cluster1 = leo.createCluster(userInfo, project, clusterName1, testClusterRequest).futureValue

    val clusterName2 = ClusterName(s"cluster-${UUID.randomUUID.toString}")
    val cluster2 = leo.createCluster(userInfo, project, clusterName2, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar"))).futureValue

    // provider fails on cluster2, succeeds on cluster1
    val newAuthProvider = new WhitelistAuthProvider(whitelistAuthConfig, serviceAccountProvider) {
      override def hasNotebookClusterPermission(userEmail: WorkbenchEmail, action: NotebookClusterActions.NotebookClusterAction, googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Boolean] = {
        if (clusterName == clusterName1) {
          super.hasNotebookClusterPermission(userEmail, action, googleProject, clusterName)
        } else {
          Future.failed(new RuntimeException)
        }
      }
    }

    // make a new LeoService
    val newLeo = new LeonardoService(dataprocConfig, clusterFilesConfig, clusterResourcesConfig, clusterDefaultsConfig, proxyConfig, swaggerConfig, gdDAO, computeDAO, iamDAO, storageDAO, DbSingleton.ref, system.actorOf(NoopActor.props), newAuthProvider, serviceAccountProvider, whitelist, bucketHelper)

    // list clusters should only return cluster1
    newLeo.listClusters(userInfo, Map.empty).futureValue shouldBe Seq(cluster1)
  }

  it should "delete the init bucket if cluster creation fails" in isolatedDbTest {
    // create the cluster
    val clusterCreateResponse = leo.createCluster(userInfo, project, gdDAO.badClusterName, testClusterRequest).failed.futureValue

    clusterCreateResponse shouldBe a [Exception] // thrown by MockGoogleDataprocDAO

    // check the firewall rule was created for the project
    computeDAO.firewallRules should contain key (project)
    computeDAO.firewallRules(project).name.value shouldBe proxyConfig.firewallRuleName

    //staging bucket lives on!
    storageDAO.buckets.keys.find(bucket => bucket.value.contains("-init")).size shouldBe 0
    storageDAO.buckets.keys.find(bucket => bucket.value.contains("-staging")).size shouldBe 1
  }

  it should "tell you if you're whitelisted" in isolatedDbTest {
    leo.isWhitelisted(userInfo).futureValue
  }

  it should "complain if you're not whitelisted" in isolatedDbTest {
    val badUserInfo = UserInfo(OAuth2BearerToken("accessToken"), WorkbenchUserId("badguy"), WorkbenchEmail("dont@whitelist.me"), 0)
    val authExc = leo.isWhitelisted(badUserInfo).failed.futureValue
    authExc shouldBe a [AuthorizationError]
  }

  it should "stop a cluster" in isolatedDbTest {
    // check that the cluster does not exist
    gdDAO.clusters should not contain key (name1)

    // create the cluster
    val clusterCreateResponse = leo.createCluster(userInfo, project, name1, testClusterRequest).futureValue

    // check that the cluster was created
    gdDAO.clusters should contain key name1

    // populate some instances for the cluster
    dbFutureValue { _.instanceQuery.saveAllForCluster(getClusterId(clusterCreateResponse.googleId), Seq(masterInstance, workerInstance1, workerInstance2)) }

    // stop the cluster
    leo.stopCluster(userInfo, project, name1).futureValue

    // cluster should still exist in Google
    gdDAO.clusters should contain key (name1)

    // cluster status should be Stopping in the DB
    dbFutureValue { _.clusterQuery.getByGoogleId(clusterCreateResponse.googleId) }.get.status shouldBe ClusterStatus.Stopping

    // instance status should still be Running in the DB
    // the ClusterMonitorActor is what updates instance status
    val instances = dbFutureValue { _.instanceQuery.getAllForCluster(getClusterId(clusterCreateResponse.googleId)) }
    instances.size shouldBe 3
    instances.map(_.status).toSet shouldBe Set(InstanceStatus.Running)
  }

  it should "start a cluster" in isolatedDbTest {
    // check that the cluster does not exist
    gdDAO.clusters should not contain key (name1)

    // create the cluster
    val clusterCreateResponse = leo.createCluster(userInfo, project, name1, testClusterRequest).futureValue

    // check that the cluster was created
    gdDAO.clusters should contain key name1

    // populate some instances for the cluster and set its status to Stopped
    dbFutureValue { _.instanceQuery.saveAllForCluster(getClusterId(clusterCreateResponse.googleId), Seq(masterInstance, workerInstance1, workerInstance2).map(_.copy(status = InstanceStatus.Stopped))) }
    dbFutureValue { _.clusterQuery.updateClusterStatus(clusterCreateResponse.googleId, ClusterStatus.Stopped) }

    // start the cluster
    leo.startCluster(userInfo, project, name1).futureValue

    // cluster should still exist in Google
    gdDAO.clusters should contain key (name1)

    // cluster status should be Starting in the DB
    dbFutureValue { _.clusterQuery.getByGoogleId(clusterCreateResponse.googleId) }.get.status shouldBe ClusterStatus.Starting

    // instance status should still be Stopped in the DB
    // the ClusterMonitorActor is what updates instance status
    val instances = dbFutureValue { _.instanceQuery.getAllForCluster(getClusterId(clusterCreateResponse.googleId)) }
    instances.size shouldBe 3
    instances.map(_.status).toSet shouldBe Set(InstanceStatus.Stopped)
  }
}
