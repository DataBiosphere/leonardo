package org.broadinstitute.dsde.workbench.leonardo.service

import java.io.ByteArrayInputStream
import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.google.api.client.googleapis.testing.json.GoogleJsonResponseExceptionFactoryTesting
import com.google.api.client.testing.json.MockJsonFactory
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.google.GoogleStorageDAO
import org.broadinstitute.dsde.workbench.google.mock.{MockGoogleDataprocDAO, MockGoogleIamDAO, MockGoogleProjectDAO, MockGoogleStorageDAO}
import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.stripFieldsForListCluster
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData
import org.broadinstitute.dsde.workbench.leonardo.auth.WhitelistAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.auth.sam.{MockPetClusterServiceAccountProvider, MockSwaggerSamClient}
import org.broadinstitute.dsde.workbench.leonardo.dao.google.MockGoogleComputeDAO
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, LeoComponent, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.model.MachineConfigOps.{NegativeIntegerArgumentInClusterRequestException, OneWorkerSpecifiedInClusterRequestException}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterStatus.Stopped
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.leonardo.util.BucketHelper
import org.broadinstitute.dsde.workbench.model._
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.util.Retry
import org.mockito.ArgumentMatchers.{any, eq => mockitoEq}
import org.mockito.Mockito.{never, verify, _}
import org.scalatest._
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Minutes, Span}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class LeonardoServiceSpec extends TestKit(ActorSystem("leonardotest")) with FlatSpecLike with Matchers
  with BeforeAndAfter with BeforeAndAfterAll with TestComponent with ScalaFutures
  with OptionValues with CommonTestData with LeoComponent with Retry with LazyLogging {

  private var gdDAO: MockGoogleDataprocDAO = _
  private var computeDAO: MockGoogleComputeDAO = _
  private var iamDAO: MockGoogleIamDAO = _
  private var projectDAO: MockGoogleProjectDAO = _
  private var storageDAO: MockGoogleStorageDAO = _
  private var samClient: MockSwaggerSamClient = _
  private var bucketHelper: BucketHelper = _
  private var leo: LeonardoService = _
  private var authProvider: LeoAuthProvider = _

  before {
    gdDAO = new MockGoogleDataprocDAO
    computeDAO = new MockGoogleComputeDAO
    iamDAO = new MockGoogleIamDAO
    projectDAO = new MockGoogleProjectDAO
    storageDAO = new MockGoogleStorageDAO
    // Pre-populate the juptyer extension bucket in the mock storage DAO, as it is passed in some requests
    storageDAO.buckets += jupyterExtensionUri.bucketName -> Set((jupyterExtensionUri.objectName, new ByteArrayInputStream("foo".getBytes())))

    samClient = serviceAccountProvider.asInstanceOf[MockPetClusterServiceAccountProvider].mockSwaggerSamClient
    authProvider = new WhitelistAuthProvider(whitelistAuthConfig, serviceAccountProvider)

    bucketHelper = new BucketHelper(dataprocConfig, gdDAO, computeDAO, storageDAO, serviceAccountProvider)
    val mockPetGoogleDAO: String => GoogleStorageDAO = _ => {
      new MockGoogleStorageDAO
    }
    leo = new LeonardoService(dataprocConfig, clusterFilesConfig, clusterResourcesConfig, clusterDefaultsConfig, proxyConfig, swaggerConfig, autoFreezeConfig, gdDAO, computeDAO, iamDAO, projectDAO, storageDAO, mockPetGoogleDAO, DbSingleton.ref, authProvider, serviceAccountProvider, bucketHelper, contentSecurityPolicy)
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  lazy val serviceAccountCredentialFile = notebookServiceAccount(project)
    .map(_ => List(ClusterInitValues.serviceAccountCredentialsFilename))
    .getOrElse(List.empty)

  lazy val configFiles = List(
    clusterResourcesConfig.jupyterDockerCompose.value,
    clusterResourcesConfig.rstudioDockerCompose.value,
    clusterResourcesConfig.proxyDockerCompose.value,
    clusterResourcesConfig.initActionsScript.value,
    clusterFilesConfig.jupyterServerCrt.getName,
    clusterFilesConfig.jupyterServerKey.getName,
    clusterFilesConfig.jupyterRootCaPem.getName,
    clusterResourcesConfig.proxySiteConf.value,
    clusterResourcesConfig.googleSignInJs.value,
    clusterResourcesConfig.jupyterGooglePlugin.value,
    clusterResourcesConfig.jupyterLabGooglePlugin.value,
    clusterResourcesConfig.jupyterSafeModePlugin.value,
    clusterResourcesConfig.jupyterNotebookConfigUri.value,
    clusterResourcesConfig.welderDockerCompose.value
  )

  lazy val initFiles = (configFiles ++ serviceAccountCredentialFile).map(GcsObjectName(_))

  "LeonardoService" should "create a single node cluster with default machine configs" in isolatedDbTest {
    forallClusterCreationMethods { (creationMethod, clusterName) =>
      // create the cluster
      val clusterCreateResponse = creationMethod(userInfo, project, clusterName, testClusterRequest).futureValue

      eventually {
        // check the create response has the correct info
        clusterCreateResponse.serviceAccountInfo.clusterServiceAccount shouldEqual clusterServiceAccount(project)
        clusterCreateResponse.serviceAccountInfo.notebookServiceAccount shouldEqual notebookServiceAccount(project)

        // check the cluster has the correct machine configs
        clusterCreateResponse.machineConfig shouldEqual singleNodeDefaultMachineConfig

        // check the firewall rule was created for the project
        computeDAO.firewallRules should contain key (project)
        computeDAO.firewallRules(project).name.value shouldBe dataprocConfig.firewallRuleName

        // should have created init and staging buckets
        val initBucketOpt = storageDAO.buckets.keys.find(_.value.startsWith("leoinit-" + clusterName.value))
        initBucketOpt shouldBe 'defined

        val stagingBucketOpt = storageDAO.buckets.keys.find(_.value.startsWith("leostaging-" + clusterName.value))
        stagingBucketOpt shouldBe 'defined

        // check the init files were added to the init bucket
        val initBucketObjects = storageDAO.buckets(initBucketOpt.get).map(_._1).map(objName => GcsPath(initBucketOpt.get, objName))
        initFiles.foreach(initFile => initBucketObjects should contain(GcsPath(initBucketOpt.get, initFile)))
        initBucketObjects should contain theSameElementsAs initFiles.map(GcsPath(initBucketOpt.get, _))

        // a service account key should only have been created if using a notebook service account
        if (notebookServiceAccount(project).isDefined) {
          iamDAO.serviceAccountKeys should contain key (samClient.serviceAccount)
        } else {
          iamDAO.serviceAccountKeys should not contain key(samClient.serviceAccount)
        }

        val dbInitBucketOpt = dbFutureValue { _.clusterQuery.getInitBucket(project, clusterName) }
        dbInitBucketOpt shouldBe 'defined
      }
    }
  }

  it should "create and get a cluster" in isolatedDbTest {
    import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.clusterEq

    // create a cluster
    val clusterCreateResponse = leo.createCluster(userInfo, project, name1, testClusterRequest).futureValue

    // get the cluster detail
    val clusterGetResponse = leo.getActiveClusterDetails(userInfo, project, name1).futureValue

    // check the create response and get response are the same
    clusterCreateResponse shouldEqual clusterGetResponse
  }

  it should "create and get a cluster via v2 API" in isolatedDbTest {
    implicit val patienceConfig = PatienceConfig(timeout = 1.second)

    // create the cluster
    val clusterRequest = testClusterRequest.copy(
      machineConfig = Some(singleNodeDefaultMachineConfig),
      stopAfterCreation = Some(true))

    leo.processClusterCreationRequest(userInfo, project, name1, clusterRequest).futureValue

    eventually {
      val createdCluster = leo.getActiveClusterDetails(userInfo, project, name1).futureValue

      createdCluster.id.toInt should be > 0
      createdCluster.clusterName shouldEqual name1
      createdCluster.dataprocInfo.googleId shouldBe defined
      createdCluster.googleProject shouldBe project
      createdCluster.serviceAccountInfo.clusterServiceAccount shouldBe clusterServiceAccount(project)
      createdCluster.serviceAccountInfo.notebookServiceAccount shouldBe notebookServiceAccount(project)
      createdCluster.machineConfig shouldBe singleNodeDefaultMachineConfig
      createdCluster.clusterUrl.toString shouldBe s"http://leonardo/${project.value}/${name1.value}"
      createdCluster.dataprocInfo.operationName shouldBe Some(OperationName("op-name"))
      createdCluster.status shouldBe ClusterStatus.Creating
      createdCluster.dataprocInfo.hostIp shouldBe None
      createdCluster.auditInfo.creator shouldBe userEmail
      createdCluster.auditInfo.createdDate should be < Instant.now
      createdCluster.auditInfo.destroyedDate shouldBe None
      createdCluster.jupyterExtensionUri shouldBe None
      createdCluster.jupyterUserScriptUri shouldBe None
      createdCluster.dataprocInfo.stagingBucket.get.value should startWith(s"leostaging-${name1.value}")
      createdCluster.errors shouldBe List()
      createdCluster.instances shouldBe Set()
      createdCluster.userJupyterExtensionConfig shouldBe defined
      createdCluster.auditInfo.dateAccessed should be >= createdCluster.auditInfo.createdDate
      createdCluster.auditInfo.dateAccessed should be < Instant.now
      createdCluster.autopauseThreshold shouldBe 30
    }
  }

  it should "create a single node cluster with an empty machine config" in isolatedDbTest {
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = Some(MachineConfig()))

    forallClusterCreationMethods { (creationMethod, clusterName) =>
      val clusterCreateResponse =
        creationMethod(userInfo, project, clusterName, clusterRequestWithMachineConfig).futureValue
      clusterCreateResponse.machineConfig shouldEqual singleNodeDefaultMachineConfig
    }
  }

  it should "create a single node cluster with zero workers explicitly defined in machine config" in isolatedDbTest {
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = Some(MachineConfig(Some(0))))

    forallClusterCreationMethods { (creationMethod, clusterName) =>
      val clusterCreateResponse =
        creationMethod(userInfo, project, clusterName, clusterRequestWithMachineConfig).futureValue
      clusterCreateResponse.machineConfig shouldEqual singleNodeDefaultMachineConfig
    }
  }

  it should "create a single node cluster with master configs defined" in isolatedDbTest {
    val singleNodeDefinedMachineConfig = MachineConfig(Some(0), Some("test-master-machine-type2"), Some(200))
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = Some(singleNodeDefinedMachineConfig))

    forallClusterCreationMethods { (creationMethod, clusterName) =>
      val clusterCreateResponse =
        creationMethod(userInfo, project, clusterName, clusterRequestWithMachineConfig).futureValue
      clusterCreateResponse.machineConfig shouldEqual singleNodeDefinedMachineConfig
    }
  }

  it should "create a single node cluster and override worker configs" in isolatedDbTest {
    // machine config is creating a single node cluster, but has worker configs defined
    val machineConfig = Some(MachineConfig(Some(0), Some("test-master-machine-type3"), Some(200), Some("test-worker-machine-type"), Some(10), Some(3), Some(4)))
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = machineConfig)

    forallClusterCreationMethods { (creationMethod, clusterName) =>
      val clusterCreateResponse =
        creationMethod(userInfo, project, clusterName, clusterRequestWithMachineConfig).futureValue
      clusterCreateResponse.machineConfig shouldEqual MachineConfig(Some(0), Some("test-master-machine-type3"), Some(200))
    }
  }

  it should "create a standard cluster with 2 workers with default worker configs" in isolatedDbTest {
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = Some(MachineConfig(Some(2))))
    val machineConfigResponse = MachineConfig(Some(2),
      Some(clusterDefaultsConfig.masterMachineType),
      Some(clusterDefaultsConfig.masterDiskSize),
      Some(clusterDefaultsConfig.workerMachineType),
      Some(clusterDefaultsConfig.workerDiskSize),
      Some(clusterDefaultsConfig.numberOfWorkerLocalSSDs),
      Some(clusterDefaultsConfig.numberOfPreemptibleWorkers))

    forallClusterCreationMethods { (creationMethod, clusterName) =>
      val clusterCreateResponse =
        creationMethod(userInfo, project, clusterName, clusterRequestWithMachineConfig).futureValue
      clusterCreateResponse.machineConfig shouldEqual machineConfigResponse
    }
  }

  it should "create a standard cluster with 10 workers with defined config" in isolatedDbTest {
    val machineConfig = MachineConfig(Some(10), Some("test-master-machine-type"), Some(200), Some("test-worker-machine-type"), Some(300), Some(3), Some(4))
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = Some(machineConfig))

    forallClusterCreationMethods { (creationMethod, clusterName) =>
      val clusterCreateResponse =
        creationMethod(userInfo, project, clusterName, clusterRequestWithMachineConfig).futureValue
      clusterCreateResponse.machineConfig shouldEqual machineConfig
    }
  }

  it should "create a standard cluster with 2 workers and override too-small disk sizes with minimum disk size" in isolatedDbTest {
    val machineConfig = MachineConfig(Some(2), Some("test-master-machine-type"), Some(5), Some("test-worker-machine-type"), Some(5), Some(3), Some(4))
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = Some(machineConfig))
    val expectedMachineConfig = MachineConfig(Some(2), Some("test-master-machine-type"), Some(10), Some("test-worker-machine-type"), Some(10), Some(3), Some(4))

    forallClusterCreationMethods { (creationMethod, clusterName) =>
      val clusterCreateResponse = creationMethod(userInfo, project, clusterName, clusterRequestWithMachineConfig).futureValue
      clusterCreateResponse.machineConfig shouldEqual expectedMachineConfig
    }
  }

  it should "throw OneWorkerSpecifiedInClusterRequestException when create a 1 worker cluster" in isolatedDbTest {
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = Some(MachineConfig(Some(1))))

    forallClusterCreationMethods { (creationMethod, clusterName) =>
      whenReady(creationMethod(userInfo, project, clusterName, clusterRequestWithMachineConfig).failed) { exc =>
        exc shouldBe a[OneWorkerSpecifiedInClusterRequestException]
      }
    }
  }

  it should "throw NegativeIntegerArgumentInClusterRequestException when master disk size in single node cluster request is a negative integer" in isolatedDbTest {
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = Some(MachineConfig(Some(0), Some("test-worker-machine-type"), Some(-30))))

    forallClusterCreationMethods { (creationMethod, clusterName) =>
      whenReady(creationMethod(userInfo, project, clusterName, clusterRequestWithMachineConfig).failed) { exc =>
        exc shouldBe a[NegativeIntegerArgumentInClusterRequestException]
      }
    }
  }

  it should "throw NegativeIntegerArgumentInClusterRequestException when number of preemptible workers in a 2 worker cluster request is a negative integer" in isolatedDbTest {
    val machineConfig = MachineConfig(Some(10), Some("test-master-machine-type"), Some(200), Some("test-worker-machine-type"), Some(300), Some(3), Some(-1))
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = Option(machineConfig))

    forallClusterCreationMethods { (creationMethod, clusterName) =>
      whenReady(creationMethod(userInfo, project, clusterName, clusterRequestWithMachineConfig).failed) { exc =>
        exc shouldBe a[NegativeIntegerArgumentInClusterRequestException]
      }
    }
  }

  it should "throw ClusterNotFoundException for nonexistent clusters" in isolatedDbTest {
    whenReady(leo.getActiveClusterDetails(userInfo, GoogleProject("nonexistent"), ClusterName("cluster")).failed) { exc =>
      exc shouldBe a[ClusterNotFoundException]
    }
  }

  it should "throw ClusterAlreadyExistsException when creating a cluster with same name and project as an existing cluster" in isolatedDbTest {
    forallClusterCreationMethods { (creationMethod, clusterName) =>
      // create the first cluster
      creationMethod(userInfo, project, clusterName, testClusterRequest).futureValue

      // creating the same cluster again should throw a ClusterAlreadyExistsException
      whenReady(creationMethod(userInfo, project, clusterName, testClusterRequest).failed) { exc =>
        exc shouldBe a[ClusterAlreadyExistsException]
      }
    }
  }

  it should "create two clusters with same name with only one active" in isolatedDbTest {
    forallClusterCreationMethods { (creationMethod, clusterName) =>
      // create first cluster
      val cluster = creationMethod(userInfo, project, clusterName, testClusterRequest).futureValue

      eventually {
        // check that the cluster was created
        gdDAO.clusters should contain key (clusterName)
      }

      // change cluster status to Running so that it can be deleted
      dbFutureValue { _.clusterQuery.setToRunning(cluster.id, IP("numbers.and.dots")) }

      // delete the cluster
      leo.deleteCluster(userInfo, project, clusterName).futureValue

      // recreate cluster with same project and cluster name
      // should throw ClusterAlreadyExistsException since the cluster is still Deleting
      creationMethod(userInfo, project, clusterName, testClusterRequest).failed.futureValue shouldBe a[ClusterAlreadyExistsException]

      // flip the cluster to Deleted in the database
      dbFutureValue { _.clusterQuery.completeDeletion(cluster.id) }

      // recreate cluster with same project and cluster name
      creationMethod(userInfo, project, clusterName, testClusterRequest).futureValue

      // confirm cluster was created
      eventually {
        gdDAO.clusters should contain key (clusterName)
      }
    }
  }

  it should "delete a cluster" in isolatedDbTest {
    // need a specialized LeonardoService for this test, so we can spy on its authProvider
    val spyProvider: LeoAuthProvider = spy(authProvider)
    val mockPetGoogleStorageDAO: String => GoogleStorageDAO = _ => {
      new MockGoogleStorageDAO
    }
    val leoForTest = new LeonardoService(dataprocConfig, clusterFilesConfig, clusterResourcesConfig, clusterDefaultsConfig, proxyConfig, swaggerConfig, autoFreezeConfig, gdDAO, computeDAO, iamDAO, projectDAO, storageDAO, mockPetGoogleStorageDAO, DbSingleton.ref, spyProvider, serviceAccountProvider, bucketHelper, contentSecurityPolicy)

    forallClusterCreationMethods(Seq((leoForTest.createCluster _).tupled, (leoForTest.processClusterCreationRequest _).tupled))(Seq(name1, name2)) {
      (creationMethod, clusterName) =>
        // check that the cluster does not exist
        gdDAO.clusters should not contain key(clusterName)

        // create the cluster
        val cluster = creationMethod(userInfo, project, clusterName, testClusterRequest).futureValue

        eventually {
          // check that the cluster was created
          gdDAO.clusters should contain key (clusterName)
          // a service account key should only have been created if using a notebook service account
          if (notebookServiceAccount(project).isDefined) {
            iamDAO.serviceAccountKeys should contain key (samClient.serviceAccount)
          } else {
            iamDAO.serviceAccountKeys should not contain key(samClient.serviceAccount)
          }
        }

        // change cluster status to Running so that it can be deleted
        dbFutureValue { _.clusterQuery.setToRunning(cluster.id, IP("numbers.and.dots")) }

        // delete the cluster
        leoForTest.deleteCluster(userInfo, project, clusterName).futureValue

        // check that the cluster no longer exists
        gdDAO.clusters should not contain key(clusterName)
        iamDAO.serviceAccountKeys should not contain key(samClient.serviceAccount)

        // the cluster has transitioned to the Deleting state (Cluster Monitor will later transition it to Deleted)

        dbFutureValue { _.clusterQuery.getActiveClusterByName(project, clusterName) }
          .map(_.status) shouldBe Some(ClusterStatus.Deleting)

        // the auth provider should have not yet been notified of deletion
        verify(spyProvider, never).notifyClusterDeleted(mockitoEq(userInfo.userEmail), mockitoEq(userInfo.userEmail), mockitoEq(project), mockitoEq(clusterName))(any[ExecutionContext])
    }
  }

  it should "delete a cluster that has status Error" in isolatedDbTest {
    forallClusterCreationMethods(Seq((leo.createCluster _).tupled, (leo.processClusterCreationRequest _).tupled))(gdDAO.errorClusters) {
      (creationMethod, clusterName) =>
        // check that the cluster does not exist
        gdDAO.clusters should not contain key(clusterName)

        // create the cluster
        val cluster = creationMethod(userInfo, project, clusterName, testClusterRequest).futureValue

        eventually {
          // check that the cluster was created
          gdDAO.clusters should contain key clusterName
        }

        // change cluster status to Running so that it can be deleted
        dbFutureValue { _.clusterQuery.setToRunning(cluster.id, IP("numbers.and.dots")) }

        // delete the cluster
        leo.deleteCluster(userInfo, project, clusterName).futureValue

        // check that the cluster no longer exists
        gdDAO.clusters should not contain key(clusterName)
    }
  }

  it should "delete a cluster's instances" in isolatedDbTest {
    // check that the cluster does not exist
    gdDAO.clusters should not contain key(name1)

    // create the cluster
    val clusterCreateResponse = leo.createCluster(userInfo, project, name1, testClusterRequest).futureValue

    // check that the cluster was created
    gdDAO.clusters should contain key name1

    // populate some instances for the cluster
    dbFutureValue { _.instanceQuery.saveAllForCluster(
        getClusterId(clusterCreateResponse), Seq(masterInstance, workerInstance1, workerInstance2)) }

    // change cluster status to Running so that it can be deleted
    dbFutureValue { _.clusterQuery.setToRunning(clusterCreateResponse.id, IP("numbers.and.dots")) }

    // delete the cluster
    leo.deleteCluster(userInfo, project, name1).futureValue

    // check that the cluster no longer exists
    gdDAO.clusters should not contain key(name1)

    // check that the instances are still in the DB (they get removed by the ClusterMonitorActor)
    val instances = dbFutureValue { _.instanceQuery.getAllForCluster(getClusterId(clusterCreateResponse)) }
    instances.toSet shouldBe Set(masterInstance, workerInstance1, workerInstance2)

    // ------------------ Grossly repeating above using v2 of the cluster creation API :( ------------------

    // Create new-ish instances to avoid collisions with the ones used above
    val masterInstanceV2 = masterInstance.copy(googleId = BigInt(67890))
    val workerInstance1V2 = workerInstance1.copy(googleId = BigInt(78901))
    val workerInstance2V2 = workerInstance2.copy(googleId = BigInt(89012))

    // check that the cluster does not exist
    gdDAO.clusters should not contain key(name2)

    // create the cluster
    val clusterCreateResponseV2 =
      leo.processClusterCreationRequest(userInfo, project, name2, testClusterRequest).futureValue

    eventually {
      // check that the cluster was created
      gdDAO.clusters should contain key name2
    }

    // populate some instances for the cluster
    dbFutureValue { _.instanceQuery.saveAllForCluster(
        getClusterId(clusterCreateResponseV2), Seq(masterInstanceV2, workerInstance1V2, workerInstance2V2)) }

    // change cluster status to Running so that it can be deleted
    dbFutureValue { _.clusterQuery.setToRunning(clusterCreateResponseV2.id, IP("numbers.and.dots")) }

    // delete the cluster
    leo.deleteCluster(userInfo, project, name2).futureValue

    // check that the cluster no longer exists
    gdDAO.clusters should not contain key(name2)

    // check that the instances are still in the DB (they get removed by the ClusterMonitorActor)
    val instancesV2 = dbFutureValue { _.instanceQuery.getAllForCluster(getClusterId(clusterCreateResponseV2)) }
    instancesV2.toSet shouldBe Set(masterInstanceV2, workerInstance1V2, workerInstance2V2)
  }

  it should "throw ClusterNotFoundException when deleting non existent clusters" in isolatedDbTest {
    whenReady(leo.deleteCluster(userInfo, GoogleProject("nonexistent"), ClusterName("cluster")).failed) { exc =>
      exc shouldBe a[ClusterNotFoundException]
    }
  }

  it should "initialize bucket with correct files" in isolatedDbTest {
    // create the bucket and add files
    val bucketName = generateUniqueBucketName(name1.value + "-init")
    val bucket = bucketHelper.createInitBucket(project, bucketName, ServiceAccountInfo(None, Some(serviceAccountEmail))).futureValue
    leo.initializeBucketObjects(userInfo.userEmail, project, name1, bucket, testClusterRequest, Some(serviceAccountKey), contentSecurityPolicy, Set(jupyterImage), stagingBucketName).futureValue

    // our bucket should now exist
    storageDAO.buckets should contain key (bucket)

    val bucketObjects = storageDAO.buckets(bucket).map(_._1).map(objName => GcsPath(bucket, objName))

    // check the init files were added to the bucket
    initFiles.map(initFile => bucketObjects should contain(GcsPath(bucket, initFile)))

    // check that the service account key was added to the bucket
    bucketObjects should contain(GcsPath(bucket, GcsObjectName(ClusterInitValues.serviceAccountCredentialsFilename)))
  }

  it should "create a firewall rule in a project only once when the first cluster is added" in isolatedDbTest {
    val clusterName2 = ClusterName("test-cluster-2")

    // Our google project should have no firewall rules
    computeDAO.firewallRules.toList should not contain(project, dataprocConfig.firewallRuleName)

    // create the first cluster, this should create a firewall rule in our project
    leo.createCluster(userInfo, project, name1, testClusterRequest).futureValue

    // check that there is exactly 1 firewall rule for our project
    computeDAO.firewallRules.filterKeys(_ == project) should have size 1

    // create the second cluster. This should check that our project has a firewall rule and not try to add it again
    leo.createCluster(userInfo, project, clusterName2, testClusterRequest).futureValue

    // check that there is still exactly 1 firewall rule in our project
    computeDAO.firewallRules.filterKeys(_ == project) should have size 1
  }

  it should "create a firewall rule in a project only once when the first cluster is added using v2 API" in isolatedDbTest {
    val clusterName2 = ClusterName("test-cluster-2")

    // Our google project should have no firewall rules
    computeDAO.firewallRules.toList should not contain(project, dataprocConfig.firewallRuleName)

    // create the first cluster, this should create a firewall rule in our project
    leo.processClusterCreationRequest(userInfo, project, name1, testClusterRequest).futureValue

    eventually {
      // check that there is exactly 1 firewall rule for our project
      computeDAO.firewallRules.filterKeys(_ == project) should have size 1
    }

    // create the second cluster. This should check that our project has a firewall rule and not try to add it again
    leo.processClusterCreationRequest(userInfo, project, clusterName2, testClusterRequest).futureValue

    eventually {
      // check that there is still exactly 1 firewall rule in our project
      computeDAO.firewallRules.filterKeys(_ == project) should have size 1
    }
  }

  it should "template a script using config values" in isolatedDbTest {
    // Create replacements map
    val clusterInit = ClusterInitValues(project, name1, initBucketPath, testClusterRequestWithExtensionAndScript, dataprocConfig, clusterFilesConfig, clusterResourcesConfig, proxyConfig, Some(serviceAccountKey), userInfo.userEmail, contentSecurityPolicy, Set(jupyterImage, rstudioImage), stagingBucketName)
    val replacements: Map[String, String] = clusterInit.toMap

    // Each value in the replacement map will replace its key in the file being processed
    val result = leo.templateResource(clusterResourcesConfig.initActionsScript, replacements)

    // Check that the values in the bash script file were correctly replaced
    val expected =
      s"""|#!/usr/bin/env bash
          |
          |"${name1.value}"
          |"${project.value}"
          |"${jupyterImage.dockerImage}"
          |"${rstudioImage.dockerImage}"
          |"${proxyConfig.jupyterProxyDockerImage}"
          |"${jupyterUserScriptUri.toUri}"
          |"${GcsPath(initBucketPath, GcsObjectName(ClusterInitValues.serviceAccountCredentialsFilename)).toUri}"
          |"${testClusterRequestWithExtensionAndScript.userJupyterExtensionConfig.get.serverExtensions.values.mkString(" ")}"
          |"${testClusterRequestWithExtensionAndScript.userJupyterExtensionConfig.get.nbExtensions.values.mkString(" ")}"
          |"${testClusterRequestWithExtensionAndScript.userJupyterExtensionConfig.get.combinedExtensions.values.mkString(" ")}"
          |"${GcsPath(stagingBucketName, GcsObjectName("userscript_output.txt")).toUri}"
          |""".stripMargin

    result shouldEqual expected
  }

  it should s"template google_sign_in.js with config values" in isolatedDbTest {

    // Create replacements map
    val clusterInit = ClusterInitValues(project, name1, initBucketPath, testClusterRequest, dataprocConfig, clusterFilesConfig, clusterResourcesConfig, proxyConfig, Some(serviceAccountKey), userInfo.userEmail, contentSecurityPolicy, Set(jupyterImage), stagingBucketName)
    val replacements: Map[String, String] = clusterInit.toMap

    // Each value in the replacement map will replace it's key in the file being processed
    val result = leo.templateResource(clusterResourcesConfig.googleSignInJs, replacements)

    // Check that the values in the bash script file were correctly replaced
    val expected =
      s"""|"${userInfo.userEmail.value}"
          |"${testClusterRequest.defaultClientId.value}"
          |""".stripMargin

    result shouldEqual expected
  }

  it should "throw a JupyterExtensionException when the extensionUri is too long" in isolatedDbTest {
    forallClusterCreationMethods { (creationMethod, clusterName) =>
      val jupyterExtensionUri = GcsPath(GcsBucketName("bucket"), GcsObjectName(Stream.continually('a').take(1025).mkString))

      // create the cluster
      val clusterRequest = testClusterRequest.copy(jupyterExtensionUri = Some(jupyterExtensionUri))
      val response = creationMethod(userInfo, project, clusterName, clusterRequest).failed.futureValue

      response shouldBe a[BucketObjectException]
    }
  }

  it should "throw a JupyterExtensionException when the jupyterExtensionUri does not point to a GCS object" in isolatedDbTest {
    forallClusterCreationMethods { (creationMethod, clusterName) =>
      val jupyterExtensionUri = parseGcsPath("gs://bogus/object.tar.gz").right.get

      // create the cluster
      val response = creationMethod(userInfo, project, clusterName, testClusterRequest.copy(jupyterExtensionUri = Some(jupyterExtensionUri))).failed.futureValue

      response shouldBe a[BucketObjectException]
    }
  }

  it should "list no clusters" in isolatedDbTest {
    leo.listClusters(userInfo, Map.empty).futureValue shouldBe 'empty
    leo.listClusters(userInfo, Map("foo" -> "bar", "baz" -> "biz")).futureValue shouldBe 'empty
  }

  it should "list all clusters" in isolatedDbTest {
    // create a couple of clusters
    val clusterName1 = ClusterName(s"cluster-${UUID.randomUUID.toString}")
    val cluster1 = leo.createCluster(userInfo, project, clusterName1, testClusterRequest).futureValue

    val clusterName2 = ClusterName(s"cluster-${UUID.randomUUID.toString}")
    val cluster2 = leo.createCluster(userInfo, project, clusterName2, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar"))).futureValue

    leo.listClusters(userInfo, Map.empty).futureValue.toSet shouldBe Set(cluster1, cluster2).map(stripFieldsForListCluster)
  }

  it should "error when trying to delete a creating cluster" in isolatedDbTest {
    forallClusterCreationMethods { (creationMethod, clusterName) =>
      // create cluster
      creationMethod(userInfo, project, clusterName, testClusterRequest).futureValue
      eventually {
        // check that the cluster was created
        gdDAO.clusters should contain key (clusterName)
      }
      // should fail to delete because cluster is in Creating status
      leo.deleteCluster(userInfo, project, clusterName).failed.futureValue shouldBe a[ClusterCannotBeDeletedException]
    }
  }

  it should "list all clusters created via v2 API" in isolatedDbTest {
    // create a couple of clusters
    val clusterName1 = ClusterName(s"cluster-${UUID.randomUUID.toString}")
    leo.processClusterCreationRequest(userInfo, project, clusterName1, testClusterRequest).futureValue

    val clusterName2 = ClusterName(s"cluster-${UUID.randomUUID.toString}")
    val testClusterRequest2 = testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar"))
    leo.processClusterCreationRequest(userInfo, project, clusterName2, testClusterRequest2).futureValue

    // We can't just use the responses to processClusterCreationRequest() since it won't contain the fields
    // that are asynchronously filled in after the call is returned so we're waiting for the asynchronous
    // operation to finish and then fetching the records with getActiveClusterDetails()
    eventually {
      val cluster1 = leo.getActiveClusterDetails(userInfo, project, clusterName1).futureValue
      val cluster2 = leo.getActiveClusterDetails(userInfo, project, clusterName2).futureValue

      leo.listClusters(userInfo, Map.empty).futureValue.toSet shouldBe Set(cluster1, cluster2).map(stripFieldsForListCluster)
    }
  }

  it should "list all active clusters" in isolatedDbTest {
    // create a couple of clusters
    val cluster1 = leo.createCluster(userInfo, project, name1, testClusterRequest).futureValue

    val clusterName2 = ClusterName("test-cluster-2")
    val cluster2 = leo.createCluster(userInfo, project, clusterName2, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar"))).futureValue

    leo.listClusters(userInfo, Map("includeDeleted" -> "false")).futureValue.toSet shouldBe Set(cluster1, cluster2).map(stripFieldsForListCluster)
    leo.listClusters(userInfo, Map.empty).futureValue.toSet shouldBe Set(cluster1, cluster2).map(stripFieldsForListCluster)
    leo.listClusters(userInfo, Map.empty).futureValue.toSet shouldBe Set(cluster1, cluster2).map(stripFieldsForListCluster)

    val clusterName3 = ClusterName("test-cluster-3")
    val cluster3 = leo.createCluster(userInfo, project, clusterName3, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar"))).futureValue

    dbFutureValue { _.clusterQuery.completeDeletion(cluster3.id) }

    leo.listClusters(userInfo, Map.empty).futureValue.toSet shouldBe Set(cluster1, cluster2).map(stripFieldsForListCluster)
    leo.listClusters(userInfo, Map("includeDeleted" -> "false")).futureValue.toSet shouldBe Set(cluster1, cluster2).map(stripFieldsForListCluster)
    leo.listClusters(userInfo, Map("includeDeleted" -> "true")).futureValue.toSet.size shouldBe 3
  }

  it should "list all active clusters created via v2 API" in isolatedDbTest {
    var cluster1, cluster2, cluster3: Cluster = null.asInstanceOf[Cluster]

    // create a couple of clusters
    leo.processClusterCreationRequest(userInfo, project, name1, testClusterRequest).futureValue

    val clusterName2 = ClusterName("test-cluster-2")
    val testClusterRequest2 = testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar"))
    leo.processClusterCreationRequest(userInfo, project, clusterName2, testClusterRequest2).futureValue

    eventually {
      cluster1 = leo.getActiveClusterDetails(userInfo, project, name1).futureValue
      cluster2 = leo.getActiveClusterDetails(userInfo, project, clusterName2).futureValue

      leo.listClusters(userInfo, Map("includeDeleted" -> "false")).futureValue.toSet shouldBe Set(cluster1, cluster2).map(stripFieldsForListCluster)
      leo.listClusters(userInfo, Map.empty).futureValue.toSet shouldBe Set(cluster1, cluster2).map(stripFieldsForListCluster)
      leo.listClusters(userInfo, Map.empty).futureValue.toSet shouldBe Set(cluster1, cluster2).map(stripFieldsForListCluster)
    }

    val clusterName3 = ClusterName("test-cluster-3")
    val testClusterRequest3 = testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar"))
    cluster3 = leo.processClusterCreationRequest(userInfo, project, clusterName3, testClusterRequest3).futureValue

    eventually {
      dbFutureValue { _.clusterQuery.completeDeletion(cluster3.id) }

      leo.listClusters(userInfo, Map.empty).futureValue.toSet shouldBe Set(cluster1, cluster2).map(stripFieldsForListCluster)
      leo.listClusters(userInfo, Map("includeDeleted" -> "false")).futureValue.toSet shouldBe Set(cluster1, cluster2).map(stripFieldsForListCluster)
      leo.listClusters(userInfo, Map("includeDeleted" -> "true")).futureValue.toSet.size shouldBe 3
    }
  }

  it should "list clusters with labels" in isolatedDbTest {
    // create a couple of clusters
    val clusterName1 = ClusterName(s"cluster-${UUID.randomUUID.toString}")
    val cluster1 = leo.createCluster(userInfo, project, clusterName1, testClusterRequest).futureValue

    val clusterName2 = ClusterName(s"test-cluster-2")
    val cluster2 = leo.createCluster(userInfo, project, clusterName2, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar"))).futureValue

    leo.listClusters(userInfo, Map("foo" -> "bar")).futureValue.toSet shouldBe Set(cluster1, cluster2).map(stripFieldsForListCluster)
    leo.listClusters(userInfo, Map("foo" -> "bar", "bam" -> "yes")).futureValue.toSet shouldBe Set(cluster1).map(stripFieldsForListCluster)
    leo.listClusters(userInfo, Map("foo" -> "bar", "bam" -> "yes", "vcf" -> "no")).futureValue.toSet shouldBe Set(cluster1).map(stripFieldsForListCluster)
    leo.listClusters(userInfo, Map("a" -> "b")).futureValue.toSet shouldBe Set(cluster2).map(stripFieldsForListCluster)
    leo.listClusters(userInfo, Map("foo" -> "bar", "baz" -> "biz")).futureValue.toSet shouldBe Set.empty
    leo.listClusters(userInfo, Map("A" -> "B")).futureValue.toSet shouldBe Set(cluster2).map(stripFieldsForListCluster) // labels are not case sensitive because MySQL
    //Assert that extensions were added as labels as well
    leo.listClusters(userInfo, Map("abc" -> "def", "pqr" -> "pqr", "xyz" -> "xyz")).futureValue.toSet shouldBe Set(cluster1, cluster2).map(stripFieldsForListCluster)
  }

  it should "list clusters with labels created via v2 API" in isolatedDbTest {
    // create a couple of clusters
    val clusterName1 = ClusterName(s"cluster-${UUID.randomUUID.toString}")
    leo.processClusterCreationRequest(userInfo, project, clusterName1, testClusterRequest).futureValue

    val clusterName2 = ClusterName(s"test-cluster-2")
    leo.processClusterCreationRequest(userInfo, project, clusterName2, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar"))).futureValue

    eventually {
      val cluster1 = leo.getActiveClusterDetails(userInfo, project, clusterName1).futureValue
      val cluster2 = leo.getActiveClusterDetails(userInfo, project, clusterName2).futureValue

      leo.listClusters(userInfo, Map("foo" -> "bar")).futureValue.toSet shouldBe Set(cluster1, cluster2).map(stripFieldsForListCluster)
      leo.listClusters(userInfo, Map("foo" -> "bar", "bam" -> "yes")).futureValue.toSet shouldBe Set(cluster1).map(stripFieldsForListCluster)
      leo.listClusters(userInfo, Map("foo" -> "bar", "bam" -> "yes", "vcf" -> "no")).futureValue.toSet shouldBe Set(cluster1).map(stripFieldsForListCluster)
      leo.listClusters(userInfo, Map("a" -> "b")).futureValue.toSet shouldBe Set(cluster2).map(stripFieldsForListCluster)
      leo.listClusters(userInfo, Map("foo" -> "bar", "baz" -> "biz")).futureValue.toSet shouldBe Set.empty
      leo.listClusters(userInfo, Map("A" -> "B")).futureValue.toSet shouldBe Set(cluster2).map(stripFieldsForListCluster) // labels are not case sensitive because MySQL

      //Assert that extensions were added as labels as well
      leo.listClusters(userInfo, Map("abc" -> "def", "pqr" -> "pqr", "xyz" -> "xyz")).futureValue.toSet shouldBe Set(cluster1, cluster2).map(stripFieldsForListCluster)
    }
  }

  it should "throw IllegalLabelKeyException when using a forbidden label" in isolatedDbTest {
    forallClusterCreationMethods { (creationMethod, clusterName) =>
      // cluster should not be allowed to have a label with key of "includeDeleted"
      val modifiedTestClusterRequest = testClusterRequest.copy(labels = Map("includeDeleted" -> "val"))
      val includeDeletedResponse = creationMethod(userInfo, project, clusterName, modifiedTestClusterRequest).failed.futureValue

      eventually {
        includeDeletedResponse shouldBe a[IllegalLabelKeyException]
      }
    }
  }

  it should "list clusters with swagger-style labels" in isolatedDbTest {
    // create a couple of clusters
    val clusterName1 = ClusterName(s"cluster-${UUID.randomUUID.toString}")
    val cluster1 = leo.createCluster(userInfo, project, clusterName1, testClusterRequest).futureValue

    val clusterName2 = ClusterName(s"cluster-${UUID.randomUUID.toString}")
    val cluster2 = leo.createCluster(userInfo, project, clusterName2, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar"))).futureValue

    leo.listClusters(userInfo, Map("_labels" -> "foo=bar")).futureValue.toSet shouldBe Set(cluster1, cluster2).map(stripFieldsForListCluster)
    leo.listClusters(userInfo, Map("_labels" -> "foo=bar,bam=yes")).futureValue.toSet shouldBe Set(cluster1).map(stripFieldsForListCluster)
    leo.listClusters(userInfo, Map("_labels" -> "foo=bar,bam=yes,vcf=no")).futureValue.toSet shouldBe Set(cluster1).map(stripFieldsForListCluster)
    leo.listClusters(userInfo, Map("_labels" -> "a=b")).futureValue.toSet shouldBe Set(cluster2).map(stripFieldsForListCluster)
    leo.listClusters(userInfo, Map("_labels" -> "baz=biz")).futureValue.toSet shouldBe Set.empty
    leo.listClusters(userInfo, Map("_labels" -> "A=B")).futureValue.toSet shouldBe Set(cluster2).map(stripFieldsForListCluster) // labels are not case sensitive because MySQL
    leo.listClusters(userInfo, Map("_labels" -> "foo%3Dbar")).failed.futureValue shouldBe a[ParseLabelsException]
    leo.listClusters(userInfo, Map("_labels" -> "foo=bar;bam=yes")).failed.futureValue shouldBe a[ParseLabelsException]
    leo.listClusters(userInfo, Map("_labels" -> "foo=bar,bam")).failed.futureValue shouldBe a[ParseLabelsException]
    leo.listClusters(userInfo, Map("_labels" -> "bogus")).failed.futureValue shouldBe a[ParseLabelsException]
    leo.listClusters(userInfo, Map("_labels" -> "a,b")).failed.futureValue shouldBe a[ParseLabelsException]
  }

  it should "list clusters with swagger-style labels, created using v2 API" in isolatedDbTest {
    // create a couple of clusters
    val clusterName1 = ClusterName(s"cluster-${UUID.randomUUID.toString}")
    leo.processClusterCreationRequest(userInfo, project, clusterName1, testClusterRequest).futureValue

    val clusterName2 = ClusterName(s"cluster-${UUID.randomUUID.toString}")
    val testClusterRequest2 = testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar"))
    leo.processClusterCreationRequest(userInfo, project, clusterName2, testClusterRequest2).futureValue

    eventually {
      val cluster1 = leo.getActiveClusterDetails(userInfo, project, clusterName1).futureValue
      val cluster2 = leo.getActiveClusterDetails(userInfo, project, clusterName2).futureValue

      leo.listClusters(userInfo, Map("_labels" -> "foo=bar")).futureValue.toSet shouldBe Set(cluster1, cluster2).map(stripFieldsForListCluster)
      leo.listClusters(userInfo, Map("_labels" -> "foo=bar,bam=yes")).futureValue.toSet shouldBe Set(cluster1).map(stripFieldsForListCluster)
      leo.listClusters(userInfo, Map("_labels" -> "foo=bar,bam=yes,vcf=no")).futureValue.toSet shouldBe Set(cluster1).map(stripFieldsForListCluster)
      leo.listClusters(userInfo, Map("_labels" -> "a=b")).futureValue.toSet shouldBe Set(cluster2).map(stripFieldsForListCluster)
      leo.listClusters(userInfo, Map("_labels" -> "baz=biz")).futureValue.toSet shouldBe Set.empty
      leo.listClusters(userInfo, Map("_labels" -> "A=B")).futureValue.toSet shouldBe Set(cluster2).map(stripFieldsForListCluster) // labels are not case sensitive because MySQL
      leo.listClusters(userInfo, Map("_labels" -> "foo%3Dbar")).failed.futureValue shouldBe a[ParseLabelsException]
      leo.listClusters(userInfo, Map("_labels" -> "foo=bar;bam=yes")).failed.futureValue shouldBe a[ParseLabelsException]
      leo.listClusters(userInfo, Map("_labels" -> "foo=bar,bam")).failed.futureValue shouldBe a[ParseLabelsException]
      leo.listClusters(userInfo, Map("_labels" -> "bogus")).failed.futureValue shouldBe a[ParseLabelsException]
      leo.listClusters(userInfo, Map("_labels" -> "a,b")).failed.futureValue shouldBe a[ParseLabelsException]
    }
  }

  it should "list clusters belonging to a project" in isolatedDbTest {
    // create a couple of clusters
    val cluster1 = leo.createCluster(userInfo, project, name1, testClusterRequest).futureValue
    val cluster2 = leo.createCluster(userInfo, project2, name2, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar"))).futureValue

    leo.listClusters(userInfo, Map.empty, Some(project)).futureValue.toSet shouldBe Set(cluster1).map(stripFieldsForListCluster)
    leo.listClusters(userInfo, Map.empty, Some(project2)).futureValue.toSet shouldBe Set(cluster2).map(stripFieldsForListCluster)
    leo.listClusters(userInfo, Map("foo" -> "bar"), Some(project)).futureValue.toSet shouldBe Set(cluster1).map(stripFieldsForListCluster)
    leo.listClusters(userInfo, Map("foo" -> "bar"), Some(project2)).futureValue.toSet shouldBe Set(cluster2).map(stripFieldsForListCluster)
    leo.listClusters(userInfo, Map("k" -> "v"), Some(project)).futureValue.toSet shouldBe Set.empty
    leo.listClusters(userInfo, Map("k" -> "v"), Some(project2)).futureValue.toSet shouldBe Set.empty
    leo.listClusters(userInfo, Map("foo" -> "bar"), Some(GoogleProject("non-existing-project"))).futureValue.toSet shouldBe Set.empty
  }

  it should "delete the init bucket if cluster creation fails" in isolatedDbTest {
    // create the cluster
    val clusterCreateResponse = leo.createCluster(userInfo, project, gdDAO.badClusterName, testClusterRequest).failed.futureValue

    clusterCreateResponse shouldBe a[Exception] // thrown by MockGoogleDataprocDAO

    // check the firewall rule was created for the project
    computeDAO.firewallRules should contain key (project)
    computeDAO.firewallRules(project).name.value shouldBe dataprocConfig.firewallRuleName

    //staging bucket lives on!
    storageDAO.buckets.keys.find(bucket => bucket.value.contains("leoinit-")).size shouldBe 0
    storageDAO.buckets.keys.find(bucket => bucket.value.contains("leostaging-")).size shouldBe 1
  }

  it should "delete the init bucket if cluster creation, via v2 API, fails" in isolatedDbTest {
    // create the cluster
    val cluster = leo.processClusterCreationRequest(userInfo, project, gdDAO.badClusterName, testClusterRequest).futureValue

    eventually {
      // check the firewall rule was created for the project
      computeDAO.firewallRules should contain key (project)
      computeDAO.firewallRules(project).name.value shouldBe dataprocConfig.firewallRuleName

      //staging bucket lives on!
      storageDAO.buckets.keys.find(bucket => bucket.value.contains("leoinit-")).size shouldBe 0
      storageDAO.buckets.keys.find(bucket => bucket.value.contains("leostaging-")).size shouldBe 1

      // verify that a corresponding error record was created in DB
      val errorRecordOpt = dbFutureValue { _.clusterErrorQuery.get(cluster.id) }

      errorRecordOpt should have size 1

      val errorRecord = errorRecordOpt.head

      val expectedErrorMessage = s"Asynchronous creation of cluster '${cluster.clusterName}' on Google project " +
        s"'${cluster.googleProject}' failed"

      errorRecord.errorMessage should startWith(expectedErrorMessage)
      errorRecord.errorCode shouldEqual -1
      errorRecord.timestamp should be < Instant.now.plusSeconds(1) // add a sec to prevent approximation errors

      // verify that the cluster status is Error
      val dbClusterOpt = dbFutureValue { _.clusterQuery.getClusterById(cluster.id) }
      dbClusterOpt.map(_.status) shouldBe Some(ClusterStatus.Error)
    }
  }

  it should "stop a cluster" in isolatedDbTest {
    // check that the cluster does not exist
    gdDAO.clusters should not contain key(name1)

    // create the cluster
    val clusterCreateResponse = leo.createCluster(userInfo, project, name1, testClusterRequest).futureValue

    // check that the cluster was created
    gdDAO.clusters should contain key name1

    // populate some instances for the cluster
    dbFutureValue { _.instanceQuery.saveAllForCluster(getClusterId(clusterCreateResponse), Seq(masterInstance, workerInstance1, workerInstance2)) }

    // set the cluster to Running
    dbFutureValue { _.clusterQuery.setToRunning(clusterCreateResponse.id, IP("1.2.3.4")) }

    // stop the cluster
    leo.stopCluster(userInfo, project, name1).futureValue

    // cluster should still exist in Google
    gdDAO.clusters should contain key (name1)

    // cluster status should be Stopping in the DB
    dbFutureValue { _.clusterQuery.getClusterByUniqueKey(clusterCreateResponse)}.get
      .status shouldBe ClusterStatus.Stopping

    // instance status should still be Running in the DB
    // the ClusterMonitorActor is what updates instance status
    val instances = dbFutureValue { _.instanceQuery.getAllForCluster(getClusterId(clusterCreateResponse)) }
    instances.size shouldBe 3
    instances.map(_.status).toSet shouldBe Set(InstanceStatus.Running)
  }

  it should "resize a cluster" in isolatedDbTest {
    // create the cluster
    val clusterCreateResponse =
      leo.processClusterCreationRequest(userInfo, project, name1, testClusterRequest).futureValue

    // set the cluster to Running
    dbFutureValue { _.clusterQuery.setToRunning(clusterCreateResponse.id, IP("1.2.3.4")) }

    leo.updateCluster(userInfo, project, name1, testClusterRequest.copy(machineConfig = Some(MachineConfig(numberOfWorkers = Some(2))))).futureValue

    //unfortunately we can't actually check that the new instances were added because the monitor
    //handles that but we will check as much as we can

    //check that status of cluster is Updating
    dbFutureValue { _.clusterQuery.getClusterStatus(clusterCreateResponse.id) } shouldBe Some(ClusterStatus.Updating)

    //check that the machine config has been updated
    dbFutureValue { _.clusterQuery.getClusterById(clusterCreateResponse.id) }.get.machineConfig.numberOfWorkers shouldBe Some(2)
  }

  it should "gracefully handle an invalid machine config being specific during cluster resize" in isolatedDbTest {
    val mockPetGoogleDAO: String => GoogleStorageDAO = _ => {
      new MockGoogleStorageDAO
    }

    //we meed to use a special version of the MockGoogleDataprocDAO to simulate an error during the call to resizeCluster
    leo = new LeonardoService(dataprocConfig, clusterFilesConfig, clusterResourcesConfig, clusterDefaultsConfig, proxyConfig, swaggerConfig, autoFreezeConfig, new ErroredMockGoogleDataprocDAO, computeDAO, iamDAO, projectDAO, storageDAO, mockPetGoogleDAO, DbSingleton.ref, authProvider, serviceAccountProvider, bucketHelper, contentSecurityPolicy)

    // create the cluster
    val clusterCreateResponse =
      leo.processClusterCreationRequest(userInfo, project, name1, testClusterRequest).futureValue

    // set the cluster to Running
    dbFutureValue { _.clusterQuery.setToRunning(clusterCreateResponse.id, IP("1.2.3.4")) }

    intercept[InvalidDataprocMachineConfigException] {
      Await.result(leo.updateCluster(userInfo, project, name1, testClusterRequest.copy(machineConfig = Some(MachineConfig(numberOfWorkers = Some(2))))), Duration.Inf)
    }

    //check that status of cluster is still Running
    dbFutureValue { _.clusterQuery.getClusterStatus(clusterCreateResponse.id) } shouldBe Some(ClusterStatus.Running)

    //check that the machine config was not updated
    dbFutureValue { _.clusterQuery.getClusterById(clusterCreateResponse.id) }.get.machineConfig.numberOfWorkers shouldBe Some(0)
  }

  it should "cluster creation should end in Error state if adding dataproc worker role fails" in isolatedDbTest {
    val mockPetGoogleDAO: String => GoogleStorageDAO = _ => {
      new MockGoogleStorageDAO
    }

    //we meed to use a special version of the MockGoogleIamDAO to simulate an error when adding IAM roles
    val iamDAO = new ErroredMockGoogleIamDAO
    leo = new LeonardoService(dataprocConfig, clusterFilesConfig, clusterResourcesConfig, clusterDefaultsConfig, proxyConfig, swaggerConfig, autoFreezeConfig, mockGoogleDataprocDAO, computeDAO, iamDAO, projectDAO, storageDAO, mockPetGoogleDAO, DbSingleton.ref, authProvider, serviceAccountProvider, bucketHelper, contentSecurityPolicy)

    // create the cluster
    val clusterCreateResponse =
      leo.processClusterCreationRequest(userInfo, project, name1, testClusterRequest).futureValue

    eventually {
      dbFutureValue { _.clusterQuery.getClusterStatus(clusterCreateResponse.id) } shouldBe Some(ClusterStatus.Error)
    }

    // IAM call should not have been retried
    iamDAO.invocationCount shouldBe 1
  }

  it should "retry 409 errors when adding dataproc worker role" in isolatedDbTest {
    val mockPetGoogleDAO: String => GoogleStorageDAO = _ => {
      new MockGoogleStorageDAO
    }

    //we meed to use a special version of the MockGoogleIamDAO to simulate a conflict when adding IAM roles
    val iamDAO = new ErroredMockGoogleIamDAO(409)
    leo = new LeonardoService(dataprocConfig, clusterFilesConfig, clusterResourcesConfig, clusterDefaultsConfig, proxyConfig, swaggerConfig, autoFreezeConfig, mockGoogleDataprocDAO, computeDAO, iamDAO, projectDAO, storageDAO, mockPetGoogleDAO, DbSingleton.ref, authProvider, serviceAccountProvider, bucketHelper, contentSecurityPolicy)

    // create the cluster
    val clusterCreateResponse =
      leo.processClusterCreationRequest(userInfo, project, name1, testClusterRequest).futureValue

    eventually(timeout(Span(5, Minutes))) {
      dbFutureValue { _.clusterQuery.getClusterStatus(clusterCreateResponse.id) } shouldBe Some(ClusterStatus.Error)
    }

    // IAM call should have been retried exponentially
    iamDAO.invocationCount shouldBe exponentialBackOffIntervals.size + 1
  }

  it should "update the autopause threshold for a cluster" in isolatedDbTest {
    // create the cluster
    val clusterCreateResponse =
      leo.processClusterCreationRequest(userInfo, project, name1, testClusterRequest).futureValue

    // set the cluster to Running
    dbFutureValue { _.clusterQuery.setToRunning(clusterCreateResponse.id, IP("1.2.3.4")) }

    leo.updateCluster(userInfo, project, name1, testClusterRequest.copy(autopause = Some(true), autopauseThreshold = Some(7))).futureValue

    //check that status of cluster is still Running
    dbFutureValue { _.clusterQuery.getClusterStatus(clusterCreateResponse.id) } shouldBe Some(ClusterStatus.Running)

    //check that the autopause threshold has been updated
    dbFutureValue { _.clusterQuery.getClusterById(clusterCreateResponse.id) }.get.autopauseThreshold shouldBe 7
  }

  it should "update the master machine type for a cluster" in isolatedDbTest {
    // create the cluster
    val clusterCreateResponse =
      leo.processClusterCreationRequest(userInfo, project, name1, testClusterRequest).futureValue

    // set the cluster to Stopped
    dbFutureValue { _.clusterQuery.updateClusterStatus(clusterCreateResponse.id, Stopped) }

    val newMachineType = "n1-micro-1"
    leo.updateCluster(userInfo, project, name1, testClusterRequest.copy(machineConfig = Some(MachineConfig(masterMachineType = Some(newMachineType))))).futureValue

    //check that status of cluster is still Stopped
    dbFutureValue { _.clusterQuery.getClusterStatus(clusterCreateResponse.id) } shouldBe Some(ClusterStatus.Stopped)

    //check that the machine config has been updated
    dbFutureValue { _.clusterQuery.getClusterById(clusterCreateResponse.id) }.get.machineConfig.masterMachineType shouldBe Some(newMachineType)
  }

  it should "not allow changing the master machine type for a cluster in RUNNING state" in isolatedDbTest {
    // create the cluster
    val clusterCreateResponse =
      leo.processClusterCreationRequest(userInfo, project, name1, testClusterRequest).futureValue

    // set the cluster to Running
    dbFutureValue { _.clusterQuery.setToRunning(clusterCreateResponse.id, IP("1.2.3.4")) }

    val newMachineType = "n1-micro-1"
    val failure = leo.updateCluster(userInfo, project, name1, testClusterRequest.copy(machineConfig = Some(MachineConfig(masterMachineType = Some(newMachineType))))).failed.futureValue

    //check that status of cluster is still Running
    dbFutureValue { _.clusterQuery.getClusterStatus(clusterCreateResponse.id) } shouldBe Some(ClusterStatus.Running)

    failure shouldBe a [ClusterMachineTypeCannotBeChangedException]
  }

  it should "update the master disk size for a cluster" in isolatedDbTest {
    // create the cluster
    val clusterCreateResponse =
      leo.processClusterCreationRequest(userInfo, project, name1, testClusterRequest).futureValue

    // set the cluster to Running
    dbFutureValue { _.clusterQuery.setToRunning(clusterCreateResponse.id, IP("1.2.3.4")) }

    val newDiskSize = 1000
    leo.updateCluster(userInfo, project, name1, testClusterRequest.copy(machineConfig = Some(MachineConfig(masterDiskSize = Some(newDiskSize))))).futureValue

    //check that status of cluster is still Running
    dbFutureValue { _.clusterQuery.getClusterStatus(clusterCreateResponse.id) } shouldBe Some(ClusterStatus.Running)

    //check that the machine config has been updated
    dbFutureValue { _.clusterQuery.getClusterById(clusterCreateResponse.id) }.get.machineConfig.masterDiskSize shouldBe Some(newDiskSize)
  }

  it should "not allow decreasing the master disk size for a cluster" in isolatedDbTest {
    // create the cluster
    val clusterCreateResponse =
      leo.processClusterCreationRequest(userInfo, project, name1, testClusterRequest).futureValue

    // set the cluster to Running
    dbFutureValue { _.clusterQuery.setToRunning(clusterCreateResponse.id, IP("1.2.3.4")) }

    val newDiskSize = 10
    val failure = leo.updateCluster(userInfo, project, name1, testClusterRequest.copy(machineConfig = Some(MachineConfig(masterDiskSize = Some(newDiskSize))))).failed.futureValue

    //check that status of cluster is still Running
    dbFutureValue { _.clusterQuery.getClusterStatus(clusterCreateResponse.id) } shouldBe Some(ClusterStatus.Running)

    failure shouldBe a [ClusterDiskSizeCannotBeDecreasedException]
  }

  ClusterStatus.monitoredStatuses foreach { status =>
    it should s"not allow updating a cluster in ${status.toString} state" in isolatedDbTest {
      // create the cluster
      val clusterCreateResponse =
        leo.processClusterCreationRequest(userInfo, project, name1, testClusterRequest).futureValue

      // set the cluster to Running
      dbFutureValue { _.clusterQuery.updateClusterStatusAndHostIp(clusterCreateResponse.id, status, Some(IP("1.2.3.4"))) }

      intercept[ClusterCannotBeUpdatedException] {
        Await.result(leo.updateCluster(userInfo, project, name1, testClusterRequest.copy(machineConfig = Some(MachineConfig(numberOfWorkers = Some(2))))), Duration.Inf)
      }

      //unfortunately we can't actually check that the new instances were added because the monitor
      //handles that but we will check as much as we can

      //check that status of cluster is Updating
      dbFutureValue { _.clusterQuery.getClusterStatus(clusterCreateResponse.id) } shouldBe Some(status)
    }
  }

  it should "stop a cluster created via v2 API" in isolatedDbTest {
    // check that the cluster does not exist
    gdDAO.clusters should not contain key(name1)

    // create the cluster
    val clusterCreateResponse =
      leo.processClusterCreationRequest(userInfo, project, name1, testClusterRequest).futureValue

    eventually {
      // check that the cluster was created
      gdDAO.clusters should contain key name1
    }

    // populate some instances for the cluster
    dbFutureValue { _.instanceQuery.saveAllForCluster(
        getClusterId(clusterCreateResponse), Seq(masterInstance, workerInstance1, workerInstance2)) }

    // set the cluster to Running
    dbFutureValue { _.clusterQuery.setToRunning(clusterCreateResponse.id, IP("1.2.3.4")) }

    // stop the cluster
    leo.stopCluster(userInfo, project, name1).futureValue

    // cluster should still exist in Google
    gdDAO.clusters should contain key (name1)

    // cluster status should be Stopping in the DB
    val status = dbFutureValue { _.clusterQuery.getClusterByUniqueKey(clusterCreateResponse) }.get.status
    status shouldBe ClusterStatus.Stopping

    // instance status should still be Running in the DB
    // the ClusterMonitorActor is what updates instance status
    val instances = dbFutureValue { _.instanceQuery.getAllForCluster(getClusterId(clusterCreateResponse)) }
    instances.size shouldBe 3
    instances.map(_.status).toSet shouldBe Set(InstanceStatus.Running)
  }

  it should "start a cluster" in isolatedDbTest {
    // check that the cluster does not exist
    gdDAO.clusters should not contain key(name1)

    // create the cluster
    val clusterCreateResponse = leo.createCluster(userInfo, project, name1, testClusterRequest).futureValue

    // check that the cluster was created
    gdDAO.clusters should contain key name1

    // populate some instances for the cluster and set its status to Stopped
    dbFutureValue { _.instanceQuery.saveAllForCluster(getClusterId(clusterCreateResponse), Seq(masterInstance, workerInstance1, workerInstance2).map(_.copy(status = InstanceStatus.Stopped))) }
    dbFutureValue { _.clusterQuery.updateClusterStatus(clusterCreateResponse.id, ClusterStatus.Stopped) }

    // start the cluster
    leo.startCluster(userInfo, project, name1).futureValue

    // cluster should still exist in Google
    gdDAO.clusters should contain key (name1)

    // cluster status should be Starting in the DB
    dbFutureValue { _.clusterQuery.getClusterByUniqueKey(clusterCreateResponse)}.get
      .status shouldBe ClusterStatus.Starting

    // instance status should still be Stopped in the DB
    // the ClusterMonitorActor is what updates instance status
    val instances = dbFutureValue {
      _.instanceQuery.getAllForCluster(getClusterId(clusterCreateResponse))
    }
    instances.size shouldBe 3
    instances.map(_.status).toSet shouldBe Set(InstanceStatus.Stopped)
  }

  it should "start a cluster created via v2 API" in isolatedDbTest {
    // check that the cluster does not exist
    gdDAO.clusters should not contain key(name1)

    // create the cluster
    val clusterCreateResponse =
      leo.processClusterCreationRequest(userInfo, project, name1, testClusterRequest).futureValue

    eventually {
      // check that the cluster was created
      gdDAO.clusters should contain key name1
    }

    // populate some instances for the cluster and set its status to Stopped
    dbFutureValue { _.instanceQuery.saveAllForCluster(getClusterId(clusterCreateResponse), Seq(masterInstance, workerInstance1, workerInstance2).map(_.copy(status = InstanceStatus.Stopped))) }
    dbFutureValue { _.clusterQuery.updateClusterStatus(clusterCreateResponse.id, ClusterStatus.Stopped) }

    // start the cluster
    leo.startCluster(userInfo, project, name1).futureValue

    // cluster should still exist in Google
    gdDAO.clusters should contain key (name1)

    // cluster status should be Starting in the DB
    dbFutureValue { _.clusterQuery.getClusterByUniqueKey(clusterCreateResponse) }.get
      .status shouldBe ClusterStatus.Starting

    // instance status should still be Stopped in the DB
    // the ClusterMonitorActor is what updates instance status
    val instances = dbFutureValue { _.instanceQuery.getAllForCluster(getClusterId(clusterCreateResponse)) }
    instances.size shouldBe 3
    instances.map(_.status).toSet shouldBe Set(InstanceStatus.Stopped)
  }

  it should "update disk size for 0 workers when a consumer specifies numberOfPreemptibleWorkers" in isolatedDbTest {
    val clusterRequest = testClusterRequest.copy(
      machineConfig = Some(singleNodeDefaultMachineConfig),
      stopAfterCreation = Some(true))

    val clusterCreateResponse = leo.processClusterCreationRequest(userInfo, project, name1, clusterRequest).futureValue

    dbFutureValue { _.clusterQuery.setToRunning(clusterCreateResponse.id, IP("1.2.3.4")) }

    val newDiskSize = 1000
    leo.updateCluster(userInfo, project, name1, testClusterRequest.copy(machineConfig = Some(MachineConfig(masterDiskSize = Some(newDiskSize), numberOfPreemptibleWorkers = Some(0))))).futureValue


    //check that status of cluster is still Running
    dbFutureValue { _.clusterQuery.getClusterStatus(clusterCreateResponse.id) } shouldBe Some(ClusterStatus.Running)

    //check that the machine config has been updated
    dbFutureValue { _.clusterQuery.getClusterById(clusterCreateResponse.id) }.get.machineConfig.masterDiskSize shouldBe Some(newDiskSize)
  }

  type ClusterCreationInput = (UserInfo, GoogleProject, ClusterName, ClusterRequest)
  type ClusterCreation = ClusterCreationInput => Future[Cluster]

  private def forallClusterCreationMethods(testCode: (ClusterCreation, ClusterName) => Any): Unit = {
    forallClusterCreationMethods(Seq((leo.createCluster _).tupled, (leo.processClusterCreationRequest _).tupled))(Seq(name1, name2)) {
      testCode
    }
  }

  private def forallClusterCreationMethods(creationMethods: Seq[ClusterCreation])
                                          (clusterNames: Seq[ClusterName])
                                          (testCode: (ClusterCreation, ClusterName) => Any): Unit = {
    creationMethods
      .zip(clusterNames)
      .foreach { case (creationMethod, clusterName) => testCode(creationMethod, clusterName) }
  }

  private class ErroredMockGoogleDataprocDAO extends MockGoogleDataprocDAO {
    override def resizeCluster(googleProject: GoogleProject, clusterName: ClusterName, numWorkers: Option[Int] = None, numPreemptibles: Option[Int] = None): Future[Unit] = {
      val jsonFactory = new MockJsonFactory
      val testException = GoogleJsonResponseExceptionFactoryTesting.newMock(jsonFactory, 400, "oh no i have failed due to a bad configuration")

      Future.failed(testException)
    }
  }

  private class ErroredMockGoogleIamDAO(statusCode: Int = 400) extends MockGoogleIamDAO {
    var invocationCount = 0
    override def addIamRolesForUser(iamProject: GoogleProject, email: WorkbenchEmail, rolesToAdd: Set[String]): Future[Unit] = {
      invocationCount += 1
      val jsonFactory = new MockJsonFactory
      val testException = GoogleJsonResponseExceptionFactoryTesting.newMock(jsonFactory, statusCode, "oh no i have failed")

      Future.failed(testException)
    }
  }
}
