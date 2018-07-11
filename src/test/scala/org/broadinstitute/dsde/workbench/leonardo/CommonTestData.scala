package org.broadinstitute.dsde.workbench.leonardo

import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.model.headers.{HttpCookiePair, OAuth2BearerToken}
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.auth.WhitelistAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.auth.sam.MockPetClusterServiceAccountProvider
import org.broadinstitute.dsde.workbench.leonardo.config.{AutoFreezeConfig, ClusterDefaultsConfig, ClusterFilesConfig, ClusterResourcesConfig, DataprocConfig, MonitorConfig, ProxyConfig, SwaggerConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.google.MockGoogleComputeDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.{MockJupyterDAO, MockSamDAO}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.model.google.{GoogleProject, ServiceAccountKey, ServiceAccountKeyId, ServiceAccountPrivateKeyData, _}
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail, WorkbenchUserId}
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

// values common to multiple tests, to reduce boilerplate

trait CommonTestData { this: ScalaFutures =>
  val name0 = ClusterName("name0")
  val name1 = ClusterName("name1")
  val name2 = ClusterName("name2")
  val name3 = ClusterName("name3")
  val project = GoogleProject("dsp-leo-test")
  val userEmail = WorkbenchEmail("user1@example.com")
  val userInfo = UserInfo(OAuth2BearerToken("accessToken"), WorkbenchUserId("user1"), userEmail, 0)
  val serviceAccountEmail = WorkbenchEmail("pet-1234567890@test-project.iam.gserviceaccount.com")
  val unauthorizedEmail = WorkbenchEmail("somecreep@example.com")
  val unauthorizedUserInfo = UserInfo(OAuth2BearerToken("accessToken"), WorkbenchUserId("somecreep"), unauthorizedEmail, 0)
  val jupyterExtensionUri = GcsPath(GcsBucketName("extension_bucket"), GcsObjectName("extension_path"))
  val jupyterUserScriptUri = GcsPath(GcsBucketName("userscript_bucket"), GcsObjectName("userscript.sh"))
  val serviceAccountKey = ServiceAccountKey(ServiceAccountKeyId("123"), ServiceAccountPrivateKeyData("abcdefg"), Some(Instant.now), Some(Instant.now.plusSeconds(300)))
  val initBucketPath = GcsBucketName("bucket-path")
  val autopause = true
  val autopauseThreshold = 30

  val config = ConfigFactory.parseResources("reference.conf").withFallback(ConfigFactory.load())
  val whitelistAuthConfig = config.getConfig("auth.whitelistProviderConfig")
  val whitelist = config.as[Set[String]]("auth.whitelistProviderConfig.whitelist").map(_.toLowerCase)
  val dataprocConfig = config.as[DataprocConfig]("dataproc")
  val clusterFilesConfig = config.as[ClusterFilesConfig]("clusterFiles")
  val clusterResourcesConfig = config.as[ClusterResourcesConfig]("clusterResources")
  val clusterDefaultsConfig = config.as[ClusterDefaultsConfig]("clusterDefaults")
  val proxyConfig = config.as[ProxyConfig]("proxy")
  val swaggerConfig = config.as[SwaggerConfig]("swagger")
  val autoFreezeConfig = config.as[AutoFreezeConfig]("autoFreeze")
  val clusterUrlBase = dataprocConfig.clusterUrlBase
  val serviceAccountsConfig = config.getConfig("serviceAccounts.config")
  val monitorConfig = config.as[MonitorConfig]("monitor")
  val mockJupyterDAO = new MockJupyterDAO
  val singleNodeDefaultMachineConfig = MachineConfig(Some(clusterDefaultsConfig.numberOfWorkers), Some(clusterDefaultsConfig.masterMachineType), Some(clusterDefaultsConfig.masterDiskSize))
  val testClusterRequest = ClusterRequest(Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"), None, None, None, None, Some(UserJupyterExtensionConfig(Map("abc" -> "def"), Map("pqr" -> "pqr"), Map("xyz" -> "xyz"))), Some(true), Some(30))
  val testClusterRequestWithExtensionAndScript = ClusterRequest(Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"), Some(jupyterExtensionUri), Some(jupyterUserScriptUri), None, None, Some(UserJupyterExtensionConfig(Map("abc" -> "def"), Map("pqr" -> "pqr"), Map("xyz" -> "xyz"))), Some(true), Some(30))



  val mockSamDAO = new MockSamDAO
  val mockGoogleDataprocDAO = new MockGoogleDataprocDAO
  val mockGoogleComputeDAO = new MockGoogleComputeDAO

  val defaultUserInfo = UserInfo(OAuth2BearerToken("accessToken"), WorkbenchUserId("user1"), WorkbenchEmail("user1@example.com"), 0)
  val tokenAge = 500000
  val tokenName = "LeoToken"
  val tokenValue = "accessToken"
  val tokenCookie = HttpCookiePair(tokenName, tokenValue)

  val serviceAccountInfo = new ServiceAccountInfo(Option(WorkbenchEmail("testServiceAccount1@example.com")), Option(WorkbenchEmail("testServiceAccount2@example.com")))

  val testCluster = new Cluster(
    clusterName = name1,
    googleId =  Option(UUID.randomUUID()),
    googleProject = project,
    serviceAccountInfo = serviceAccountInfo,
    machineConfig = MachineConfig(Some(0),Some(""), Some(500)),
    clusterUrl = Cluster.getClusterUrl(project, name1, clusterUrlBase),
    operationName = Option(OperationName("op")),
    status = ClusterStatus.Unknown,
    hostIp = None,
    creator = userEmail,
    createdDate = Instant.now(),
    destroyedDate = None,
    labels = Map(),
    jupyterExtensionUri = Option(GcsPath(GcsBucketName("bucketName"), GcsObjectName("extension"))),
    jupyterUserScriptUri = Option(GcsPath(GcsBucketName("bucketName"), GcsObjectName("userScript"))),
    stagingBucket = Some(GcsBucketName("testStagingBucket1")),
    errors = List.empty,
    instances = Set.empty,
    userJupyterExtensionConfig = None,
    dateAccessed = Instant.now(),
    autopauseThreshold = if (autopause) autopauseThreshold else 0)

  // TODO look into parameterized tests so both provider impls can both be tested
  // Also remove code duplication with LeonardoServiceSpec, TestLeoRoutes, and CommonTestData
  val serviceAccountProvider = new MockPetClusterServiceAccountProvider(serviceAccountsConfig)
  val whitelistAuthProvider = new WhitelistAuthProvider(whitelistAuthConfig, serviceAccountProvider)

  val userExtConfig = UserJupyterExtensionConfig(Map("nbExt1"->"abc", "nbExt2"->"def"), Map("serverExt1"->"pqr"), Map("combinedExt1"->"xyz"))

  protected def clusterServiceAccount(googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Option[WorkbenchEmail] = {
    serviceAccountProvider.getClusterServiceAccount(userInfo, googleProject).futureValue
  }

  protected def notebookServiceAccount(googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Option[WorkbenchEmail] = {
    serviceAccountProvider.getNotebookServiceAccount(userInfo, googleProject).futureValue
  }

  val masterInstance = Instance(
    InstanceKey(
      project,
      ZoneUri("my-zone"),
      InstanceName("master-instance")),
    googleId = BigInt(12345),
    status = InstanceStatus.Running,
    ip = Some(IP("1.2.3.4")),
    dataprocRole = Some(DataprocRole.Master),
    createdDate = Instant.now())

  val workerInstance1 = Instance(
    InstanceKey(
      project,
      ZoneUri("my-zone"),
      InstanceName("worker-instance-1")),
    googleId = BigInt(23456),
    status = InstanceStatus.Running,
    ip = Some(IP("1.2.3.5")),
    dataprocRole = Some(DataprocRole.Worker),
    createdDate = Instant.now())

  val workerInstance2 = Instance(
    InstanceKey(
      project,
      ZoneUri("my-zone"),
      InstanceName("worker-instance-2")),
    googleId = BigInt(34567),
    status = InstanceStatus.Running,
    ip = Some(IP("1.2.3.6")),
    dataprocRole = Some(DataprocRole.Worker),
    createdDate = Instant.now())

  protected def modifyInstance(instance: Instance): Instance = {
    instance.copy(key = modifyInstanceKey(instance.key), googleId = instance.googleId + 1)
  }
  protected def modifyInstanceKey(instanceKey: InstanceKey): InstanceKey = {
    instanceKey.copy(name = InstanceName(instanceKey.name.value + "_2"))
  }
}

trait GcsPathUtils {
  def gcsPath(str: String): GcsPath = {
    parseGcsPath(str).right.get
  }
}
