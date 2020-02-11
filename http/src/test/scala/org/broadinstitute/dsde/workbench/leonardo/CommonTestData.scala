package org.broadinstitute.dsde.workbench.leonardo

import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.model.headers.{HttpCookiePair, OAuth2BearerToken}
import cats.effect.IO
import cats.mtl.ApplicativeAsk
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleDataprocDAO
import org.broadinstitute.dsde.workbench.google2.mock.BaseFakeGoogleStorage
import org.broadinstitute.dsde.workbench.leonardo.auth.WhitelistAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.auth.sam.MockPetClusterServiceAccountProvider
import org.broadinstitute.dsde.workbench.leonardo.config.Config._
import org.broadinstitute.dsde.workbench.leonardo.config._
import org.broadinstitute.dsde.workbench.leonardo.dao.MockSamDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.google.MockGoogleComputeDAO
import org.broadinstitute.dsde.workbench.leonardo.ClusterImageType._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.model.google.{
  GoogleProject,
  ServiceAccountKey,
  ServiceAccountKeyId,
  ServiceAccountPrivateKeyData,
  _
}
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail, WorkbenchUserId}

object CommonTestData {
// values common to multiple tests, to reduce boilerplate
  val name0 = ClusterName("clustername0")
  val name1 = ClusterName("clustername1")
  val name2 = ClusterName("clustername2")
  val name3 = ClusterName("clustername3")
  val internalId = ClusterInternalId("067e2867-5d4a-47f3-a53c-fd711529b287")
  val project = GoogleProject("dsp-leo-test")
  val project2 = GoogleProject("dsp-leo-test-2")
  val userEmail = WorkbenchEmail("user1@example.com")
  val userInfo = UserInfo(OAuth2BearerToken("accessToken"), WorkbenchUserId("user1"), userEmail, 0)
  val serviceAccountEmail = WorkbenchEmail("pet-1234567890@test-project.iam.gserviceaccount.com")
  val unauthorizedEmail = WorkbenchEmail("somecreep@example.com")
  val unauthorizedUserInfo =
    UserInfo(OAuth2BearerToken("accessToken"), WorkbenchUserId("somecreep"), unauthorizedEmail, 0)
  val jupyterExtensionUri = GcsPath(GcsBucketName("extension_bucket"), GcsObjectName("extension_path"))
  val jupyterUserScriptBucketName = GcsBucketName("userscript_bucket")
  val jupyterUserScriptObjectName = GcsObjectName("userscript.sh")
  val jupyterUserScriptUri = UserScriptPath.Gcs(GcsPath(jupyterUserScriptBucketName, jupyterUserScriptObjectName))
  val jupyterStartUserScriptBucketName = GcsBucketName("startscript_bucket")
  val jupyterStartUserScriptObjectName = GcsObjectName("startscript.sh")
  val jupyterStartUserScriptUri =
    UserScriptPath.Gcs(GcsPath(jupyterStartUserScriptBucketName, jupyterStartUserScriptObjectName))
  val serviceAccountKey = ServiceAccountKey(ServiceAccountKeyId("123"),
                                            ServiceAccountPrivateKeyData("abcdefg"),
                                            Some(Instant.now),
                                            Some(Instant.now.plusSeconds(300)))
  val initBucketPath = GcsBucketName("init-bucket-path")
  val stagingBucketName = GcsBucketName("staging-bucket-name")
  val autopause = true
  val autopauseThreshold = 30
  val defaultScopes = Set(
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/source.read_only"
  )

  val config = ConfigFactory.parseResources("reference.conf").withFallback(ConfigFactory.load()).resolve()
  val dataprocImageProjectGroupName = config.getString("google.groups.dataprocImageProjectGroupName")
  val dataprocImageProjectGroupEmail = WorkbenchEmail(config.getString("google.groups.dataprocImageProjectGroupEmail"))
  val whitelistAuthConfig = config.getConfig("auth.whitelistProviderConfig")
  val whitelist = config.as[Set[String]]("auth.whitelistProviderConfig.whitelist").map(_.toLowerCase)
  val googleGroupsConfig = config.as[GoogleGroupsConfig]("google.groups")
  val dataprocConfig = config.as[DataprocConfig]("dataproc")
  val imageConfig = config.as[ImageConfig]("image")
  val welderConfig = config.as[WelderConfig]("welder")
  val clusterFilesConfig = config.as[ClusterFilesConfig]("clusterFiles")
  val clusterResourcesConfig = config.as[ClusterResourcesConfig]("clusterResources")
  val clusterDefaultsConfig = config.as[ClusterDefaultsConfig]("clusterDefaults")
  val proxyConfig = config.as[ProxyConfig]("proxy")
  val swaggerConfig = config.as[SwaggerConfig]("swagger")
  val autoFreezeConfig = config.as[AutoFreezeConfig]("autoFreeze")
  val zombieClusterConfig = config.as[ZombieClusterConfig]("zombieClusterMonitor")
  val clusterToolConfig = config.as[ClusterToolConfig](path = "clusterToolMonitor")
  val dnsCacheConfig = config.as[ClusterDnsCacheConfig]("clusterDnsCache")
  val clusterUrlBase = dataprocConfig.clusterUrlBase
  val monitorConfig = config.as[MonitorConfig]("monitor")
  val clusterBucketConfig = config.as[ClusterBucketConfig]("clusterBucket")
  val contentSecurityPolicy =
    config.as[Option[String]]("jupyterConfig.contentSecurityPolicy").getOrElse("default-src: 'self'")
  val singleNodeDefaultMachineConfigRequest = RuntimeConfigRequest.DataprocConfig(
    Some(clusterDefaultsConfig.numberOfWorkers),
    Some(clusterDefaultsConfig.masterMachineType),
    Some(clusterDefaultsConfig.masterDiskSize)
  )
  val singleNodeDefaultMachineConfig =
    singleNodeDefaultMachineConfigRequest.toRuntimeConfigDataprocConfig(
      MachineConfigOps.createFromDefaults(clusterDefaultsConfig)
    )

  val testClusterRequest = ClusterRequest(
    Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"),
    None,
    None,
    None,
    None,
    Map.empty,
    None,
    false,
    Some(UserJupyterExtensionConfig(Map("abc" -> "def"), Map("pqr" -> "pqr"), Map("xyz" -> "xyz"))),
    Some(true),
    Some(30),
    Some("ThisIsADefaultClientID")
  )
  val testClusterRequestWithExtensionAndScript = ClusterRequest(
    Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"),
    Some(jupyterExtensionUri),
    Some(jupyterUserScriptUri),
    Some(jupyterStartUserScriptUri),
    None,
    Map.empty,
    None,
    false,
    Some(UserJupyterExtensionConfig(Map("abc" -> "def"), Map("pqr" -> "pqr"), Map("xyz" -> "xyz"))),
    None,
    Some(30),
    Some("ThisIsADefaultClientID")
  )

  val mockSamDAO = new MockSamDAO
  val mockGoogleDataprocDAO = new MockGoogleDataprocDAO
  val mockGoogleComputeDAO = new MockGoogleComputeDAO
  val mockGoogle2StorageDAO = new BaseFakeGoogleStorage

  val defaultUserInfo =
    UserInfo(OAuth2BearerToken("accessToken"), WorkbenchUserId("user1"), WorkbenchEmail("user1@example.com"), 0)
  val tokenAge = 500000
  val tokenName = "LeoToken"
  val tokenValue = "accessToken"
  val tokenCookie = HttpCookiePair(tokenName, tokenValue)

  val clusterServiceAccount = Some(WorkbenchEmail("testClusterServiceAccount@example.com"))
  val notebookServiceAccount = Some(WorkbenchEmail("testNotebookServiceAccount@example.com"))
  val serviceAccountInfo = ServiceAccountInfo(clusterServiceAccount, notebookServiceAccount)

  val auditInfo = AuditInfo(userEmail, Instant.now(), None, Instant.now(), None)
  val jupyterImage = ClusterImage(Jupyter, "jupyter/jupyter-base:latest", Instant.now)
  val rstudioImage = ClusterImage(RStudio, "rocker/tidyverse:latest", Instant.now)
  val welderImage = ClusterImage(Welder, "welder/welder:latest", Instant.now)
  val customDataprocImage = ClusterImage(CustomDataProc, "custom_dataproc", Instant.now)

  val clusterResourceConstraints = ClusterResourceConstraints(MemorySize.fromMb(3584))

  def makeDataprocInfo(index: Int): DataprocInfo =
    DataprocInfo(
      UUID.randomUUID(),
      OperationName("operationName" + index.toString),
      GcsBucketName("stagingbucketname" + index.toString),
      Some(IP("numbers.and.dots"))
    )

  val defaultRuntimeConfig = RuntimeConfig.DataprocConfig(0, "", 500)
  val defaultRuntimeConfigRequest = RuntimeConfigRequest.DataprocConfig(Some(0), Some(""), Some(500))
  def makeCluster(index: Int): Cluster = {
    val clusterName = ClusterName("clustername" + index.toString)
    Cluster(
      clusterName = clusterName,
      internalId = internalId,
      googleProject = project,
      serviceAccountInfo = serviceAccountInfo,
      dataprocInfo = Some(makeDataprocInfo(index)),
      auditInfo = auditInfo,
      properties = Map.empty,
      clusterUrl = Cluster.getClusterUrl(project, clusterName, Set(jupyterImage), Map.empty),
      status = ClusterStatus.Unknown,
      labels = Map(),
      jupyterExtensionUri = None,
      jupyterUserScriptUri = None,
      jupyterStartUserScriptUri = None,
      errors = List.empty,
      instances = Set.empty,
      userJupyterExtensionConfig = None,
      autopauseThreshold = 30,
      defaultClientId = Some("defaultClientId"),
      stopAfterCreation = false,
      allowStop = false,
      clusterImages = Set(jupyterImage),
      scopes = defaultScopes,
      welderEnabled = false,
      customClusterEnvironmentVariables = Map.empty
    )
  }

  val testCluster = new Cluster(
    clusterName = name1,
    internalId = internalId,
    googleProject = project,
    serviceAccountInfo = serviceAccountInfo,
    dataprocInfo = Some(DataprocInfo(UUID.randomUUID(), OperationName("op"), GcsBucketName("testStagingBucket1"), None)),
    auditInfo = AuditInfo(userEmail, Instant.now(), None, Instant.now(), None),
    properties = Map.empty,
    clusterUrl = Cluster.getClusterUrl(project, name1, Set(jupyterImage), Map.empty),
    status = ClusterStatus.Unknown,
    labels = Map(),
    jupyterExtensionUri = Some(GcsPath(GcsBucketName("bucketName"), GcsObjectName("extension"))),
    jupyterUserScriptUri = Some(UserScriptPath.Gcs(GcsPath(GcsBucketName("bucketName"), GcsObjectName("userScript")))),
    jupyterStartUserScriptUri =
      Some(UserScriptPath.Gcs(GcsPath(GcsBucketName("bucketName"), GcsObjectName("startScript")))),
    errors = List.empty,
    instances = Set.empty,
    userJupyterExtensionConfig = None,
    autopauseThreshold = if (autopause) autopauseThreshold else 0,
    defaultClientId = None,
    stopAfterCreation = false,
    allowStop = false,
    clusterImages = Set(jupyterImage),
    scopes = defaultScopes,
    welderEnabled = false,
    customClusterEnvironmentVariables = Map.empty
  )

  // TODO look into parameterized tests so both provider impls can be tested
  // Also remove code duplication with LeonardoServiceSpec, TestLeoRoutes, and CommonTestData
  val serviceAccountProvider = new MockPetClusterServiceAccountProvider
  val whitelistAuthProvider = new WhitelistAuthProvider(whitelistAuthConfig, serviceAccountProvider)

  val userExtConfig = UserJupyterExtensionConfig(Map("nbExt1" -> "abc", "nbExt2" -> "def"),
                                                 Map("serverExt1" -> "pqr"),
                                                 Map("combinedExt1" -> "xyz"))

  val traceId = ApplicativeAsk.const[IO, TraceId](TraceId(UUID.randomUUID())) //we don't care much about traceId in unit tests, hence providing a constant UUID here

  def clusterServiceAccountFromProject(googleProject: GoogleProject): Option[WorkbenchEmail] =
    serviceAccountProvider.getClusterServiceAccount(userInfo, googleProject)(traceId).unsafeRunSync()

  def notebookServiceAccountFromProject(googleProject: GoogleProject): Option[WorkbenchEmail] =
    serviceAccountProvider.getNotebookServiceAccount(userInfo, googleProject)(traceId).unsafeRunSync()

  val masterInstance = Instance(
    InstanceKey(project, ZoneUri("my-zone"), InstanceName("master-instance")),
    googleId = BigInt(12345),
    status = InstanceStatus.Running,
    ip = Some(IP("1.2.3.4")),
    dataprocRole = Some(DataprocRole.Master),
    createdDate = Instant.now()
  )

  val workerInstance1 = Instance(
    InstanceKey(project, ZoneUri("my-zone"), InstanceName("worker-instance-1")),
    googleId = BigInt(23456),
    status = InstanceStatus.Running,
    ip = Some(IP("1.2.3.5")),
    dataprocRole = Some(DataprocRole.Worker),
    createdDate = Instant.now()
  )

  val workerInstance2 = Instance(
    InstanceKey(project, ZoneUri("my-zone"), InstanceName("worker-instance-2")),
    googleId = BigInt(34567),
    status = InstanceStatus.Running,
    ip = Some(IP("1.2.3.6")),
    dataprocRole = Some(DataprocRole.Worker),
    createdDate = Instant.now()
  )

  def modifyInstance(instance: Instance): Instance =
    instance.copy(key = modifyInstanceKey(instance.key), googleId = instance.googleId + 1)
  def modifyInstanceKey(instanceKey: InstanceKey): InstanceKey =
    instanceKey.copy(name = InstanceName(instanceKey.name.value + "_2"))
}

trait GcsPathUtils {
  def gcsPath(str: String): GcsPath =
    parseGcsPath(str).right.get
}
