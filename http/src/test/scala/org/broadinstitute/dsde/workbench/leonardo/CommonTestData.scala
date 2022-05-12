package org.broadinstitute.dsde.workbench.leonardo

import akka.http.scaladsl.model.Uri.Host
import akka.http.scaladsl.model.headers.{HttpCookiePair, OAuth2BearerToken}
import cats.effect.IO
import cats.effect.Ref
import cats.effect.unsafe.implicits.global
import cats.mtl.Ask
import com.google.auth.oauth2.{AccessToken, GoogleCredentials}
import com.google.cloud.compute.v1.Instance.Status
import com.google.cloud.compute.v1._
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.google2.mock.BaseFakeGoogleStorage
import org.broadinstitute.dsde.workbench.google2.{
  DataprocRole,
  DiskName,
  InstanceName,
  MachineTypeName,
  NetworkName,
  OperationName,
  RegionName,
  SubnetworkName,
  ZoneName
}
import org.broadinstitute.dsde.workbench.leonardo
import org.broadinstitute.dsde.workbench.leonardo.ContainerRegistry.DockerHub
import org.broadinstitute.dsde.workbench.leonardo.RuntimeImageType.{
  BootSource,
  CryptoDetector,
  Jupyter,
  Proxy,
  RStudio,
  Welder
}
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId._
import org.broadinstitute.dsde.workbench.leonardo.auth.{MockPetClusterServiceAccountProvider, WhitelistAuthProvider}
import org.broadinstitute.dsde.workbench.leonardo.config._
import org.broadinstitute.dsde.workbench.leonardo.dao.{
  AccessScope,
  CloningInstructions,
  ControlledResourceCommonFields,
  ControlledResourceDescription,
  ControlledResourceIamRole,
  ControlledResourceName,
  ManagedBy,
  MockSamDAO,
  PrivateResourceUser
}
import org.broadinstitute.dsde.workbench.leonardo.db.ClusterRecord
import org.broadinstitute.dsde.workbench.leonardo.http.{
  userScriptStartupOutputUriMetadataKey,
  ConfigReader,
  CreateRuntime2Request,
  RuntimeConfigRequest
}
import org.broadinstitute.dsde.workbench.model._
import org.broadinstitute.dsde.workbench.model.google.{
  GoogleProject,
  ServiceAccountKey,
  ServiceAccountKeyId,
  ServiceAccountPrivateKeyData,
  _
}

import java.nio.file.Paths
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.{Date, UUID}
import com.azure.resourcemanager.compute.models.VirtualMachineSizeTypes
import org.broadinstitute.dsde.workbench.leonardo.http.service.AzureServiceConfig
import org.broadinstitute.dsde.workbench.oauth2.mock.FakeOpenIDConnectConfiguration

import scala.concurrent.duration._

object CommonTestData {
// values common to multiple tests, to reduce boilerplate
  val name0 = RuntimeName("clustername0")
  val name1 = RuntimeName("clustername1")
  val name2 = RuntimeName("clustername2")
  val name3 = RuntimeName("clustername3")
  val runtimeSamResource = RuntimeSamResourceId("067e2867-5d4a-47f3-a53c-fd711529b287")
  val project = GoogleProject("dsp-leo-test")
  val cloudContext = CloudContext.Gcp(project)
  val project2 = GoogleProject("dsp-leo-test-2")
  val cloudContext2 = CloudContext.Gcp(project2)
  val userEmail = WorkbenchEmail("user1@example.com")
  val userEmail2 = WorkbenchEmail("user2@example.com")
  val userInfo = UserInfo(OAuth2BearerToken("accessToken"), WorkbenchUserId("user1"), userEmail, 0)
  val serviceAccountEmail = WorkbenchEmail("pet-1234567890@test-project.iam.gserviceaccount.com")
  val unauthorizedEmail = WorkbenchEmail("somecreep@example.com")
  val unauthorizedUserInfo =
    UserInfo(OAuth2BearerToken("accessToken"), WorkbenchUserId("somecreep"), unauthorizedEmail, 0)
  val credentials =
    GoogleCredentials.create(new AccessToken("accessToken", new Date(1000000000000000L))) // don't refresh this token
  val jupyterExtensionBucket = GcsBucketName("bucket-name")
  val jupyterExtensionObject = GcsObjectName("extension")
  val userJupyterExtensionConfig =
    UserJupyterExtensionConfig(nbExtensions =
      Map("notebookExtension" -> s"gs://${jupyterExtensionBucket.value}/${jupyterExtensionObject.value}")
    )
  val userScriptBucketName = GcsBucketName("userscript_bucket")
  val userScriptObjectName = GcsObjectName("userscript.sh")
  val userScriptUri = UserScriptPath.Gcs(GcsPath(userScriptBucketName, userScriptObjectName))
  val startUserScriptBucketName = GcsBucketName("startscript_bucket")
  val startUserScriptObjectName = GcsObjectName("startscript.sh")
  val startUserScriptUri =
    UserScriptPath.Gcs(GcsPath(startUserScriptBucketName, startUserScriptObjectName))
  val serviceAccountKey = ServiceAccountKey(
    ServiceAccountKeyId("123"),
    ServiceAccountPrivateKeyData("abcdefg"),
    Some(Instant.now.truncatedTo(ChronoUnit.MICROS)),
    Some(Instant.now.plusSeconds(300).truncatedTo(ChronoUnit.MICROS))
  )
  val initBucketName = GcsBucketName("init-bucket-path")
  val stagingBucketName = GcsBucketName("staging-bucket-name")
  val autopause = true
  val autopauseThreshold = 30
  val defaultScopes = Set(
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/source.read_only"
  )
  val zone = ZoneName("us-central1-a")
  val diskName = DiskName("disk-name")
  val googleId = ProxyHostName("google-id")
  val diskSamResource = PersistentDiskSamResourceId("disk-resource-id")
  val diskSize = DiskSize(250)
  val diskType = leonardo.DiskType.Standard
  val blockSize = BlockSize(4096)

  val config = ConfigFactory.parseResources("reference.conf").withFallback(ConfigFactory.load()).resolve()
  val applicationConfig = Config.applicationConfig
  val whitelistAuthConfig = config.getConfig("auth.whitelistProviderConfig")
  val whitelist = config.as[Set[String]]("auth.whitelistProviderConfig.whitelist").map(_.toLowerCase)
  // Let's not use this pattern and directly use `Config.???` going forward :)
  // By using Config.xxx, we'll be actually testing our Config.scala code as well
  val dataprocConfig = Config.dataprocConfig
  val vpcConfig = Config.vpcConfig
  val imageConfig = Config.imageConfig
  val welderConfig = Config.welderConfig
  val clusterFilesConfig = Config.securityFilesConfig
  val clusterResourcesConfig = Config.clusterResourcesConfig
  val proxyConfig = Config.proxyConfig
  val swaggerConfig = Config.swaggerConfig
  val autoFreezeConfig = Config.autoFreezeConfig
  val clusterToolConfig = Config.clusterToolMonitorConfig
  val proxyUrlBase = proxyConfig.proxyUrlBase
  val clusterBucketConfig = Config.clusterBucketConfig
  val contentSecurityPolicy = Config.contentSecurityPolicy
  val refererConfig = Config.refererConfig
  val leoKubernetesConfig = Config.leoKubernetesConfig
  val openIdConnectionConfiguration = FakeOpenIDConnectConfiguration
  val azureServiceConfig = AzureServiceConfig(
    //For now azure disks share same defaults as normal disks
    ConfigReader.appConfig.persistentDisk,
    ConfigReader.appConfig.azure.service
  )
  val singleNodeDefaultMachineConfig = dataprocConfig.runtimeConfigDefaults
  val singleNodeDefaultMachineConfigRequest = RuntimeConfigRequest.DataprocConfig(
    Some(singleNodeDefaultMachineConfig.numberOfWorkers),
    Some(singleNodeDefaultMachineConfig.masterMachineType),
    Some(singleNodeDefaultMachineConfig.masterDiskSize),
    workerMachineType = None,
    workerDiskSize = None,
    numberOfWorkerLocalSSDs = None,
    numberOfPreemptibleWorkers = None,
    properties = Map.empty,
    region = Some(singleNodeDefaultMachineConfig.region),
    componentGatewayEnabled = singleNodeDefaultMachineConfig.componentGatewayEnabled,
    workerPrivateAccess = singleNodeDefaultMachineConfig.workerPrivateAccess
  )

  val mockSamDAO = new MockSamDAO
  val mockGoogle2StorageDAO = new BaseFakeGoogleStorage

  val tokenAge = 500000
  val defaultUserInfo =
    UserInfo(OAuth2BearerToken("accessToken"), WorkbenchUserId("user1"), WorkbenchEmail("user1@example.com"), tokenAge)
  val tokenName = "LeoToken"
  val tokenValue = "accessToken"
  val tokenCookie = HttpCookiePair(tokenName, tokenValue)

  // assumption: this pet SA is linked to userEmail (user1@example.com)
  val petUserInfo = UserInfo(OAuth2BearerToken("accessToken"), WorkbenchUserId("user1"), serviceAccountEmail, 0)

  val validRefererUri = "http://example.com/this/is/a/test"

  val networkName = NetworkName("default")
  val subNetworkName = SubnetworkName("default")
  val ipRange = IpRange("0.0.0.0/20")
  val networkFields = NetworkFields(networkName, subNetworkName, ipRange)

  val serviceAccount = WorkbenchEmail("testServiceAccount@example.com")

  val auditInfo = AuditInfo(userEmail,
                            Instant.now().truncatedTo(ChronoUnit.MICROS),
                            None,
                            Instant.now().truncatedTo(ChronoUnit.MICROS))
  val olderRuntimeAuditInfo = AuditInfo(userEmail,
                                        Instant.now().minus(1, ChronoUnit.DAYS).truncatedTo(ChronoUnit.MICROS),
                                        None,
                                        Instant.now().truncatedTo(ChronoUnit.MICROS))
  val jupyterImage =
    RuntimeImage(Jupyter,
                 "init-resources/jupyter-base:latest",
                 Some(Paths.get("/home/jupyter")),
                 Instant.now.truncatedTo(ChronoUnit.MICROS))
  val rstudioImage = RuntimeImage(RStudio, "rocker/tidyverse:latest", None, Instant.now.truncatedTo(ChronoUnit.MICROS))
  val welderImage = RuntimeImage(Welder, "welder/welder:latest", None, Instant.now.truncatedTo(ChronoUnit.MICROS))
  val proxyImage =
    RuntimeImage(Proxy, imageConfig.proxyImage.imageUrl, None, Instant.now.truncatedTo(ChronoUnit.MICROS))
  val customDataprocImage =
    RuntimeImage(BootSource, "custom_dataproc", None, Instant.now.truncatedTo(ChronoUnit.MICROS))
  val cryptoDetectorImage =
    RuntimeImage(CryptoDetector, "crypto/crypto:0.0.1", None, Instant.now.truncatedTo(ChronoUnit.MICROS))
  val azureImage = RuntimeImage(RuntimeImageType.Azure, "test", None, Instant.now.truncatedTo(ChronoUnit.MICROS))

  val clusterResourceConstraints = RuntimeResourceConstraints(MemorySize.fromMb(3584))
  val hostToIpMapping = Ref.unsafe[IO, Map[Host, IP]](Map.empty)

  def makeAsyncRuntimeFields(index: Int): AsyncRuntimeFields =
    AsyncRuntimeFields(
      ProxyHostName(UUID.randomUUID().toString),
      OperationName("operationName" + index.toString),
      GcsBucketName("stagingbucketname" + index.toString),
      Some(IP("numbers.and.dots"))
    )
  val defaultMachineType = MachineTypeName("n1-standard-4")
  val defaultDataprocRuntimeConfig =
    RuntimeConfig.DataprocConfig(0,
                                 MachineTypeName("n1-standard-4"),
                                 DiskSize(500),
                                 None,
                                 None,
                                 None,
                                 None,
                                 Map.empty,
                                 RegionName("us-central1"),
                                 true,
                                 true)

  val defaultCreateRuntimeRequest = CreateRuntime2Request(
    Map("lbl1" -> "true"),
    None,
    Some(UserScriptPath.Gcs(GcsPath(GcsBucketName("bucket"), GcsObjectName("script.sh")))),
    Some(RuntimeConfigRequest.GceConfig(Some(MachineTypeName("n1-standard-4")), Some(DiskSize(100)), None, None)),
    None,
    Some(true),
    Some(30.minutes),
    None,
    Some(ContainerImage("myrepo/myimage", DockerHub)),
    Some(DockerHub),
    Set.empty,
    Map.empty
  )
  val defaultGceRuntimeConfig =
    RuntimeConfig.GceConfig(MachineTypeName("n1-standard-4"),
                            DiskSize(500),
                            bootDiskSize = Some(DiskSize(50)),
                            zone = ZoneName("us-west2-b"),
                            None)
  val defaultRuntimeConfigRequest =
    RuntimeConfigRequest.DataprocConfig(
      Some(0),
      Some(MachineTypeName("n1-standard-4")),
      Some(DiskSize(500)),
      None,
      None,
      None,
      None,
      Map.empty[String, String],
      Some(RegionName("us-central1")),
      true,
      true
    )

  val gpuConfig = Some(GpuConfig(GpuType.NvidiaTeslaT4, 2))
  val gceRuntimeConfig =
    RuntimeConfig.GceConfig(MachineTypeName("n1-standard-4"),
                            DiskSize(500),
                            bootDiskSize = Some(DiskSize(50)),
                            zone = ZoneName("us-west2-b"),
                            None)

  val gceRuntimeConfigWithGpu = gceRuntimeConfig.copy(gpuConfig = gpuConfig)
  val gceWithPdRuntimeConfig =
    RuntimeConfig.GceWithPdConfig(MachineTypeName("n1-standard-4"),
                                  Some(DiskId(1234)),
                                  DiskSize(50),
                                  ZoneName("us-west2-b"),
                                  None)

  def makeCluster(index: Int): Runtime = {
    val clusterName = RuntimeName("clustername" + index.toString)
    Runtime(
      id = -1,
      workspaceId = Some(WorkspaceId(UUID.randomUUID())),
      samResource = runtimeSamResource,
      runtimeName = clusterName,
      cloudContext = cloudContext,
      serviceAccount = serviceAccount,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(index)),
      auditInfo = auditInfo,
      kernelFoundBusyDate = None,
      proxyUrl = Runtime.getProxyUrl(proxyUrlBase, cloudContext, clusterName, Set(jupyterImage), Map.empty),
      status = RuntimeStatus.Unknown,
      labels = Map(),
      userScriptUri = None,
      startUserScriptUri = None,
      errors = List.empty,
      userJupyterExtensionConfig = None,
      autopauseThreshold = 30,
      defaultClientId = Some("defaultClientId"),
      allowStop = false,
      runtimeImages = Set(jupyterImage, welderImage),
      scopes = defaultScopes,
      welderEnabled = false,
      customEnvironmentVariables = Map.empty,
      runtimeConfigId = RuntimeConfigId(-1),
      patchInProgress = false
    )
  }

  val testCluster = Runtime(
    id = -1,
    None,
    samResource = runtimeSamResource,
    runtimeName = name1,
    cloudContext = cloudContext,
    serviceAccount = serviceAccount,
    asyncRuntimeFields = Some(
      AsyncRuntimeFields(ProxyHostName(UUID.randomUUID().toString), OperationName("op"), stagingBucketName, None)
    ),
    auditInfo = AuditInfo(userEmail,
                          Instant.now().truncatedTo(ChronoUnit.MICROS),
                          None,
                          Instant.now().truncatedTo(ChronoUnit.MICROS)),
    kernelFoundBusyDate = None,
    proxyUrl = Runtime.getProxyUrl(proxyUrlBase, cloudContext, name1, Set(jupyterImage), Map.empty),
    status = RuntimeStatus.Unknown,
    labels = Map(),
    userScriptUri = Some(UserScriptPath.Gcs(GcsPath(GcsBucketName("bucket-name"), GcsObjectName("userScript")))),
    startUserScriptUri = Some(UserScriptPath.Gcs(GcsPath(GcsBucketName("bucket-name"), GcsObjectName("startScript")))),
    errors = List.empty,
    userJupyterExtensionConfig =
      Some(UserJupyterExtensionConfig(nbExtensions = Map("notebookExtension" -> "gs://bucket-name/extension"))),
    autopauseThreshold = if (autopause) autopauseThreshold else 0,
    defaultClientId = Some("clientId"),
    allowStop = false,
    runtimeImages = Set(jupyterImage, welderImage, proxyImage, cryptoDetectorImage),
    scopes = defaultScopes,
    welderEnabled = true,
    customEnvironmentVariables = Map.empty,
    runtimeConfigId = RuntimeConfigId(-1),
    patchInProgress = false
  )

  val testClusterRecord = ClusterRecord(
    id = -1,
    runtimeName = name1,
    internalId = runtimeSamResource.resourceId,
    cloudContext = cloudContext,
    googleId = testCluster.asyncRuntimeFields.map(_.proxyHostName),
    operationName = testCluster.asyncRuntimeFields.map(_.operationName.value),
    status = testCluster.status,
    auditInfo = testCluster.auditInfo,
    kernelFoundBusyDate = None,
    hostIp = None,
    userScriptUri = Some(UserScriptPath.Gcs(GcsPath(GcsBucketName("bucket-name"), GcsObjectName("userScript")))),
    startUserScriptUri = Some(UserScriptPath.Gcs(GcsPath(GcsBucketName("bucket-name"), GcsObjectName("startScript")))),
    autopauseThreshold = if (autopause) autopauseThreshold else 0,
    defaultClientId = Some("clientId"),
    initBucket = Some(initBucketName.value),
    serviceAccountInfo = serviceAccount,
    stagingBucket = Some(stagingBucketName.value),
    welderEnabled = true,
    customClusterEnvironmentVariables = Map.empty,
    runtimeConfigId = RuntimeConfigId(-1),
    deletedFrom = None,
    workspaceId = None
  )

  val readyInstance = Instance
    .newBuilder()
    .setStatus(Status.RUNNING.toString)
    .setMetadata(
      Metadata
        .newBuilder()
        .addItems(
          Items.newBuilder
            .setKey(userScriptStartupOutputUriMetadataKey)
            .setValue("gs://success/object")
            .build()
        )
        .build()
    )
    .addNetworkInterfaces(
      NetworkInterface
        .newBuilder()
        .addAccessConfigs(AccessConfig.newBuilder().setNatIP("fakeIP").build())
        .build()
    )
    .build()

  def makePersistentDisk(diskName: Option[DiskName] = None,
                         formattedBy: Option[FormattedBy] = None,
                         appRestore: Option[AppRestore] = None,
                         zoneName: Option[ZoneName] = None,
                         cloudContextOpt: Option[CloudContext] = None): PersistentDisk =
    PersistentDisk(
      DiskId(-1),
      cloudContextOpt.getOrElse(cloudContext),
      zoneName.getOrElse(zone),
      diskName.getOrElse(DiskName("disk")),
      serviceAccount,
      diskSamResource,
      DiskStatus.Ready,
      auditInfo,
      diskSize,
      diskType,
      blockSize,
      formattedBy,
      appRestore,
      Map("key1" -> "value1", "key2" -> "value2", "key3" -> "value3")
    )

  // TODO look into parameterized tests so both provider impls can be tested
  // Also remove code duplication with LeonardoServiceSpec, TestLeoRoutes, and CommonTestData
  val serviceAccountProvider = new MockPetClusterServiceAccountProvider
  val whitelistAuthProvider = new WhitelistAuthProvider(whitelistAuthConfig, serviceAccountProvider)

  val userExtConfig = UserJupyterExtensionConfig(Map("nbExt1" -> "abc", "nbExt2" -> "def"),
                                                 Map("serverExt1" -> "pqr"),
                                                 Map("combinedExt1" -> "xyz"))

  val traceId = Ask.const[IO, TraceId](TraceId(UUID.randomUUID())) //we don't care much about traceId in unit tests, hence providing a constant UUID here

  def clusterServiceAccountFromProject(googleProject: GoogleProject): Option[WorkbenchEmail] =
    serviceAccountProvider.getClusterServiceAccount(userInfo, googleProject)(traceId).unsafeRunSync()

  def notebookServiceAccountFromProject(googleProject: GoogleProject): Option[WorkbenchEmail] =
    serviceAccountProvider.getNotebookServiceAccount(userInfo, googleProject)(traceId).unsafeRunSync()

  val masterInstance = DataprocInstance(
    DataprocInstanceKey(project, ZoneName("my-zone"), InstanceName("master-instance")),
    googleId = BigInt(12345),
    status = GceInstanceStatus.Running,
    ip = Some(IP("1.2.3.4")),
    dataprocRole = DataprocRole.Master,
    createdDate = Instant.now().truncatedTo(ChronoUnit.MICROS)
  )

  val workerInstance1 = DataprocInstance(
    DataprocInstanceKey(project, ZoneName("my-zone"), InstanceName("worker-instance-1")),
    googleId = BigInt(23456),
    status = GceInstanceStatus.Running,
    ip = Some(IP("1.2.3.5")),
    dataprocRole = DataprocRole.Worker,
    createdDate = Instant.now().truncatedTo(ChronoUnit.MICROS)
  )

  val workerInstance2 = DataprocInstance(
    DataprocInstanceKey(project, ZoneName("my-zone"), InstanceName("worker-instance-2")),
    googleId = BigInt(34567),
    status = GceInstanceStatus.Running,
    ip = Some(IP("1.2.3.6")),
    dataprocRole = DataprocRole.Worker,
    createdDate = Instant.now().truncatedTo(ChronoUnit.MICROS)
  )

  val azureRegion: com.azure.core.management.Region = com.azure.core.management.Region.US_EAST

  val azureCloudContext =
    AzureCloudContext(TenantId("testTenant"), SubscriptionId("testSubscription"), ManagedResourceGroupName("testMrg"))
  val workspaceId = WorkspaceId(UUID.randomUUID())
  val wsmResourceId = WsmControlledResourceId(UUID.randomUUID())

  val testCommonControlledResourceFields = ControlledResourceCommonFields(
    ControlledResourceName("name"),
    ControlledResourceDescription("desc"),
    CloningInstructions.Nothing,
    AccessScope.PrivateAccess,
    ManagedBy.User,
    Some(
      PrivateResourceUser(
        userEmail,
        List(ControlledResourceIamRole.Editor)
      )
    )
  )

  val defaultCreateAzureRuntimeReq = CreateAzureRuntimeRequest(
    Map.empty,
    azureRegion,
    VirtualMachineSizeTypes.STANDARD_A1,
    Some(AzureImageUri(azureImage.imageUrl)),
    Map.empty,
    CreateAzureDiskRequest(
      Map.empty,
      AzureDiskName("diskName1"),
      Some(DiskSize(100)),
      None
    )
  )

  def modifyInstance(instance: DataprocInstance): DataprocInstance =
    instance.copy(key = modifyInstanceKey(instance.key), googleId = instance.googleId + 1)
  def modifyInstanceKey(instanceKey: DataprocInstanceKey): DataprocInstanceKey =
    instanceKey.copy(name = InstanceName(instanceKey.name.value + "_2"))
}

trait GcsPathUtils {
  def gcsPath(str: String): GcsPath =
    parseGcsPath(str).right.get
}
