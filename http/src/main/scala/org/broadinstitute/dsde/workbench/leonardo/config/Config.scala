package org.broadinstitute.dsde.workbench.leonardo
package config

import com.google.pubsub.v1.{ProjectSubscriptionName, ProjectTopicName, TopicName}
import com.typesafe.config.{Config => TypeSafeConfig, ConfigFactory}
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ValueReader
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName._
import org.broadinstitute.dsde.workbench.google2.{
  DeviceName,
  FirewallRuleName,
  KubernetesName,
  Location,
  MachineTypeName,
  MaxRetries,
  NetworkName,
  PublisherConfig,
  RegionName,
  SubnetworkName,
  SubscriberConfig,
  SubscriberDeadLetterPolicy,
  ZoneName
}
import org.broadinstitute.dsde.workbench.leonardo.CustomImage.{DataprocCustomImage, GceCustomImage}
import org.broadinstitute.dsde.workbench.leonardo.auth.SamAuthProviderConfig
import org.broadinstitute.dsde.workbench.leonardo.config.ContentSecurityPolicyComponent._
import org.broadinstitute.dsde.workbench.leonardo.dao.{GroupName, HttpSamDaoConfig}
import org.broadinstitute.dsde.workbench.leonardo.http.ConfigReader
import org.broadinstitute.dsde.workbench.leonardo.http.service.AppServiceConfig
import org.broadinstitute.dsde.workbench.leonardo.http.service.LeoAppServiceInterp.LeoKubernetesConfig
import org.broadinstitute.dsde.workbench.leonardo.model.ServiceAccountProviderConfig
import org.broadinstitute.dsde.workbench.leonardo.monitor.MonitorConfig.{DataprocMonitorConfig, GceMonitorConfig}
import org.broadinstitute.dsde.workbench.leonardo.monitor.{
  CreateDiskTimeout,
  DateAccessedUpdaterConfig,
  InterruptablePollMonitorConfig,
  LeoPubsubMessageSubscriberConfig,
  PersistentDiskMonitorConfig,
  PollMonitorConfig
}
import org.broadinstitute.dsde.workbench.leonardo.util.RuntimeInterpreterConfig.{
  DataprocInterpreterConfig,
  GceInterpreterConfig
}
import org.broadinstitute.dsde.workbench.leonardo.util.{GKEInterpreterConfig, VPCInterpreterConfig}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.util.toScalaDuration
import org.broadinstitute.dsp.{ChartName, ChartVersion, Release}
import org.http4s.Uri

import java.net.URL
import java.nio.file.{Path, Paths}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

object Config {

  /** Loads all the configs for the Leo App. All values defined in `src/main/resources/leo.conf` will take precedence over any other configs. In this way, we
   * can still use configs rendered by `firecloud-develop` that render to `config/leonardo.conf` if we want.
   *     If you want to use leo.conf you must populate the appropriate ENV vars (done in terra-helmfile)
   *     leonardo.conf is what firecloud-develop renders, and it will be used if the ENV vars are not populated for leo.conf
   */
  // Config that lives in leo repo in /src/main/resources
  val leoConfig = ConfigFactory.parseResourcesAnySyntax("leo")

  // Config that is generated to /config from firecloud-develop
  val firecloudDevelopConfig = ConfigFactory
    .parseResources("leonardo.conf")

  // Load any other configs on the classpath following: https://github.com/lightbend/config#standard-behavior
  // This is where things like `src/main/resources/reference.conf` will get loaded
  val referenceConfig = ConfigFactory.load()

  // leoConfig has precedence here
  val config = leoConfig
    .withFallback(firecloudDevelopConfig)
    .withFallback(referenceConfig)
    .resolve()

  implicit private val deviceNameReader: ValueReader[DeviceName] = stringValueReader.map(DeviceName)
  implicit private val groupNameReader: ValueReader[GroupName] = stringValueReader.map(GroupName)

  implicit private val applicationConfigReader: ValueReader[ApplicationConfig] = ValueReader.relative { config =>
    ApplicationConfig(
      config.getString("applicationName"),
      config.as[GoogleProject]("leoGoogleProject"),
      config.as[Path]("leoServiceAccountJsonFile"),
      config.as[WorkbenchEmail]("leoServiceAccountEmail"),
      config.as[URL]("leoUrlBase"),
      config.getString("environment"),
      config.as[Long]("concurrency")
    )
  }

  implicit private val dataprocRuntimeConfigReader: ValueReader[RuntimeConfig.DataprocConfig] = ValueReader.relative {
    config =>
      RuntimeConfig.DataprocConfig(
        config.getInt("numberOfWorkers"),
        config.as[MachineTypeName]("masterMachineType"),
        config.as[DiskSize]("masterDiskSize"),
        config.getAs[String]("workerMachineType").map(MachineTypeName),
        config.getAs[DiskSize]("workerDiskSize"),
        config.getAs[Int]("numberOfWorkerLocalSSDs"),
        config.getAs[Int]("numberOfPreemptibleWorkers"),
        Map.empty,
        config.as[RegionName]("region"),
        // Below 2 fields are not used; defaults are defined in RuntimeConfigRequest.DataprocConfig decoder
        false,
        false
      )
  }

  implicit private val gceRuntimeConfigReader: ValueReader[RuntimeConfig.GceConfig] = ValueReader.relative { config =>
    RuntimeConfig.GceConfig(
      machineType = config.as[MachineTypeName]("machineType"),
      diskSize = config.as[DiskSize]("diskSize"),
      bootDiskSize = Some(config.as[DiskSize]("bootDiskSize")),
      zone = config.as[ZoneName]("zone"),
      gpuConfig = None
    )
  }

  implicit private val dataprocConfigReader: ValueReader[DataprocConfig] = ValueReader.relative { config =>
    DataprocConfig(
      config.getStringList("defaultScopes").asScala.toSet,
      config.as[DataprocCustomImage]("customDataprocImage"),
      config.getAs[Double]("sparkMemoryConfigRatio"),
      config.getAs[Double]("minimumRuntimeMemoryInGb"),
      config.as[RuntimeConfig.DataprocConfig]("runtimeDefaults"),
      config.as[Set[RegionName]]("supportedRegions")
    )
  }

  implicit private val gceConfigReader: ValueReader[GceConfig] = ValueReader.relative { config =>
    GceConfig(
      config.as[GceCustomImage]("customGceImage"),
      config.as[DeviceName]("userDiskDeviceName"),
      config.getStringList("defaultScopes").asScala.toSet,
      config.getAs[MemorySize]("gceReservedMemory"),
      config.as[RuntimeConfig.GceConfig]("runtimeDefaults"),
      config.as[FiniteDuration]("setMetadataPollDelay"),
      config.as[Int]("setMetadataPollMaxAttempts")
    )
  }

  implicit private val allowedConfigReader: ValueReader[Allowed] = ValueReader.relative { config =>
    Allowed(
      config.as[String]("protocol"),
      config.as[Option[String]]("port")
    )
  }

  implicit private val googleGroupConfigReader: ValueReader[GoogleGroupsConfig] = ValueReader.relative { config =>
    GoogleGroupsConfig(
      config.as[WorkbenchEmail]("subEmail"),
      config.getString("dataprocImageProjectGroupName"),
      config.as[WorkbenchEmail]("dataprocImageProjectGroupEmail"),
      config.as[PollMonitorConfig]("waitForMemberAddedPollConfig")
    )
  }

  implicit private val imageConfigReader: ValueReader[ImageConfig] = ValueReader.relative { config =>
    val welderGcrUri = config.getString("welderGcrUri")
    val welderDockerHubUri = config.getString("welderDockerHubUri")
    val welderHash = config.getString("welderHash")
    ImageConfig(
      ContainerImage
        .fromPrefixAndHash(welderGcrUri, welderHash)
        .getOrElse(throw new RuntimeException(s"Invalid welder image: $welderGcrUri:$welderHash")),
      ContainerImage
        .fromPrefixAndHash(welderDockerHubUri, welderHash)
        .getOrElse(throw new RuntimeException(s"Invalid welder image: $welderDockerHubUri:$welderHash")),
      config.getString("welderHash"),
      config.as[ContainerImage]("jupyterImage"),
      config.as[ContainerImage]("proxyImage"),
      config.as[ContainerImage]("cryptoDetectorImage"),
      config.getString("jupyterContainerName"),
      config.getString("rstudioContainerName"),
      config.getString("welderContainerName"),
      config.getString("proxyContainerName"),
      config.getString("cryptoDetectorContainerName"),
      config.getString("jupyterImageRegex"),
      config.getString("rstudioImageRegex"),
      config.getString("broadDockerhubImageRegex"),
      Paths.get(config.getString("defaultJupyterUserHome"))
    )
  }

  implicit private val welderConfigReader: ValueReader[WelderConfig] = ValueReader.relative { config =>
    WelderConfig(
      config.getAs[String]("deployWelderLabel"),
      config.getAs[String]("updateWelderLabel"),
      config.getAs[String]("deployWelderCutoffDate"),
      config.getAs[MemorySize]("welderReservedMemory")
    )
  }

  implicit private val clusterResourcesConfigReader: ValueReader[ClusterResourcesConfig] = ValueReader.relative {
    config =>
      ClusterResourcesConfig(
        config.as[RuntimeResource]("initScript"),
        config.getAs[RuntimeResource]("cloudInit"),
        config.as[RuntimeResource]("startupScript"),
        config.as[RuntimeResource]("shutdownScript"),
        config.as[RuntimeResource]("jupyterDockerCompose"),
        config.getAs[RuntimeResource]("gpuDockerCompose"),
        config.as[RuntimeResource]("rstudioDockerCompose"),
        config.as[RuntimeResource]("proxyDockerCompose"),
        config.as[RuntimeResource]("welderDockerCompose"),
        config.as[RuntimeResource]("proxySiteConf"),
        config.as[RuntimeResource]("jupyterNotebookConfigUri"),
        config.as[RuntimeResource]("jupyterNotebookFrontendConfigUri"),
        config.as[RuntimeResource]("customEnvVarsConfigUri")
      )
  }

  implicit private val clusterFilesConfigReader: ValueReader[SecurityFilesConfig] = ValueReader.relative { config =>
    SecurityFilesConfig(
      config.as[Path]("proxyServerCrt"),
      config.as[Path]("proxyServerKey"),
      config.as[Path]("proxyRootCaPem"),
      config.as[Path]("proxyRootCaKey")
    )
  }

  implicit private val cacheConfigValueReader: ValueReader[CacheConfig] = ValueReader.relative { config =>
    CacheConfig(
      toScalaDuration(config.getDuration("cacheExpiryTime")),
      config.getInt("cacheMaxSize")
    )
  }

  implicit private val liquibaseReader: ValueReader[LiquibaseConfig] = ValueReader.relative { config =>
    LiquibaseConfig(config.as[String]("changelog"), config.as[Boolean]("initWithLiquibase"))
  }

  implicit private val samConfigReader: ValueReader[SamConfig] = ValueReader.relative { config =>
    SamConfig(config.getString("server"))
  }

  implicit private val prometheusConfigReader: ValueReader[PrometheusConfig] = ValueReader.relative { config =>
    PrometheusConfig(
      config.getInt("endpointPort")
    )
  }

  implicit private val proxyConfigReader: ValueReader[ProxyConfig] = ValueReader.relative { config =>
    ProxyConfig(
      config.getString("proxyDomain"),
      config.getString("proxyUrlBase"),
      config.getInt("proxyPort"),
      toScalaDuration(config.getDuration("dnsPollPeriod")),
      toScalaDuration(config.getDuration("tokenCacheExpiryTime")),
      config.getInt("tokenCacheMaxSize"),
      toScalaDuration(config.getDuration("internalIdCacheExpiryTime")),
      config.getInt("internalIdCacheMaxSize")
    )
  }

  implicit private val contentSecurityPolicyConfigReader: ValueReader[ContentSecurityPolicyConfig] =
    ValueReader.relative { config =>
      ContentSecurityPolicyConfig(
        config.as[FrameAncestors]("frameAncestors"),
        config.as[ScriptSrc]("scriptSrc"),
        config.as[StyleSrc]("styleSrc"),
        config.as[ConnectSrc]("connectSrc"),
        config.as[ObjectSrc]("objectSrc"),
        config.as[ReportUri]("reportUri")
      )
    }

  implicit private val refererConfigReader: ValueReader[RefererConfig] =
    ValueReader.relative { config =>
      RefererConfig(
        config.as[Set[String]]("validHosts"),
        config.as[Boolean]("enabled"),
        config.as[Boolean]("originStrict")
      )
    }

  implicit private val leoPubsubConfigReader: ValueReader[PubsubConfig] = ValueReader.relative { config =>
    PubsubConfig(
      GoogleProject(config.getString("pubsubGoogleProject")),
      config.getString("topicName"),
      config.getInt("queueSize")
    )
  }

  implicit private val autoFreezeConfigReader: ValueReader[AutoFreezeConfig] = ValueReader.relative { config =>
    AutoFreezeConfig(
      config.getBoolean("enableAutoFreeze"),
      toScalaDuration(config.getDuration("autoFreezeAfter")),
      toScalaDuration(config.getDuration("autoFreezeCheckScheduler")),
      toScalaDuration(config.getDuration("maxKernelBusyLimit"))
    )
  }

  implicit private val autodeleteConfigReader: ValueReader[AutoDeleteConfig] = ValueReader.relative { config =>
    AutoDeleteConfig(
      toScalaDuration(config.getDuration("autodeleteCheckInterval"))
    )
  }

  implicit private val pollMonitorConfigReader: ValueReader[PollMonitorConfig] = ValueReader.relative { config =>
    PollMonitorConfig(
      config.as[FiniteDuration]("initial-delay"),
      config.as[Int]("max-attempts"),
      config.as[FiniteDuration]("interval")
    )
  }

  implicit private val interruptablePollMonitorConfigReader: ValueReader[InterruptablePollMonitorConfig] =
    ValueReader.relative { config =>
      InterruptablePollMonitorConfig(
        config.as[Int]("max-attempts"),
        config.as[FiniteDuration]("interval"),
        config.as[FiniteDuration]("interruptAfter")
      )
    }

  implicit private val createDiskTimeoutConfigReader: ValueReader[CreateDiskTimeout] = ValueReader.relative { c =>
    CreateDiskTimeout(
      c.getInt("checkToolsInterruptAfter"),
      c.getInt("timeoutWithSourceDiskCopyInMinutes")
    )
  }

  implicit private val persistentDiskMonitorConfigReader: ValueReader[PersistentDiskMonitorConfig] =
    ValueReader.relative { config =>
      PersistentDiskMonitorConfig(
        config.as[CreateDiskTimeout]("create"),
        config.as[PollMonitorConfig]("delete"),
        config.as[PollMonitorConfig]("update")
      )
    }

  implicit private val clusterToolConfigValueReader: ValueReader[ClusterToolConfig] = ValueReader.relative { config =>
    ClusterToolConfig(
      toScalaDuration(config.getDuration("pollPeriod"))
    )
  }

  implicit private val leoExecutionModeConfigValueReader: ValueReader[LeoExecutionModeConfig] = stringValueReader.map {
    s =>
      s match {
        case "combined" => LeoExecutionModeConfig.Combined
        case "backLeo"  => LeoExecutionModeConfig.BackLeoOnly
        case "frontLeo" => LeoExecutionModeConfig.FrontLeoOnly
        case x          => throw new RuntimeException(s"invalid configuration for leonardoExecutionMode: '$x'")
      }
  }

  implicit private val clusterBucketConfigValueReader: ValueReader[RuntimeBucketConfig] = ValueReader.relative {
    config =>
      RuntimeBucketConfig(
        toScalaDuration(config.getDuration("stagingBucketExpiration"))
      )
  }
  implicit private val clusterUIConfigValueReader: ValueReader[ClusterUIConfig] = ValueReader.relative { config =>
    ClusterUIConfig(
      config.getString("terraLabel"),
      config.getString("allOfUsLabel")
    )
  }
  implicit private val samAuthConfigConfigValueReader: ValueReader[SamAuthProviderConfig] = ValueReader.relative {
    config =>
      SamAuthProviderConfig(
        config.getOrElse("notebookAuthCacheEnabled", true),
        config.getAs[Int]("notebookAuthCacheMaxSize").getOrElse(1000),
        config.getAs[FiniteDuration]("notebookAuthCacheExpiryTime").getOrElse(15 minutes),
        config
          .getOrElse[GroupName]("customAppCreationAllowedGroup",
                                throw new Exception("No customAppCreationAllowedGroup key found")
          ),
        config
          .getOrElse[GroupName]("sasAppCreationAllowedGroup",
                                throw new Exception("No sasAppCreationAllowedGroup key found")
          )
      )
  }

  implicit private val serviceAccountProviderConfigValueReader: ValueReader[ServiceAccountProviderConfig] =
    ValueReader.relative { config =>
      ServiceAccountProviderConfig(
        config.as[Path]("leoServiceAccountJsonFile"),
        config.as[WorkbenchEmail]("leoServiceAccountEmail")
      )
    }

  implicit private val httpSamDao2ConfigValueReader: ValueReader[HttpSamDaoConfig] = ValueReader.relative { config =>
    HttpSamDaoConfig(
      Uri.unsafeFromString(config.as[String]("samServer")),
      config.getOrElse("petKeyCacheEnabled", true),
      config.getAs[FiniteDuration]("petKeyCacheExpiryTime").getOrElse(60 minutes),
      config.getAs[Int]("petKeyCacheMaxSize").getOrElse(1000)
    )
  }

  implicit private val dateAccessUpdaterConfigReader: ValueReader[DateAccessedUpdaterConfig] = ValueReader.relative {
    config =>
      DateAccessedUpdaterConfig(
        config.as[FiniteDuration]("interval"),
        config.as[Int]("maxUpdate"),
        config.as[Int]("queueSize")
      )
  }

  implicit private val workbenchEmailValueReader: ValueReader[WorkbenchEmail] = stringValueReader.map(WorkbenchEmail)
  implicit private val googleProjectValueReader: ValueReader[GoogleProject] = stringValueReader.map(GoogleProject)
  implicit private val pathValueReader: ValueReader[Path] = stringValueReader.map(s => Paths.get(s))
  implicit private val regionNameReader: ValueReader[RegionName] = stringValueReader.map(RegionName(_))
  implicit private val zoneNameReader: ValueReader[ZoneName] = stringValueReader.map(ZoneName(_))
  implicit private val machineTypeReader: ValueReader[MachineTypeName] = stringValueReader.map(MachineTypeName)
  implicit private val dataprocCustomImageReader: ValueReader[DataprocCustomImage] =
    stringValueReader.map(DataprocCustomImage)
  implicit private val gceCustomImageReader: ValueReader[GceCustomImage] = stringValueReader.map(GceCustomImage)
  implicit private val containerImageValueReader: ValueReader[ContainerImage] = stringValueReader.map(s =>
    ContainerImage.fromImageUrl(s).getOrElse(throw new RuntimeException(s"Unable to parse ContainerImage from $s"))
  )
  implicit private val runtimeResourceValueReader: ValueReader[RuntimeResource] = stringValueReader.map(RuntimeResource)
  implicit private val memorySizeReader: ValueReader[MemorySize] = (config: TypeSafeConfig, path: String) =>
    MemorySize(config.getBytes(path))
  implicit private val networkNameValueReader: ValueReader[NetworkName] = stringValueReader.map(NetworkName)
  implicit private val subnetworkNameValueReader: ValueReader[SubnetworkName] = stringValueReader.map(SubnetworkName)
  implicit private val firewallAllowHttpsLabelKeyReader: ValueReader[FirewallAllowHttpsLabelKey] =
    stringValueReader.map(FirewallAllowHttpsLabelKey)
  implicit private val firewallAllowInternalLabelKeyReader: ValueReader[FirewallAllowInternalLabelKey] =
    stringValueReader.map(FirewallAllowInternalLabelKey)
  implicit private val ipRangeValueReader: ValueReader[IpRange] = stringValueReader.map(IpRange)
  implicit private val subnetworkRegionIpRangeMapReader: ValueReader[Map[RegionName, IpRange]] =
    mapValueReader[IpRange].map(mp => mp.map { case (k, v) => RegionName(k) -> v })

  implicit private val vpcConfigReader: ValueReader[VPCConfig] = ValueReader.relative { config =>
    VPCConfig(
      config.as[NetworkLabel]("highSecurityProjectNetworkLabel"),
      config.as[SubnetworkLabel]("highSecurityProjectSubnetworkLabel"),
      config.as[FirewallAllowHttpsLabelKey]("firewallAllowHttpsLabelKey"),
      config.as[FirewallAllowInternalLabelKey]("firewallAllowInternalLabelKey"),
      config.as[NetworkName]("networkName"),
      config.as[NetworkTag]("networkTag"),
      config.as[NetworkTag]("privateAccessNetworkTag"),
      config.as[Boolean]("autoCreateSubnetworks"),
      config.as[SubnetworkName]("subnetworkName"),
      config.as[Map[RegionName, IpRange]]("subnetworkRegionIpRangeMap"),
      config.as[List[FirewallRuleConfig]]("firewallsToAdd"),
      config.as[List[FirewallRuleName]]("firewallsToRemove"),
      config.as[FiniteDuration]("pollPeriod"),
      config.as[Int]("maxAttempts")
    )
  }

  implicit private val sourceRangesReader: ValueReader[Map[RegionName, List[IpRange]]] =
    mapValueReader[List[IpRange]].map(mp => mp.map { case (k, v) => RegionName(k) -> v })

  implicit private val firewallRuleConfigReader: ValueReader[FirewallRuleConfig] = ValueReader.relative { config =>
    FirewallRuleConfig(
      config.as[String]("name-prefix"),
      config.as[Option[String]]("rbs-name"),
      config.as[Map[RegionName, List[IpRange]]]("sourceRanges"),
      config.as[List[Allowed]]("allowed")
    )
  }

  implicit private val networkTagValueReader: ValueReader[NetworkTag] = stringValueReader.map(NetworkTag)
  implicit private val containerRegistryUsernameValueReader: ValueReader[ContainerRegistryUsername] =
    stringValueReader.map(ContainerRegistryUsername)
  implicit private val containerRegistryPasswordValueReader: ValueReader[ContainerRegistryPassword] =
    stringValueReader.map(ContainerRegistryPassword)
  implicit private val firewallRuleNameValueReader: ValueReader[FirewallRuleName] =
    stringValueReader.map(FirewallRuleName)
  implicit private val networkLabelValueReader: ValueReader[NetworkLabel] = stringValueReader.map(NetworkLabel)
  implicit private val subnetworkLabelValueReader: ValueReader[SubnetworkLabel] = stringValueReader.map(SubnetworkLabel)
  implicit private val diskSizeValueReader: ValueReader[DiskSize] = intValueReader.map(DiskSize)
  implicit private val diskTypeValueReader: ValueReader[DiskType] = stringValueReader.map(s =>
    DiskType.stringToObject.getOrElse(s, throw new RuntimeException(s"Unable to parse diskType from $s"))
  )
  implicit private val blockSizeValueReader: ValueReader[BlockSize] = intValueReader.map(BlockSize)
  implicit private val frameAncestorsReader: ValueReader[FrameAncestors] =
    traversableReader[List, String].map(FrameAncestors)
  implicit private val scriptSrcReader: ValueReader[ScriptSrc] = traversableReader[List, String].map(ScriptSrc)
  implicit private val styleSrcReader: ValueReader[StyleSrc] = traversableReader[List, String].map(StyleSrc)
  implicit private val connectSrcReader: ValueReader[ConnectSrc] = traversableReader[List, String].map(ConnectSrc)
  implicit private val objectSrcReader: ValueReader[ObjectSrc] = traversableReader[List, String].map(ObjectSrc)
  implicit private val reportUriReader: ValueReader[ReportUri] = traversableReader[List, String].map(ReportUri)
  implicit private val asyncTaskProcessorConfigReader: ValueReader[AsyncTaskProcessor.Config] = ValueReader.relative {
    c =>
      AsyncTaskProcessor.Config(
        c.getInt("queue-bound"),
        c.getInt("max-concurrent-tasks")
      )
  }

  implicit private val customApplicationAllowListConfigReader: ValueReader[CustomApplicationAllowListConfig] =
    ValueReader.relative { config =>
      CustomApplicationAllowListConfig(
        config.getStringList("default").asScala.toList,
        config.getStringList("highSecurity").asScala.toList
      )
    }

  val dateAccessUpdaterConfig = config.as[DateAccessedUpdaterConfig]("dateAccessedUpdater")
  val applicationConfig = config.as[ApplicationConfig]("application")
  val googleGroupsConfig = config.as[GoogleGroupsConfig]("groups")

  val dataprocConfig = config.as[DataprocConfig]("dataproc")
  val gceConfig = config.as[GceConfig]("gce")
  val imageConfig = config.as[ImageConfig]("image")
  val prometheusConfig = config.as[PrometheusConfig]("prometheus")
  val proxyConfig = config.as[ProxyConfig]("proxy")
  val securityFilesConfig = config.as[SecurityFilesConfig]("clusterFiles")
  val gceClusterResourcesConfig = config.as[ClusterResourcesConfig]("gceClusterResources")
  val clusterResourcesConfig = config.as[ClusterResourcesConfig]("clusterResources")
  val samConfig = config.as[SamConfig]("sam")
  val autoFreezeConfig = config.as[AutoFreezeConfig]("autoFreeze")
  val autodeleteConfig = config.as[AutoDeleteConfig]("autodelete")
  val serviceAccountProviderConfig = config.as[ServiceAccountProviderConfig]("serviceAccounts.providerConfig")
  val kubeServiceAccountProviderConfig = config.as[ServiceAccountProviderConfig]("serviceAccounts.kubeConfig")
  val contentSecurityPolicy = config.as[ContentSecurityPolicyConfig]("contentSecurityPolicy")
  val refererConfig = config.as[RefererConfig]("refererConfig")
  val vpcConfig = config.as[VPCConfig]("vpc")

  implicit private val zombieClusterConfigValueReader: ValueReader[ZombieRuntimeMonitorConfig] = ValueReader.relative {
    config =>
      ZombieRuntimeMonitorConfig(
        config.getBoolean("enableZombieRuntimeMonitor"),
        toScalaDuration(config.getDuration("pollPeriod")),
        config.getString("deletionConfirmationLabelKey"),
        toScalaDuration(config.getDuration("creationHangTolerance")),
        config.getInt("concurrency")
      )
  }

  val clusterToolMonitorConfig = config.as[ClusterToolConfig](path = "clusterToolMonitor")
  val runtimeDnsCacheConfig = config.as[CacheConfig]("runtimeDnsCache")
  val kubernetesDnsCacheConfig = config.as[CacheConfig]("kubernetesDnsCache")
  val leoExecutionModeConfig = config.as[LeoExecutionModeConfig]("leonardoExecutionMode")
  val clusterBucketConfig = config.as[RuntimeBucketConfig]("clusterBucket")

  implicit private val gceMonitorConfigReader: ValueReader[GceMonitorConfig] = ValueReader.relative { config =>
    val statusTimeouts = config.getConfig("statusTimeouts")
    val timeoutMap: Map[RuntimeStatus, FiniteDuration] = statusTimeouts.entrySet.asScala.flatMap { e =>
      for {
        status <- RuntimeStatus.withNameInsensitiveOption(e.getKey)
        duration <- statusTimeouts.getAs[FiniteDuration](e.getKey)
      } yield (status, duration)
    }.toMap

    GceMonitorConfig(
      config.as[FiniteDuration]("initialDelay"),
      config.as[PollMonitorConfig]("pollStatus"),
      timeoutMap,
      config.as[InterruptablePollMonitorConfig]("checkTools"),
      clusterBucketConfig,
      imageConfig
    )
  }

  implicit private val dataprocMonitorConfigReader: ValueReader[DataprocMonitorConfig] = ValueReader.relative {
    config =>
      val statusTimeouts = config.getConfig("statusTimeouts")
      val timeoutMap: Map[RuntimeStatus, FiniteDuration] = statusTimeouts.entrySet.asScala.flatMap { e =>
        for {
          status <- RuntimeStatus.withNameInsensitiveOption(e.getKey)
          duration <- statusTimeouts.getAs[FiniteDuration](e.getKey)
        } yield (status, duration)
      }.toMap

      DataprocMonitorConfig(
        config.as[FiniteDuration]("initialDelay"),
        config.as[PollMonitorConfig]("pollStatus"),
        timeoutMap,
        config.as[InterruptablePollMonitorConfig]("checkTools"),
        clusterBucketConfig,
        imageConfig,
        vpcConfig
      )
  }

  val gceMonitorConfig = config.as[GceMonitorConfig]("gce.monitor")
  val dataprocMonitorConfig = config.as[DataprocMonitorConfig]("dataproc.monitor")
  val uiConfig = config.as[ClusterUIConfig]("ui")
  val samAuthConfig = config.as[SamAuthProviderConfig]("auth.providerConfig")
  val httpSamDaoConfig = config.as[HttpSamDaoConfig]("auth.providerConfig")
  val liquibaseConfig = config.as[LiquibaseConfig]("liquibase")
  val welderConfig = config.as[WelderConfig]("welder")
  val dbConcurrency = config.as[Long]("mysql.concurrency")

  implicit private val cidrIPReader: ValueReader[CidrIP] = stringValueReader.map(CidrIP)

  implicit private val kubeClusterConfigReader: ValueReader[KubernetesClusterConfig] = ValueReader.relative { config =>
    KubernetesClusterConfig(
      config.as[Location]("location"),
      config.as[RegionName]("region"),
      config.as[List[CidrIP]]("authorizedNetworks"),
      config.as[KubernetesClusterVersion]("version"),
      config.as[FiniteDuration]("nodepoolLockCacheExpiryTime"),
      config.getInt("nodepoolLockCacheMaxSize")
    )
  }

  implicit private val maxNodepoolsPerDefaultNodeReader: ValueReader[MaxNodepoolsPerDefaultNode] =
    intValueReader.map(MaxNodepoolsPerDefaultNode)

  implicit private val defaultNodepoolConfigReader: ValueReader[DefaultNodepoolConfig] = ValueReader.relative {
    config =>
      DefaultNodepoolConfig(
        config.as[MachineTypeName]("machineType"),
        config.as[NumNodes]("numNodes"),
        config.as[Boolean]("autoscalingEnabled"),
        config.as[MaxNodepoolsPerDefaultNode]("maxNodepoolsPerDefaultNode")
      )
  }

  implicit private val galaxyNodepoolConfigReader: ValueReader[GalaxyNodepoolConfig] = ValueReader.relative { config =>
    GalaxyNodepoolConfig(
      config.as[MachineTypeName]("machineType"),
      config.as[NumNodes]("numNodes"),
      config.as[Boolean]("autoscalingEnabled"),
      config.as[AutoscalingConfig]("autoscalingConfig")
    )
  }

  implicit private val autoscalingConfigReader: ValueReader[AutoscalingConfig] = ValueReader.relative { config =>
    AutoscalingConfig(
      config.as[AutoscalingMin]("autoscalingMin"),
      config.as[AutoscalingMax]("autoscalingMax")
    )
  }

  implicit private val ingressConfigReader: ValueReader[KubernetesIngressConfig] = ValueReader.relative { config =>
    KubernetesIngressConfig(
      config.as[NamespaceName]("namespace"),
      config.as[Release]("release"),
      config.as[ChartName]("chartName"),
      config.as[ChartVersion]("chartVersion"),
      config.as[ServiceName]("loadBalancerService"),
      config.as[List[ValueConfig]]("values")
    )
  }

  implicit private val namespaceNameSuffixReader: ValueReader[NamespaceNameSuffix] =
    stringValueReader.map(NamespaceNameSuffix)
  implicit private val releaseNameSuffixReader: ValueReader[ReleaseNameSuffix] =
    stringValueReader.map(ReleaseNameSuffix)
  implicit private val dbPasswordReader: ValueReader[DbPassword] = stringValueReader.map(DbPassword)
  implicit private val galaxyOrchUrlReader: ValueReader[GalaxyOrchUrl] = stringValueReader.map(GalaxyOrchUrl)
  implicit private val galaxyDrsUrlReader: ValueReader[GalaxyDrsUrl] = stringValueReader.map(GalaxyDrsUrl)

  implicit private val galaxyAppConfigReader: ValueReader[GalaxyAppConfig] = ValueReader.relative { config =>
    GalaxyAppConfig(
      config.as[ReleaseNameSuffix]("releaseNameSuffix"),
      config.as[ChartName]("chartName"),
      config.as[ChartVersion]("chartVersion"),
      config.as[NamespaceNameSuffix]("namespaceNameSuffix"),
      config.as[List[ServiceConfig]]("services"),
      config.as[ServiceAccountName]("serviceAccountName"),
      config.as[Boolean]("uninstallKeepHistory"),
      config.as[DbPassword]("postgres.password"),
      config.as[GalaxyOrchUrl]("orchUrl"),
      config.as[GalaxyDrsUrl]("drsUrl"),
      config.as[Int]("minMemoryGb"),
      config.as[Int]("minNumOfCpus"),
      config.as[Boolean]("enabled"),
      config.as[List[ChartVersion]]("chartVersionsToExcludeFromUpdates")
    )
  }

  implicit private val appDiskConfigReader: ValueReader[GalaxyDiskConfig] = ValueReader.relative { config =>
    GalaxyDiskConfig(
      config.as[String]("nfsPersistenceName"),
      config.as[DiskSize]("nfsMinimumDiskSizeGB"),
      config.as[String]("postgresPersistenceName"),
      config.as[String]("postgresDiskNameSuffix"),
      config.as[DiskSize]("postgresDiskSizeGB"),
      config.as[BlockSize]("postgresDiskBlockSize")
    )
  }

  implicit private val cromwellAppConfigReader: ValueReader[CromwellAppConfig] = ValueReader.relative { config =>
    CromwellAppConfig(
      chartName = config.as[ChartName]("chartName"),
      chartVersion = config.as[ChartVersion]("chartVersion"),
      namespaceNameSuffix = config.as[NamespaceNameSuffix]("namespaceNameSuffix"),
      releaseNameSuffix = config.as[ReleaseNameSuffix]("releaseNameSuffix"),
      services = config.as[List[ServiceConfig]]("services"),
      serviceAccountName = config.as[ServiceAccountName]("serviceAccountName"),
      dbPassword = config.as[DbPassword]("dbPassword"),
      enabled = config.as[Boolean]("enabled"),
      chartVersionsToExcludeFromUpdates = config.as[List[ChartVersion]]("chartVersionsToExcludeFromUpdates")
    )
  }

  implicit private val customAppConfigReader: ValueReader[CustomAppConfig] = ValueReader.relative { config =>
    CustomAppConfig(
      config.as[ChartName]("chartName"),
      config.as[ChartVersion]("chartVersion"),
      config.as[ReleaseNameSuffix]("releaseNameSuffix"),
      config.as[NamespaceNameSuffix]("namespaceNameSuffix"),
      config.as[ServiceAccountName]("serviceAccountName"),
      config.as[CustomApplicationAllowListConfig]("customApplicationAllowList"),
      config.as[Boolean]("enabled"),
      config.as[List[ChartVersion]]("chartVersionsToExcludeFromUpdates")
    )
  }

  implicit private val containerRegistryConfigReader: ValueReader[ContainerRegistryCredentials] = ValueReader.relative {
    config =>
      ContainerRegistryCredentials(
        config.as[ContainerRegistryUsername]("sasRegistryUsername"),
        config.as[ContainerRegistryPassword]("sasRegistryPassword")
      )
  }

  implicit private val aouAppConfigReader: ValueReader[AllowedAppConfig] = ValueReader.relative { config =>
    AllowedAppConfig(
      config.as[ChartName]("chartName"),
      config.as[ChartVersion]("rstudioChartVersion"),
      config.as[ChartVersion]("sasChartVersion"),
      config.as[NamespaceNameSuffix]("namespaceNameSuffix"),
      config.as[ReleaseNameSuffix]("releaseNameSuffix"),
      config.as[List[ServiceConfig]]("services"),
      config.as[ServiceAccountName]("serviceAccountName"),
      config.as[ContainerRegistryCredentials]("sasContainerRegistry"),
      config.as[List[ChartVersion]]("chartVersionsToExcludeFromUpdates"),
      config.as[Int]("numOfReplicas")
    )
  }

  implicit private val releaseNameReader: ValueReader[Release] = stringValueReader.map(Release)
  implicit private val namespaceNameReader: ValueReader[NamespaceName] = stringValueReader.map(NamespaceName)
  implicit private val chartNameReader: ValueReader[ChartName] = stringValueReader.map(ChartName)
  implicit private val chartVersionReader: ValueReader[ChartVersion] = stringValueReader.map(ChartVersion)
  implicit private val kubernetesServiceAccountReader: ValueReader[ServiceAccountName] =
    stringValueReader.map(ServiceAccountName)
  implicit private val valueConfigReader: ValueReader[ValueConfig] = stringValueReader.map(ValueConfig)

  implicit private val serviceReader: ValueReader[ServiceConfig] = ValueReader.relative { config =>
    ServiceConfig(
      config.as[ServiceName]("name"),
      config.as[KubernetesServiceKindName]("kind"),
      config.as[Option[ServicePath]]("path")
    )
  }

  implicit private val locationValueReader: ValueReader[Location] = stringValueReader.map(Location)
  implicit private val numNodesValueReader: ValueReader[NumNodes] = intValueReader.map(NumNodes)
  implicit private val autoscalingMinValueReader: ValueReader[AutoscalingMin] = intValueReader.map(AutoscalingMin)
  implicit private val autoscalingMaxValueReader: ValueReader[AutoscalingMax] = intValueReader.map(AutoscalingMax)
  implicit private val serviceNameValueReader: ValueReader[ServiceName] = stringValueReader.map(s =>
    KubernetesName
      .withValidation(s, ServiceName)
      .getOrElse(throw new Exception(s"Invalid service name in config: ${s}"))
  )
  implicit private val serviceKindValueReader: ValueReader[KubernetesServiceKindName] =
    stringValueReader.map(KubernetesServiceKindName.apply)
  implicit private val kubernetesClusterVersionReader: ValueReader[KubernetesClusterVersion] =
    stringValueReader.map(KubernetesClusterVersion)
  implicit private val servicePathReader: ValueReader[ServicePath] =
    stringValueReader.map(ServicePath)

  val gkeClusterConfig = config.as[KubernetesClusterConfig]("gke.cluster")
  val gkeDefaultNodepoolConfig = config.as[DefaultNodepoolConfig]("gke.defaultNodepool")
  val gkeGalaxyNodepoolConfig = config.as[GalaxyNodepoolConfig]("gke.galaxyNodepool")
  val gkeIngressConfig = config.as[KubernetesIngressConfig]("gke.ingress")
  val gkeGalaxyAppConfig = config.as[GalaxyAppConfig]("gke.galaxyApp")
  val gkeCromwellAppConfig = config.as[CromwellAppConfig]("gke.cromwellApp")
  val gkeCustomAppConfig = config.as[CustomAppConfig]("gke.customApp")
  val gkeAllowedAppConfig = config.as[AllowedAppConfig]("gke.allowedApp")
  val gkeNodepoolConfig = NodepoolConfig(gkeDefaultNodepoolConfig, gkeGalaxyNodepoolConfig)
  val gkeGalaxyDiskConfig = config.as[GalaxyDiskConfig]("gke.galaxyDisk")

  implicit private val leoPubsubMessageSubscriberConfigReader: ValueReader[LeoPubsubMessageSubscriberConfig] =
    ValueReader.relative { config =>
      LeoPubsubMessageSubscriberConfig(
        config.getInt("concurrency"),
        config.as[FiniteDuration]("timeout"),
        config.as[PersistentDiskMonitorConfig]("persistent-disk-monitor"),
        gkeGalaxyDiskConfig
      )
    }

  val leoKubernetesConfig = LeoKubernetesConfig(
    kubeServiceAccountProviderConfig,
    gkeClusterConfig,
    gkeNodepoolConfig,
    gkeIngressConfig,
    gkeGalaxyAppConfig,
    gkeGalaxyDiskConfig,
    ConfigReader.appConfig.persistentDisk,
    gkeCromwellAppConfig,
    gkeCustomAppConfig,
    gkeAllowedAppConfig
  )

  val appServiceConfig = AppServiceConfig(
    config.getBoolean("app-service.enable-custom-app-check"),
    config.getBoolean("app-service.enable-sas-app"),
    leoKubernetesConfig
  )
  val pubsubConfig = config.as[PubsubConfig]("pubsub")
  val topic = ProjectTopicName.of(pubsubConfig.pubsubGoogleProject.value, pubsubConfig.topicName)

  val subscriberConfig: SubscriberConfig = SubscriberConfig(
    applicationConfig.leoServiceAccountJsonFile.toString,
    topic,
    None,
    config.as[FiniteDuration]("pubsub.ackDeadLine"),
    None,
    None,
    Some("attributes:leonardo")
  )

  val nonLeoMessageSubscriberConfig: SubscriberConfig = SubscriberConfig(
    applicationConfig.leoServiceAccountJsonFile.toString,
    topic,
    Some(
      ProjectSubscriptionName.of(applicationConfig.leoGoogleProject.value,
                                 config.as[String]("pubsub.non-leo-message-subscriber.subscription-name")
      )
    ),
    config.as[FiniteDuration]("pubsub.ackDeadLine"),
    Some(
      SubscriberDeadLetterPolicy(
        TopicName.of(applicationConfig.leoGoogleProject.value,
                     config.as[String]("pubsub.non-leo-message-subscriber.dead-letter-topic")
        ),
        MaxRetries(5)
      )
    ),
    None,
    Some("NOT attributes:leonardo")
  )

  private val nonLeoMessageSubscriberCryptominingTopic =
    config.as[String]("pubsub.non-leo-message-subscriber.terra-cryptomining-topic")

  val cryptominingTopicPublisherConfig: PublisherConfig =
    PublisherConfig(
      applicationConfig.leoServiceAccountJsonFile.toString,
      ProjectTopicName.of(pubsubConfig.pubsubGoogleProject.value, nonLeoMessageSubscriberCryptominingTopic)
    )

  val publisherConfig: PublisherConfig =
    PublisherConfig(applicationConfig.leoServiceAccountJsonFile.toString, topic)

  val dataprocInterpreterConfig = DataprocInterpreterConfig(
    dataprocConfig,
    googleGroupsConfig,
    welderConfig,
    imageConfig,
    proxyConfig,
    vpcConfig,
    clusterResourcesConfig,
    securityFilesConfig,
    dataprocMonitorConfig.monitorStatusTimeouts
      .getOrElse(RuntimeStatus.Creating, throw new Exception("Missing dataproc.monitor.statusTimeouts.creating"))
  )

  val gceInterpreterConfig = GceInterpreterConfig(
    gceConfig,
    welderConfig,
    imageConfig,
    proxyConfig,
    vpcConfig,
    gceClusterResourcesConfig,
    securityFilesConfig,
    gceMonitorConfig.monitorStatusTimeouts.getOrElse(RuntimeStatus.Creating,
                                                     throw new Exception("Missing gce.monitor.statusTimeouts.creating")
    )
  )
  val vpcInterpreterConfig = VPCInterpreterConfig(vpcConfig)

  val leoPubsubMessageSubscriberConfig = config.as[LeoPubsubMessageSubscriberConfig]("pubsub.subscriber")
  val asyncTaskProcessorConfig = config.as[AsyncTaskProcessor.Config]("async-task-processor")

  implicit private val appMonitorConfigReader: ValueReader[AppMonitorConfig] = ValueReader.relative { config =>
    AppMonitorConfig(
      config.as[PollMonitorConfig]("createNodepool"),
      config.as[PollMonitorConfig]("deleteNodepool"),
      config.as[PollMonitorConfig]("createCluster"),
      config.as[PollMonitorConfig]("deleteCluster"),
      config.as[PollMonitorConfig]("createIngress"),
      config.as[InterruptablePollMonitorConfig]("createApp"),
      config.as[PollMonitorConfig]("deleteApp"),
      config.as[PollMonitorConfig]("scalingUpNodepool"),
      config.as[PollMonitorConfig]("scalingDownNodepool"),
      config.as[InterruptablePollMonitorConfig]("startApp"),
      config.as[InterruptablePollMonitorConfig]("updateApp"),
      config.as[PollMonitorConfig]("appLivenessCheck")
    )
  }

  val appMonitorConfig = config.as[AppMonitorConfig]("pubsub.kubernetes-monitor")

  val gkeInterpConfig =
    GKEInterpreterConfig(
      applicationConfig.leoUrlBase,
      vpcConfig.networkTag,
      org.broadinstitute.dsde.workbench.leonardo.http.ConfigReader.appConfig.terraAppSetupChart,
      gkeIngressConfig,
      gkeGalaxyAppConfig,
      gkeCromwellAppConfig,
      gkeCustomAppConfig,
      gkeAllowedAppConfig,
      appMonitorConfig,
      gkeClusterConfig,
      proxyConfig,
      gkeGalaxyDiskConfig
    )
}
