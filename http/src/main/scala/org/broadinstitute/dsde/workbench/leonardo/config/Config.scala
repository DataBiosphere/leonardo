package org.broadinstitute.dsde.workbench.leonardo
package config

import java.io.File
import java.nio.file.{Path, Paths}

import com.google.pubsub.v1.ProjectTopicName
import com.typesafe.config.{ConfigFactory, Config => TypeSafeConfig}
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ValueReader
import org.broadinstitute.dsde.workbench.google2.{
  GoogleTopicAdminInterpreter,
  MachineTypeName,
  PublisherConfig,
  RegionName,
  SubscriberConfig,
  ZoneName
}
import org.broadinstitute.dsde.workbench.leonardo.CustomImage.{DataprocCustomImage, GceCustomImage}
import org.broadinstitute.dsde.workbench.leonardo.auth.sam.SamAuthProviderConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.HttpSamDaoConfig
import org.broadinstitute.dsde.workbench.leonardo.model.ServiceAccountProviderConfig
import org.broadinstitute.dsde.workbench.leonardo.util.RuntimeInterpreterConfig.{
  DataprocInterpreterConfig,
  GceInterpreterConfig
}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.util.toScalaDuration
import org.http4s.Uri

import scala.collection.JavaConverters._
import scala.concurrent.duration._

object Config {
  val config = ConfigFactory.parseResources("leonardo.conf").withFallback(ConfigFactory.load()).resolve()

  implicit val applicationConfigReader: ValueReader[ApplicationConfig] = ValueReader.relative { config =>
    ApplicationConfig(
      config.getString("applicationName"),
      config.as[GoogleProject]("leoGoogleProject"),
      config.as[Path]("leoServiceAccountJsonFile"),
      config.as[WorkbenchEmail]("leoServiceAccountEmail")
    )
  }

  implicit val dataprocRuntimeConfigReader: ValueReader[RuntimeConfig.DataprocConfig] = ValueReader.relative { config =>
    RuntimeConfig.DataprocConfig(
      config.getInt("numberOfWorkers"),
      config.as[MachineTypeName]("masterMachineType"),
      config.getInt("masterDiskSize"),
      config.getAs[String]("workerMachineType").map(MachineTypeName),
      config.getAs[Int]("workerDiskSize"),
      config.getAs[Int]("numberOfWorkerLocalSSDs"),
      config.getAs[Int]("numberOfPreemptibleWorkers"),
      Map.empty
    )
  }

  implicit val gceRuntimeConfigReader: ValueReader[RuntimeConfig.GceConfig] = ValueReader.relative { config =>
    RuntimeConfig.GceConfig(
      config.as[MachineTypeName]("machineType"),
      config.getInt("diskSize")
    )
  }

  implicit val dataprocConfigReader: ValueReader[DataprocConfig] = ValueReader.relative { config =>
    DataprocConfig(
      config.as[RegionName]("region"),
      config.getAs[ZoneName]("zone"),
      config.getStringList("defaultScopes").asScala.toSet,
      config.as[DataprocCustomImage]("legacyCustomDataprocImage"),
      config.as[DataprocCustomImage]("customDataprocImage"),
      config.getAs[MemorySize]("dataprocReservedMemory"),
      config.as[RuntimeConfig.DataprocConfig]("runtimeDefaults")
    )
  }

  implicit val gceConfigReader: ValueReader[GceConfig] = ValueReader.relative { config =>
    GceConfig(
      config.as[GceCustomImage]("customGceImage"),
      config.as[RegionName]("region"),
      config.as[ZoneName]("zone"),
      config.getStringList("defaultScopes").asScala.toSet,
      config.getAs[MemorySize]("gceReservedMemory"),
      config.as[RuntimeConfig.GceConfig]("runtimeDefaults")
    )
  }

  implicit val fireallRuleConfigReader: ValueReader[FirewallRuleConfig] = ValueReader.relative { config =>
    FirewallRuleConfig(
      config.as[FirewallRuleName]("name"),
      config.as[NetworkName]("network"),
      config.as[List[IpRange]]("ipRange"),
      config.as[String]("protocol"),
      config.as[Int]("port")
    )
  }

  implicit val vpcConfigReader: ValueReader[VPCConfig] = ValueReader.relative { config =>
    VPCConfig(
      config.getString("highSecurityProjectNetworkLabel"),
      config.getString("highSecurityProjectSubnetworkLabel"),
      config.as[NetworkName]("networkName"),
      config.as[SubnetworkName]("subnetworkName"),
      config.as[RegionName]("subnetworkRegion"),
      config.as[IpRange]("subnetworkIpRange"),
      config.as[NetworkTag]("networkTag"),
      config.as[FirewallRuleConfig]("httpsFirewallRule"),
      config.as[FirewallRuleConfig]("sshFirewallRule"),
      config.as[List[FirewallRuleName]]("firewallsToRemove")
    )
  }

  implicit val googleGroupConfigReader: ValueReader[GoogleGroupsConfig] = ValueReader.relative { config =>
    GoogleGroupsConfig(
      config.as[WorkbenchEmail]("subEmail"),
      config.getString("dataprocImageProjectGroupName"),
      config.as[WorkbenchEmail]("dataprocImageProjectGroupEmail")
    )
  }

  implicit val imageConfigReader: ValueReader[ImageConfig] = ValueReader.relative { config =>
    ImageConfig(
      config.as[ContainerImage]("welderImage"),
      config.as[ContainerImage]("jupyterImage"),
      config.as[ContainerImage]("legacyJupyterImage"),
      config.as[ContainerImage]("proxyImage"),
      config.getString("jupyterContainerName"),
      config.getString("rstudioContainerName"),
      config.getString("welderContainerName"),
      config.getString("proxyContainerName"),
      config.getString("jupyterImageRegex"),
      config.getString("rstudioImageRegex"),
      config.getString("broadDockerhubImageRegex")
    )
  }

  implicit val welderConfigReader: ValueReader[WelderConfig] = ValueReader.relative { config =>
    WelderConfig(
      Paths.get(config.getString("welderEnabledNotebooksDir")),
      Paths.get(config.getString("welderDisabledNotebooksDir")),
      config.getAs[String]("deployWelderLabel"),
      config.getAs[String]("updateWelderLabel"),
      config.getAs[String]("deployWelderCutoffDate"),
      config.getAs[MemorySize]("welderReservedMemory")
    )
  }

  implicit val clusterResourcesConfigReader: ValueReader[ClusterResourcesConfig] = ValueReader.relative { config =>
    ClusterResourcesConfig(
      config.as[RuntimeResource]("initActionsScript"),
      config.as[RuntimeResource]("gceInitScript"),
      config.as[RuntimeResource]("startupScript"),
      config.as[RuntimeResource]("shutdownScript"),
      config.as[RuntimeResource]("jupyterDockerCompose"),
      config.as[RuntimeResource]("jupyterDockerComposeGce"),
      config.as[RuntimeResource]("rstudioDockerCompose"),
      config.as[RuntimeResource]("proxyDockerCompose"),
      config.as[RuntimeResource]("welderDockerCompose"),
      config.as[RuntimeResource]("proxySiteConf"),
      config.as[RuntimeResource]("jupyterNotebookConfigUri"),
      config.as[RuntimeResource]("jupyterNotebookFrontendConfigUri"),
      config.as[RuntimeResource]("customEnvVarsConfigUri")
    )
  }

  implicit val clusterFilesConfigReader: ValueReader[ClusterFilesConfig] = ValueReader.relative { config =>
    val baseDir = config.getString("configFolderPath")
    ClusterFilesConfig(
      new File(baseDir, config.getString("jupyterServerCrt")),
      new File(baseDir, config.getString("jupyterServerKey")),
      new File(baseDir, config.getString("jupyterRootCaPem")),
      new File(baseDir, config.getString("jupyterRootCaKey"))
    )
  }

  implicit val clusterDnsCacheConfigValueReader: ValueReader[ClusterDnsCacheConfig] = ValueReader.relative { config =>
    ClusterDnsCacheConfig(
      toScalaDuration(config.getDuration("cacheExpiryTime")),
      config.getInt("cacheMaxSize")
    )
  }

  implicit val liquibaseReader: ValueReader[LiquibaseConfig] = ValueReader.relative { config =>
    LiquibaseConfig(config.as[String]("changelog"), config.as[Boolean]("initWithLiquibase"))
  }

  implicit val samConfigReader: ValueReader[SamConfig] = ValueReader.relative { config =>
    SamConfig(config.getString("server"))
  }

  implicit val proxyConfigReader: ValueReader[ProxyConfig] = ValueReader.relative { config =>
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

  implicit val swaggerReader: ValueReader[SwaggerConfig] = ValueReader.relative { config =>
    SwaggerConfig(
      config.getString("googleClientId"),
      config.getString("realm")
    )
  }

  implicit val monitorConfigReader: ValueReader[MonitorConfig] = ValueReader.relative { config =>
    val statusTimeouts = config.getConfig("statusTimeouts")
    val timeoutMap: Map[RuntimeStatus, FiniteDuration] = statusTimeouts.entrySet.asScala.flatMap { e =>
      for {
        status <- RuntimeStatus.withNameInsensitiveOption(e.getKey)
        duration <- statusTimeouts.getAs[FiniteDuration](e.getKey)
      } yield (status, duration)
    }.toMap

    MonitorConfig(toScalaDuration(config.getDuration("pollPeriod")),
                  config.getInt("maxRetries"),
                  config.getBoolean("recreateCluster"),
                  timeoutMap)
  }

  implicit val leoPubsubConfigReader: ValueReader[PubsubConfig] = ValueReader.relative { config =>
    PubsubConfig(
      GoogleProject(config.getString("pubsubGoogleProject")),
      config.getString("topicName"),
      config.getInt("queueSize")
    )
  }

  implicit val autoFreezeConfigReader: ValueReader[AutoFreezeConfig] = ValueReader.relative { config =>
    AutoFreezeConfig(
      config.getBoolean("enableAutoFreeze"),
      toScalaDuration(config.getDuration("dateAccessedMonitorScheduler")),
      toScalaDuration(config.getDuration("autoFreezeAfter")),
      toScalaDuration(config.getDuration("autoFreezeCheckScheduler")),
      toScalaDuration(config.getDuration("maxKernelBusyLimit"))
    )
  }

  implicit val clusterToolConfigValueReader: ValueReader[ClusterToolConfig] = ValueReader.relative { config =>
    ClusterToolConfig(
      toScalaDuration(config.getDuration("pollPeriod"))
    )
  }

  implicit val leoExecutionModeConfigValueReader: ValueReader[LeoExecutionModeConfig] = ValueReader.relative { config =>
    LeoExecutionModeConfig(
      config.getBoolean("backLeo")
    )
  }

  implicit val clusterBucketConfigValueReader: ValueReader[ClusterBucketConfig] = ValueReader.relative { config =>
    ClusterBucketConfig(
      toScalaDuration(config.getDuration("stagingBucketExpiration"))
    )
  }
  implicit val clusterUIConfigValueReader: ValueReader[ClusterUIConfig] = ValueReader.relative { config =>
    ClusterUIConfig(
      config.getString("terraLabel"),
      config.getString("allOfUsLabel")
    )
  }
  implicit val samAuthConfigConfigValueReader: ValueReader[SamAuthProviderConfig] = ValueReader.relative { config =>
    SamAuthProviderConfig(
      config.getOrElse("notebookAuthCacheEnabled", true),
      config.getAs[Int]("notebookAuthCacheMaxSize").getOrElse(1000),
      config.getAs[FiniteDuration]("notebookAuthCacheExpiryTime").getOrElse(15 minutes)
    )
  }

  implicit val serviceAccountProviderConfigValueReader: ValueReader[ServiceAccountProviderConfig] =
    ValueReader.relative { config =>
      ServiceAccountProviderConfig(
        config.as[Path]("leoServiceAccountJsonFile"),
        config.as[WorkbenchEmail]("leoServiceAccountEmail")
      )
    }

  implicit val httpSamDao2ConfigValueReader: ValueReader[HttpSamDaoConfig] = ValueReader.relative { config =>
    HttpSamDaoConfig(
      Uri.unsafeFromString(config.as[String]("samServer")),
      config.getOrElse("petTokenCacheEnabled", true),
      config.getAs[FiniteDuration]("petTokenCacheExpiryTime").getOrElse(60 minutes),
      config.getAs[Int]("petTokenCacheMaxSize").getOrElse(1000),
      serviceAccountProviderConfig
    )
  }

  implicit val workbenchEmailValueReader: ValueReader[WorkbenchEmail] = stringValueReader.map(WorkbenchEmail)
  implicit val googleProjectValueReader: ValueReader[GoogleProject] = stringValueReader.map(GoogleProject)
  implicit val fileValueReader: ValueReader[File] = stringValueReader.map(s => new File(s))
  implicit val pathValueReader: ValueReader[Path] = stringValueReader.map(s => Paths.get(s))
  implicit val regionNameReader: ValueReader[RegionName] = stringValueReader.map(RegionName)
  implicit val zoneNameReader: ValueReader[ZoneName] = stringValueReader.map(ZoneName)
  implicit val machineTypeReader: ValueReader[MachineTypeName] = stringValueReader.map(MachineTypeName)
  implicit val dataprocCustomImageReader: ValueReader[DataprocCustomImage] = stringValueReader.map(DataprocCustomImage)
  implicit val gceCustomImageReader: ValueReader[GceCustomImage] = stringValueReader.map(GceCustomImage)
  implicit val containerImageValueReader: ValueReader[ContainerImage] = stringValueReader.map(
    s => ContainerImage.fromString(s).getOrElse(throw new RuntimeException(s"Unable to parse ContainerImage from $s"))
  )
  implicit val runtimeResourceValueReader: ValueReader[RuntimeResource] = stringValueReader.map(RuntimeResource)
  implicit val memorySizeReader: ValueReader[MemorySize] = (config: TypeSafeConfig, path: String) =>
    MemorySize(config.getBytes(path))
  implicit val networkNameValueReader: ValueReader[NetworkName] = stringValueReader.map(NetworkName)
  implicit val subnetworkNameValueReader: ValueReader[SubnetworkName] = stringValueReader.map(SubnetworkName)
  implicit val ipRangeValueReader: ValueReader[IpRange] = stringValueReader.map(IpRange)
  implicit val networkTagValueReader: ValueReader[NetworkTag] = stringValueReader.map(NetworkTag)
  implicit val firewallRuleNameValueReader: ValueReader[FirewallRuleName] = stringValueReader.map(FirewallRuleName)

  val applicationConfig = config.as[ApplicationConfig]("application")
  val googleGroupsConfig = config.as[GoogleGroupsConfig]("groups")

  val dataprocConfig = config.as[DataprocConfig]("dataproc")
  val gceConfig = config.as[GceConfig]("gce")
  val imageConfig = config.as[ImageConfig]("image")
  val proxyConfig = config.as[ProxyConfig]("proxy")
  val swaggerConfig = config.as[SwaggerConfig]("swagger")
  val clusterFilesConfig = config.as[ClusterFilesConfig]("clusterFiles")
  val clusterResourcesConfig = config.as[ClusterResourcesConfig]("clusterResources")
  val monitorConfig = config.as[MonitorConfig]("monitor")
  val samConfig = config.as[SamConfig]("sam")
  val autoFreezeConfig = config.as[AutoFreezeConfig]("autoFreeze")
  val serviceAccountProviderConfig = config.as[ServiceAccountProviderConfig]("serviceAccounts.providerConfig")
  val contentSecurityPolicy =
    config.as[Option[String]]("jupyterConfig.contentSecurityPolicy").getOrElse("default-src: 'self'")

  implicit val zombieClusterConfigValueReader: ValueReader[ZombieRuntimeMonitorConfig] = ValueReader.relative {
    config =>
      ZombieRuntimeMonitorConfig(
        config.getBoolean("enableZombieClusterMonitor"),
        toScalaDuration(config.getDuration("pollPeriod")),
        toScalaDuration(config.getDuration("creationHangTolerance")),
        config.getInt("concurrency"),
        gceConfig.zoneName
      )
  }

  val zombieClusterMonitorConfig = config.as[ZombieRuntimeMonitorConfig]("zombieClusterMonitor")
  val clusterToolMonitorConfig = config.as[ClusterToolConfig](path = "clusterToolMonitor")
  val clusterDnsCacheConfig = config.as[ClusterDnsCacheConfig]("clusterDnsCache")
  val leoExecutionModeConfig = config.as[LeoExecutionModeConfig]("leoExecutionMode")
  val clusterBucketConfig = config.as[ClusterBucketConfig]("clusterBucket")
  val uiConfig = config.as[ClusterUIConfig]("ui")
  val serviceAccountProviderClass = config.as[String]("serviceAccounts.providerClass")
  val samAuthConfig = config.as[SamAuthProviderConfig]("auth.providerConfig")
  val httpSamDap2Config = config.as[HttpSamDaoConfig]("auth.providerConfig")
  val liquibaseConfig = config.as[LiquibaseConfig]("liquibase")
  val welderConfig = config.as[WelderConfig]("welder")
  val dbConcurrency = config.as[Long]("mysql.concurrency")

  val pubsubConfig = config.as[PubsubConfig]("pubsub")
  val vpcConfig = config.as[VPCConfig]("vpc")

  val topicName = ProjectTopicName.of(pubsubConfig.pubsubGoogleProject.value, pubsubConfig.topicName)
  val subscriberConfig: SubscriberConfig =
    SubscriberConfig(applicationConfig.leoServiceAccountJsonFile.toString, topicName, 1 minute, None)

  private val topic = ProjectTopicName.of(pubsubConfig.pubsubGoogleProject.value, pubsubConfig.topicName)
  private val retryConfig = GoogleTopicAdminInterpreter.defaultRetryConfig
  val publisherConfig: PublisherConfig =
    PublisherConfig(applicationConfig.leoServiceAccountJsonFile.toString, topic, retryConfig)

  val dataprocInterpreterConfig = DataprocInterpreterConfig(dataprocConfig,
                                                            googleGroupsConfig,
                                                            welderConfig,
                                                            imageConfig,
                                                            proxyConfig,
                                                            clusterResourcesConfig,
                                                            clusterFilesConfig,
                                                            monitorConfig)

  val gceInterpreterConfig = GceInterpreterConfig(gceConfig,
                                                  welderConfig,
                                                  imageConfig,
                                                  proxyConfig,
                                                  clusterResourcesConfig,
                                                  clusterFilesConfig,
                                                  monitorConfig)
  val vpcInterpreterConfig = VPCInterpreterConfig(vpcConfig)
}
