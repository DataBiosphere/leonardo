package org.broadinstitute.dsde.workbench.leonardo.config

import java.io.File
import java.nio.file.Paths

import com.google.pubsub.v1.ProjectTopicName
import com.typesafe.config.{ConfigFactory, Config => TypeSafeConfig}
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ValueReader
import org.broadinstitute.dsde.workbench.google2.{
  GoogleTopicAdminInterpreter,
  MachineTypeName,
  PublisherConfig,
  SubscriberConfig
}
import org.broadinstitute.dsde.workbench.leonardo.{
  CustomDataprocImage,
  MemorySize,
  RuntimeConfig,
  RuntimeResource,
  RuntimeStatus
}
import org.broadinstitute.dsde.workbench.leonardo.auth.sam.SamAuthProviderConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.HttpSamDaoConfig
import org.broadinstitute.dsde.workbench.leonardo.model.ServiceAccountProviderConfig
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
      GoogleProject(config.getString("leoGoogleProject")),
      new File(config.getString("leoServiceAccountJsonFile"))
    )
  }

  implicit val dataprocRuntimeConfigReader: ValueReader[RuntimeConfig.DataprocConfig] = ValueReader.relative { config =>
    RuntimeConfig.DataprocConfig(
      config.getInt("numberOfWorkers"),
      MachineTypeName(config.getString("masterMachineType")),
      config.getInt("masterDiskSize"),
      config.getAs[String]("workerMachineType").map(MachineTypeName),
      config.getAs[Int]("workerDiskSize"),
      config.getAs[Int]("numberOfWorkerLocalSSDs"),
      config.getAs[Int]("numberOfPreemptibleWorkers")
    )
  }

  implicit val gceRuntimeConfigReader: ValueReader[RuntimeConfig.GceConfig] = ValueReader.relative { config =>
    RuntimeConfig.GceConfig(
      MachineTypeName(config.getString("machineType")),
      config.getInt("diskSize")
    )
  }

  implicit val dataprocConfigReader: ValueReader[DataprocConfig] = ValueReader.relative { config =>
    DataprocConfig(
      config.getString("dataprocDefaultRegion"),
      config.getAs[String]("dataprocZone"),
      config.getStringList("defaultScopes").asScala.toSet,
      CustomDataprocImage(config.getString("legacyCustomDataprocImage")),
      CustomDataprocImage(config.getString("customDataprocImage")),
      config.getAs[MemorySize]("dataprocReservedMemory"),
      config.as[RuntimeConfig.DataprocConfig]("runtimeDefaults")
    )
  }

  implicit val gceConfigReader: ValueReader[GceConfig] = ValueReader.relative { config =>
    GceConfig(
      config.getStringList("defaultScopes").asScala.toSet,
      config.getAs[MemorySize]("gceReservedMemory"),
      config.as[RuntimeConfig.GceConfig]("runtimeDefaults")
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
      config.getString("welderImage"),
      config.getString("jupyterImage"),
      config.getString("legacyJupyterImage"),
      config.getString("proxyImage"),
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
      RuntimeResource(config.getString("initActionsScript")),
      RuntimeResource(config.getString("startupScript")),
      RuntimeResource(config.getString("shutdownScript")),
      RuntimeResource(config.getString("jupyterDockerCompose")),
      RuntimeResource(config.getString("rstudioDockerCompose")),
      RuntimeResource(config.getString("proxyDockerCompose")),
      RuntimeResource(config.getString("welderDockerCompose")),
      RuntimeResource(config.getString("proxySiteConf")),
      RuntimeResource(config.getString("jupyterNotebookConfigUri")),
      RuntimeResource(config.getString("jupyterNotebookFrontendConfigUri")),
      RuntimeResource(config.getString("customEnvVarsConfigUri"))
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
      config.getString("proxyProtocol"),
      config.getString("firewallRuleName"),
      config.getString("networkTag"),
      config.getString("projectVPCNetworkLabel"),
      config.getString("projectVPCSubnetLabel"),
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

  implicit val zombieClusterConfigValueReader: ValueReader[ZombieClusterConfig] = ValueReader.relative { config =>
    ZombieClusterConfig(
      config.getBoolean("enableZombieClusterMonitor"),
      toScalaDuration(config.getDuration("pollPeriod")),
      toScalaDuration(config.getDuration("creationHangTolerance")),
      config.getInt("concurrency")
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
  implicit val workbenchEmailValueReader: ValueReader[WorkbenchEmail] = stringValueReader.map(WorkbenchEmail)
  implicit val fileValueReader: ValueReader[File] = stringValueReader.map(s => new File(s))
  implicit val serviceAccountProviderConfigValueReader: ValueReader[ServiceAccountProviderConfig] =
    ValueReader.relative { config =>
      ServiceAccountProviderConfig(
        config.as[WorkbenchEmail]("leoServiceAccountEmail"),
        config.as[File]("leoServiceAccountPemFile")
      )
    }

  val serviceAccountProviderConfig = config.as[ServiceAccountProviderConfig]("serviceAccounts.providerConfig")

  implicit val httpSamDao2ConfigValueReader: ValueReader[HttpSamDaoConfig] = ValueReader.relative { config =>
    HttpSamDaoConfig(
      Uri.unsafeFromString(config.as[String]("samServer")),
      config.getOrElse("petTokenCacheEnabled", true),
      config.getAs[FiniteDuration]("petTokenCacheExpiryTime").getOrElse(60 minutes),
      config.getAs[Int]("petTokenCacheMaxSize").getOrElse(1000),
      serviceAccountProviderConfig
    )
  }

  implicit val memorySizeReader: ValueReader[MemorySize] = (config: TypeSafeConfig, path: String) =>
    MemorySize(config.getBytes(path))

  val applicationConfig = config.as[ApplicationConfig]("application")
  val googleGroupsConfig = config.as[GoogleGroupsConfig]("google.groups")
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
  val contentSecurityPolicy =
    config.as[Option[String]]("jupyterConfig.contentSecurityPolicy").getOrElse("default-src: 'self'")
  val zombieClusterMonitorConfig = config.as[ZombieClusterConfig]("zombieClusterMonitor")
  val clusterToolMonitorConfig = config.as[ClusterToolConfig](path = "clusterToolMonitor")
  val clusterDnsCacheConfig = config.as[ClusterDnsCacheConfig]("clusterDnsCache")
  val leoExecutionModeConfig = config.as[LeoExecutionModeConfig]("leoExecutionMode")
  val clusterBucketConfig = config.as[ClusterBucketConfig]("clusterBucket")
  val uiConfig = config.as[ClusterUIConfig]("ui")
  val serviceAccountProviderClass = config.as[String]("serviceAccounts.providerClass")
  val leoServiceAccountJsonFile = config.as[String]("google.leoServiceAccountJsonFile")
  val samAuthConfig = config.as[SamAuthProviderConfig]("auth.providerConfig")
  val httpSamDap2Config = config.as[HttpSamDaoConfig]("auth.providerConfig")
  val liquibaseConfig = config.as[LiquibaseConfig]("liquibase")
  val welderConfig = config.as[WelderConfig]("welder")
  val dbConcurrency = config.as[Long]("mysql.concurrency")

  val pubsubConfig = config.as[PubsubConfig]("pubsub")

  val topicName = ProjectTopicName.of(pubsubConfig.pubsubGoogleProject.value, pubsubConfig.topicName)
  val subscriberConfig: SubscriberConfig = SubscriberConfig(leoServiceAccountJsonFile, topicName, 1 minute, None)

  private val topic = ProjectTopicName.of(pubsubConfig.pubsubGoogleProject.value, pubsubConfig.topicName)
  private val retryConfig = GoogleTopicAdminInterpreter.defaultRetryConfig
  val publisherConfig: PublisherConfig = PublisherConfig(leoServiceAccountJsonFile, topic, retryConfig)
}
