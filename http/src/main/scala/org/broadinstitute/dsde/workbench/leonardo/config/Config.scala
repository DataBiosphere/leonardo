package org.broadinstitute.dsde.workbench.leonardo.config

import java.io.File

import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ValueReader
import org.broadinstitute.dsde.workbench.leonardo.auth.sam.SamAuthProviderConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.HttpSamDaoConfig
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterStatus
import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterResource, ServiceAccountProviderConfig}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.util.toScalaDuration
import org.http4s.Uri

import scala.collection.JavaConverters._
import scala.concurrent.duration._

object Config {
  val config = ConfigFactory.parseResources("leonardo.conf").withFallback(ConfigFactory.load())

  implicit val swaggerReader: ValueReader[SwaggerConfig] = ValueReader.relative { config =>
    SwaggerConfig(
      config.getString("googleClientId"),
      config.getString("realm")
    )
  }

  implicit val dataprocConfigReader: ValueReader[DataprocConfig] = ValueReader.relative { config =>
    DataprocConfig(
      config.getString("applicationName"),
      config.getString("dataprocDefaultRegion"),
      config.getAs[String]("dataprocZone"),
      GoogleProject(config.getString("leoGoogleProject")),
      config.getString("jupyterImage"),
      config.getString("clusterUrlBase"),
      config.getString("jupyterServerName"),
      config.getString("rstudioServerName"),
      config.getString("welderServerName"),
      config.getString("firewallRuleName"),
      config.getString("networkTag"),
      config.getStringList("defaultScopes").asScala.toSet,
      config.getString("welderDockerImage"),
      config.getAs[String]("vpcNetwork"),
      config.getAs[String]("vpcSubnet"),
      config.getAs[String]("projectVPCNetworkLabel"),
      config.getAs[String]("projectVPCSubnetLabel"),
      config.getString("welderEnabledNotebooksDir"),
      config.getString("welderDisabledNotebooksDir"),
      config.getAs[String]("customDataprocImage"),
      config.getAs[String]("deployWelderLabel"),
      config.getAs[String]("updateWelderLabel"),
      config.getAs[String]("deployWelderCutoffDate")
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

  implicit val clusterResourcesConfigReader: ValueReader[ClusterResourcesConfig] = ValueReader.relative { config =>
    ClusterResourcesConfig(
      ClusterResource(config.getString("initActionsScript")),
      ClusterResource(config.getString("initVmScript")),
      ClusterResource(config.getString("startupScript")),
      ClusterResource(config.getString("jupyterDockerCompose")),
      ClusterResource(config.getString("rstudioDockerCompose")),
      ClusterResource(config.getString("proxyDockerCompose")),
      ClusterResource(config.getString("welderDockerCompose")),
      ClusterResource(config.getString("proxySiteConf")),
      ClusterResource(config.getString("jupyterNotebookConfigUri")),
      ClusterResource(config.getString("jupyterNotebookFrontendConfigUri")),
      ClusterResource(config.getString("customEnvVarsConfigUri"))
    )
  }

  implicit val clusterDefaultConfigReader: ValueReader[ClusterDefaultsConfig] = ValueReader.relative { config =>
    ClusterDefaultsConfig(
      config.getInt("numberOfWorkers"),
      config.getString("masterMachineType"),
      config.getInt("masterDiskSize"),
      config.getString("workerMachineType"),
      config.getInt("workerDiskSize"),
      config.getInt("numberOfWorkerLocalSSDs"),
      config.getInt("numberOfPreemptibleWorkers")
    )
  }

  implicit val liquibaseReader: ValueReader[LiquibaseConfig] = ValueReader.relative { config =>
    LiquibaseConfig(config.as[String]("changelog"), config.as[Boolean]("initWithLiquibase"))
  }

  implicit val proxyConfigReader: ValueReader[ProxyConfig] = ValueReader.relative { config =>
    ProxyConfig(
      config.getString("jupyterProxyDockerImage"),
      config.getString("proxyServerName"),
      config.getInt("jupyterPort"),
      config.getString("jupyterProtocol"),
      config.getString("jupyterDomain"),
      toScalaDuration(config.getDuration("dnsPollPeriod")),
      toScalaDuration(config.getDuration("cacheExpiryTime")),
      config.getInt("cacheMaxSize")
    )
  }

  implicit val monitorConfigReader: ValueReader[MonitorConfig] = ValueReader.relative { config =>
    val timeouts: Map[ClusterStatus, FiniteDuration] = Map(
      ClusterStatus.Creating -> toScalaDuration(config.getDuration("creatingTimeLimit")),
      ClusterStatus.Starting -> toScalaDuration(config.getDuration("startingTimeLimit")),
      ClusterStatus.Stopping -> toScalaDuration(config.getDuration("stoppingTimeLimit")),
      ClusterStatus.Deleting -> toScalaDuration(config.getDuration("deletingTimeLimit")),
      ClusterStatus.Updating -> toScalaDuration(config.getDuration("updatingTimeLimit"))
    )

    MonitorConfig(toScalaDuration(config.getDuration("pollPeriod")), config.getInt("maxRetries"), config.getBoolean("recreateCluster"), timeouts)
  }

  implicit val samConfigReader: ValueReader[SamConfig] = ValueReader.relative { config =>
    SamConfig(config.getString("server"))
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
      toScalaDuration(config.getDuration("creationHangTolerance"))
    )
  }

  implicit val clusterServiceValueReader: ValueReader[ClusterToolConfig] = ValueReader.relative { config =>
    ClusterToolConfig(
      toScalaDuration(config.getDuration("pollPeriod"))
    )
  }

  implicit val clusterDnsCacheConfigValueReader: ValueReader[ClusterDnsCacheConfig] = ValueReader.relative { config =>
    ClusterDnsCacheConfig(
      toScalaDuration(config.getDuration("cacheExpiryTime")),
      config.getInt("cacheMaxSize")
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
  implicit val samAuthConfigConfigValueReader: ValueReader[SamAuthProviderConfig] = ValueReader.relative { config =>
    SamAuthProviderConfig(
      config.getOrElse("notebookAuthCacheEnabled", true),
      config.getAs[Int]("notebookAuthCacheMaxSize").getOrElse(1000),
      config.getAs[FiniteDuration]("notebookAuthCacheExpiryTime").getOrElse(15 minutes),
    )
  }
  implicit val workbenchEmailValueReader: ValueReader[WorkbenchEmail] = stringValueReader.map(WorkbenchEmail)
  implicit val fileValueReader: ValueReader[File] = stringValueReader.map(s => new File(s))
  implicit val serviceAccountProviderConfigValueReader: ValueReader[ServiceAccountProviderConfig] = ValueReader.relative { config =>
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

  val dataprocConfig = config.as[DataprocConfig]("dataproc")
  val proxyConfig = config.as[ProxyConfig]("proxy")
  val swaggerConfig = config.as[SwaggerConfig]("swagger")
  val clusterFilesConfig = config.as[ClusterFilesConfig]("clusterFiles")
  val clusterResourcesConfig = config.as[ClusterResourcesConfig]("clusterResources")
  val clusterDefaultsConfig = config.as[ClusterDefaultsConfig]("clusterDefaults")
  val monitorConfig = config.as[MonitorConfig]("monitor")
  val samConfig = config.as[SamConfig]("sam")
  val autoFreezeConfig = config.as[AutoFreezeConfig]("autoFreeze")
  val contentSecurityPolicy = config.as[Option[String]]("jupyterConfig.contentSecurityPolicy").getOrElse("default-src: 'self'")
  val zombieClusterMonitorConfig = config.as[ZombieClusterConfig]("zombieClusterMonitor")
  val clusterToolMonitorConfig = config.as[ClusterToolConfig](path = "clusterToolMonitor")
  val clusterDnsCacheConfig = config.as[ClusterDnsCacheConfig]("clusterDnsCache")
  val leoExecutionModeConfig = config.as[LeoExecutionModeConfig]("leoExecutionMode")
  val clusterBucketConfig = config.as[ClusterBucketConfig]("clusterBucket")
  val serviceAccountProviderClass = config.as[String]("serviceAccounts.providerClass")
  val leoServiceAccountJsonFile = config.as[String]("google.leoServiceAccountJsonFile")
  val samAuthConfig = config.as[SamAuthProviderConfig]("auth.providerConfig")
  val httpSamDap2Config = config.as[HttpSamDaoConfig]("auth.providerConfig")
  val liquibaseConfig = config.as[LiquibaseConfig]("liquibase")
}
