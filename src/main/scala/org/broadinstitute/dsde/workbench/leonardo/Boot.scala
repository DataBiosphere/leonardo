package org.broadinstitute.dsde.workbench.leonardo

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.google.GoogleCredentialModes.{Pem, Token}
import org.broadinstitute.dsde.workbench.google.{GoogleStorageDAO, HttpGoogleIamDAO, HttpGoogleProjectDAO, HttpGoogleStorageDAO}
import org.broadinstitute.dsde.workbench.leonardo.api.{LeoRoutes, StandardUserInfoDirectives}
import org.broadinstitute.dsde.workbench.leonardo.auth.{LeoAuthProviderHelper, ServiceAccountProviderHelper}
import org.broadinstitute.dsde.workbench.leonardo.config.{AutoDeleteConfig, AutoFreezeConfig, ClusterBucketConfig, ClusterDefaultsConfig, ClusterDnsCacheConfig, ClusterFilesConfig, ClusterLifecycleConfig, ClusterResourcesConfig, DataprocConfig, LeoExecutionModeConfig, MonitorConfig, ProxyConfig, SamConfig, SwaggerConfig, ZombieClusterConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.{HttpJupyterDAO, HttpRStudioDAO, HttpSamDAO}
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{HttpGoogleComputeDAO, HttpGoogleDataprocDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache
import org.broadinstitute.dsde.workbench.leonardo.model.google.{NetworkTag, VPCNetworkName, VPCSubnetName}
import org.broadinstitute.dsde.workbench.leonardo.monitor.{ClusterDateAccessedActor, ClusterMonitorSupervisor, ZombieClusterMonitor}
import org.broadinstitute.dsde.workbench.leonardo.service.{LeonardoService, ProxyService, StatusService}
import org.broadinstitute.dsde.workbench.leonardo.util.BucketHelper

object Boot extends App with LazyLogging {
  private def startup(): Unit = {

    // Specifies the name service providers to use, in priority order.
    // "dns,Jupyter" maps to the org.broadinstitute.dsde.workbench.leonardo.dns.JupyterNameService class.
    // "default" is the Java default name service.
    //
    // We do this so we can map *.jupyter-{{env}}.firecloud.org names to real Dataproc IP addresses, and
    // still pass SSL wildcard certificate verification. If we change this to not use a wildcard cert
    // and instead generate a new cert per Dataproc cluster, then we could probably generate the certs
    // based on the IP address and get rid of this code.
    //
    // See also: http://docs.oracle.com/javase/8/docs/technotes/guides/net/properties.html
    System.setProperty("sun.net.spi.nameservice.provider.1", "dns,Jupyter")
    System.setProperty("sun.net.spi.nameservice.provider.2", "default")

    val config = ConfigFactory.parseResources("leonardo.conf").withFallback(ConfigFactory.load())
    val whitelist = config.as[Set[String]]("auth.whitelistProviderConfig.whitelist").map(_.toLowerCase)
    val dataprocConfig = config.as[DataprocConfig]("dataproc")
    val proxyConfig = config.as[ProxyConfig]("proxy")
    val swaggerConfig = config.as[SwaggerConfig]("swagger")
    val clusterFilesConfig = config.as[ClusterFilesConfig]("clusterFiles")
    val clusterResourcesConfig = config.as[ClusterResourcesConfig]("clusterResources")
    val clusterDefaultsConfig = config.as[ClusterDefaultsConfig]("clusterDefaults")
    val monitorConfig = config.as[MonitorConfig]("monitor")
    val samConfig = config.as[SamConfig]("sam")
    val clusterLifecycleConfig = config.as[ClusterLifecycleConfig]("clusterLifecycle")
    val contentSecurityPolicy = config.as[Option[String]]("jupyterConfig.contentSecurityPolicy").getOrElse("default-src: 'self'")
    val zombieClusterMonitorConfig = config.as[ZombieClusterConfig]("zombieClusterMonitor")
    val clusterDnsCacheConfig = config.as[ClusterDnsCacheConfig]("clusterDnsCache")
    val leoExecutionModeConfig = config.as[LeoExecutionModeConfig]("leoExecutionMode")
    val clusterBucketConfig = config.as[ClusterBucketConfig]("clusterBucket")

    // we need an ActorSystem to host our application in
    implicit val system = ActorSystem("leonardo")
    implicit val materializer = ActorMaterializer()
    import system.dispatcher

    val serviceAccountProviderClass = config.as[String]("serviceAccounts.providerClass")
    val serviceAccountConfig = config.getConfig("serviceAccounts.providerConfig")
    val serviceAccountProvider = ServiceAccountProviderHelper.create(serviceAccountProviderClass, serviceAccountConfig)

    val authProviderClass = config.as[String]("auth.providerClass")
    val authConfig = config.getConfig("auth.providerConfig")
    val authProvider = LeoAuthProviderHelper.create(authProviderClass, authConfig, serviceAccountProvider)

    val dbRef = DbReference.init(config)
    system.registerOnTermination {
      dbRef.database.close()
    }

    val petGoogleStorageDAO: String => GoogleStorageDAO = token => {
      new HttpGoogleStorageDAO(dataprocConfig.applicationName, Token(() => token), "google")
    }

    val (leoServiceAccountEmail, leoServiceAccountPemFile) = serviceAccountProvider.getLeoServiceAccountAndKey
    val gdDAO = new HttpGoogleDataprocDAO(dataprocConfig.applicationName, Pem(leoServiceAccountEmail, leoServiceAccountPemFile), "google", NetworkTag(dataprocConfig.networkTag), dataprocConfig.vpcNetwork.map(VPCNetworkName), dataprocConfig.vpcSubnet.map(VPCSubnetName), dataprocConfig.dataprocDefaultRegion, dataprocConfig.dataprocZone, dataprocConfig.defaultExecutionTimeout)
    val googleComputeDAO = new HttpGoogleComputeDAO(dataprocConfig.applicationName, Pem(leoServiceAccountEmail, leoServiceAccountPemFile), "google")
    val googleIamDAO = new HttpGoogleIamDAO(dataprocConfig.applicationName, Pem(leoServiceAccountEmail, leoServiceAccountPemFile), "google")
    val googleStorageDAO = new HttpGoogleStorageDAO(dataprocConfig.applicationName, Pem(leoServiceAccountEmail, leoServiceAccountPemFile), "google")
    val clusterDnsCache = new ClusterDnsCache(proxyConfig, dbRef, clusterDnsCacheConfig)
    val bucketHelper = new BucketHelper(dataprocConfig, gdDAO, googleComputeDAO, googleStorageDAO, serviceAccountProvider)
    val leonardoService = new LeonardoService(dataprocConfig, clusterFilesConfig, clusterResourcesConfig, clusterDefaultsConfig, proxyConfig, swaggerConfig, clusterLifecycleConfig.autoFreezeConfig, clusterLifecycleConfig.autoDeleteConfig, gdDAO, googleComputeDAO, googleIamDAO, googleStorageDAO, petGoogleStorageDAO, dbRef, authProvider, serviceAccountProvider, whitelist, bucketHelper, contentSecurityPolicy)

    if(leoExecutionModeConfig.backLeo) {
      val googleProjectDAO = new HttpGoogleProjectDAO(dataprocConfig.applicationName, Pem(leoServiceAccountEmail, leoServiceAccountPemFile), "google")
      val jupyterDAO = new HttpJupyterDAO(clusterDnsCache)
      val rstudioDAO = new HttpRStudioDAO(clusterDnsCache)
      val clusterMonitorSupervisor = system.actorOf(ClusterMonitorSupervisor.props(monitorConfig, dataprocConfig, clusterBucketConfig, gdDAO, googleComputeDAO, googleIamDAO, googleStorageDAO, dbRef, authProvider, clusterLifecycleConfig, jupyterDAO, rstudioDAO, leonardoService))
      val zombieClusterMonitor = system.actorOf(ZombieClusterMonitor.props(zombieClusterMonitorConfig, gdDAO, googleProjectDAO, dbRef))
    }
    
    val samDAO = new HttpSamDAO(samConfig.server)
    val clusterDateAccessedActor = system.actorOf(ClusterDateAccessedActor.props(clusterLifecycleConfig, dbRef))
    val proxyService = new ProxyService(proxyConfig, gdDAO, dbRef, clusterDnsCache, authProvider, clusterDateAccessedActor)
    val statusService = new StatusService(gdDAO, samDAO, dbRef, dataprocConfig)
    val leoRoutes = new LeoRoutes(leonardoService, proxyService, statusService, swaggerConfig) with StandardUserInfoDirectives

    Http().bindAndHandle(leoRoutes.route, "0.0.0.0", 8080)
      .recover {
        case t: Throwable =>
          logger.error("FATAL - failure starting http server", t)
          throw t
      }
  }

  startup()
}
