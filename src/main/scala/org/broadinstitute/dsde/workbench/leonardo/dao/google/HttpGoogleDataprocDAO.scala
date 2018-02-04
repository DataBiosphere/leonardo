package org.broadinstitute.dsde.workbench.leonardo.dao.google

import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import cats.data.OptionT
import cats.implicits._
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.http.HttpResponseException
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.BigqueryScopes
import com.google.api.services.cloudresourcemanager.CloudResourceManager
import com.google.api.services.compute.model.Firewall.Allowed
import com.google.api.services.compute.model.{Firewall, Instance}
import com.google.api.services.compute.{Compute, ComputeScopes}
import com.google.api.services.dataproc.Dataproc
import com.google.api.services.dataproc.model.{Cluster => DataprocCluster, ClusterConfig => DataprocClusterConfig, ClusterStatus => DataprocClusterStatus, Operation => DataprocOperation, _}
import com.google.api.services.oauth2.{Oauth2, Oauth2Scopes}
import org.broadinstitute.dsde.workbench.google.GoogleCredentialModes.GoogleCredentialMode
import org.broadinstitute.dsde.workbench.google.AbstractHttpGoogleDAO
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterStatus.ClusterStatus
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.metrics.GoogleInstrumentedService
import org.broadinstitute.dsde.workbench.metrics.GoogleInstrumentedService.GoogleInstrumentedService
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsPath, GoogleProject}
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail, WorkbenchUserId}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class HttpGoogleDataprocDAO(appName: String,
                            googleCredentialMode: GoogleCredentialMode,
                            override val workbenchMetricBaseName: String,
                            defaultNetworkTag: NetworkTag,
                            defaultRegion: String)
                           (implicit override val system: ActorSystem, override val executionContext: ExecutionContext)
  extends AbstractHttpGoogleDAO(appName, googleCredentialMode, workbenchMetricBaseName) with GoogleDataprocDAO {

  override implicit val service: GoogleInstrumentedService = GoogleInstrumentedService.Dataproc

  override val scopes: Seq[String] = Seq(ComputeScopes.CLOUD_PLATFORM)

  private lazy val oauth2Scopes = List(Oauth2Scopes.USERINFO_EMAIL, Oauth2Scopes.USERINFO_PROFILE)
  private lazy val bigqueryScopes = List(BigqueryScopes.BIGQUERY)

  private val httpTransport = GoogleNetHttpTransport.newTrustedTransport
  private val jsonFactory = JacksonFactory.getDefaultInstance

  private lazy val dataproc = {
    new Dataproc.Builder(httpTransport, jsonFactory, googleCredential)
      .setApplicationName(appName).build()
  }

  private lazy val compute = {
    new Compute.Builder(httpTransport, jsonFactory, googleCredential)
      .setApplicationName(appName).build()
  }

  // TODO move out of this DAO
  private lazy val oauth2 =
    new Oauth2.Builder(httpTransport, jsonFactory, null)
      .setApplicationName(appName).build()

  // TODO move out of this DAO
  private lazy val cloudResourceManager = {
    new CloudResourceManager.Builder(httpTransport, jsonFactory, googleCredential)
      .setApplicationName(appName).build()
  }

  override def createCluster(googleProject: GoogleProject, clusterName: ClusterName, machineConfig: MachineConfig, initScript: GcsPath, clusterServiceAccount: Option[WorkbenchEmail], credentialsFileName: Option[String]): Future[Operation] = {
    val cluster = new DataprocCluster()
      .setClusterName(clusterName.value)
      .setConfig(getClusterConfig(machineConfig, initScript, clusterServiceAccount,credentialsFileName))

    val request = dataproc.projects().regions().clusters().create(googleProject.value, defaultRegion, cluster)

    retryWhen500orGoogleError(() => executeGoogleRequest(request)).map { op =>
      Operation(OperationName(op.getName), getOperationUUID(op))
    }
  }

  override def deleteCluster(googleProject: GoogleProject, clusterName: ClusterName): Future[Unit] = {
    val request = dataproc.projects().regions().clusters().delete(googleProject.value, defaultRegion, clusterName.value)
    retryWithRecoverWhen500orGoogleError { () =>
      executeGoogleRequest(request)
      ()
    } {
      case e: HttpResponseException if e.getStatusCode == StatusCodes.NotFound.intValue => ()
      case e: HttpResponseException if e.getStatusCode == StatusCodes.BadRequest &&
        e.getMessage.contains("it has other pending delete operations against it") => ()
    }
  }

  override def getClusterStatus(googleProject: GoogleProject, clusterName: ClusterName): Future[ClusterStatus] = {
    val transformed = for {
      cluster <- OptionT(getCluster(googleProject, clusterName))
      status <- OptionT.pure[Future, ClusterStatus](
        Try(ClusterStatus.withNameIgnoreCase(cluster.getStatus.getState)).toOption.getOrElse(ClusterStatus.Unknown))
    } yield status

    transformed.value.map(_.getOrElse(ClusterStatus.Deleted))
  }

  override def listClusters(googleProject: GoogleProject): Future[List[UUID]] = {
    val request = dataproc.projects().regions().clusters().list(googleProject.value, defaultRegion)
    // Use OptionT to handle nulls in the Google response
    val transformed = for {
      result <- OptionT.liftF(retryWhen500orGoogleError(() => executeGoogleRequest(request)))
      googleClusters <- OptionT.fromOption[Future](Option(result.getClusters))
    } yield {
      googleClusters.asScala.toList.map(c => UUID.fromString(c.getClusterUuid))
    }
    transformed.value.map(_.getOrElse(List.empty))
  }

  override def getClusterMasterInstanceIp(googleProject: GoogleProject, clusterName: ClusterName): Future[Option[IP]] = {
    // OptionT is handy when you potentially want to deal with Future[A], Option[A],
    // Future[Option[A]], and A all in the same flatMap!
    //
    // Legend:
    // - OptionT.pure turns an A into an OptionT[F, A]
    // - OptionT.liftF turns an F[A] into an OptionT[F, A]
    // - OptionT.fromOption turns an Option[A] into an OptionT[F, A]

    val ipOpt: OptionT[Future, IP] = for {
      cluster <- OptionT(getCluster(googleProject, clusterName))
      masterInstanceName <- OptionT.fromOption[Future] { getMasterInstanceName(cluster) }
      masterInstanceZone <- OptionT.fromOption[Future] { getZone(cluster) }
      masterInstance <- OptionT(getInstance(googleProject, masterInstanceZone, masterInstanceName))
      masterInstanceIp <- OptionT.fromOption[Future] { getInstanceIP(masterInstance) }
    } yield masterInstanceIp

    // OptionT[Future, String] is simply a case class wrapper for Future[Option[String]].
    // So this just grabs the inner value and returns it.
    ipOpt.value
  }

  def getClusterStagingBucket(googleProject: GoogleProject, clusterName: ClusterName): Future[Option[GcsBucketName]] = {
    // If an expression might be null, need to use `OptionT.fromOption(Option(expr))`.
    // `OptionT.pure(expr)` throws a NPE!
    val transformed = for {
      cluster <- OptionT(getCluster(googleProject, clusterName))
      config <- OptionT.fromOption[Future](Option(cluster.getConfig))
      bucket <- OptionT.fromOption[Future](Option(config.getConfigBucket))
    } yield GcsBucketName(bucket)

    transformed.value
  }

  override def getClusterErrorDetails(operationName: OperationName): Future[Option[ClusterErrorDetails]] = {
    val errorOpt: OptionT[Future, ClusterErrorDetails] = for {
      operation <- OptionT(getOperation(operationName)) if operation.getDone
      error <- OptionT.fromOption[Future] { Option(operation.getError) }
      code <- OptionT.fromOption[Future] { Option(error.getCode) }
    } yield ClusterErrorDetails(code, Option(error.getMessage))

    errorOpt.value
  }

  override def updateFirewallRule(googleProject: GoogleProject, firewallRule: FirewallRule): Future[Unit] = {
    val request = compute.firewalls().get(googleProject.value, firewallRule.name.value)
    retryWhen500orGoogleError(() => executeGoogleRequest(request)).recoverWith {
      case e: HttpResponseException if e.getStatusCode == StatusCodes.NotFound.intValue =>
        addFirewallRule(googleProject, firewallRule)
    }.void
  }

  /**
    * Adds a firewall rule in the given google project. This firewall rule allows ingress traffic through a specified port for all
    * VMs with the network tag "leonardo". This rule should only be added once per project.
    * To think about: do we want to remove this rule if a google project no longer has any clusters? */
  private def addFirewallRule(googleProject: GoogleProject, firewallRule: FirewallRule): Future[Unit] = {
    val allowed = new Allowed().setIPProtocol(firewallRule.protocol.value).setPorts(firewallRule.ports.map(_.value).asJava)
    val googleFirewall = new Firewall()
      .setName(firewallRule.name.value)
      .setNetwork(firewallRule.network.value)
      .setTargetTags(firewallRule.targetTags.map(_.value).asJava)
      .setAllowed(List(allowed).asJava)

    val request = compute.firewalls().insert(googleProject.value, googleFirewall)
    logger.info(s"Creating firewall rule with name '${firewallRule.name.value}' in project ${googleProject.value}")
    retryWhen500orGoogleError(() => executeGoogleRequest(request)).void
  }

  override def getUserInfoAndExpirationFromAccessToken(accessToken: String): Future[(UserInfo, Instant)] = {
    val request = oauth2.tokeninfo().setAccessToken(accessToken)
    retryWhen500orGoogleError(() => executeGoogleRequest(request)).map { tokenInfo =>
      (UserInfo(OAuth2BearerToken(accessToken), WorkbenchUserId(tokenInfo.getUserId), WorkbenchEmail(tokenInfo.getEmail), tokenInfo.getExpiresIn.toInt), Instant.now().plusSeconds(tokenInfo.getExpiresIn.toInt))
    }
  }

  override def getComputeEngineDefaultServiceAccount(googleProject: GoogleProject): Future[Option[WorkbenchEmail]] = {
    getProjectNumber(googleProject).map { numberOpt =>
      numberOpt.map { number =>
        // Service account email format documented in:
        // https://cloud.google.com/compute/docs/access/service-accounts#compute_engine_default_service_account
        WorkbenchEmail(s"$number-compute@developer.gserviceaccount.com")
      }
    }
  }

  private def getClusterConfig(machineConfig: MachineConfig, initScript: GcsPath, clusterServiceAccount: Option[WorkbenchEmail], credentialsFileName: Option[String]): DataprocClusterConfig = {
    // Create a GceClusterConfig, which has the common config settings for resources of Google Compute Engine cluster instances,
    // applicable to all instances in the cluster.
    // Set the network tag, which is needed by the firewall rule that allows leo to talk to the cluster
    val gceClusterConfig = new GceClusterConfig().setTags(List(defaultNetworkTag.value).asJava)

    // Set the cluster service account, if present.
    // This is the service account passed to the create cluster API call.
    clusterServiceAccount.foreach { serviceAccountEmail =>
      gceClusterConfig.setServiceAccount(serviceAccountEmail.value).setServiceAccountScopes((oauth2Scopes ++ bigqueryScopes).asJava)
    }

    // Create a NodeInitializationAction, which specifies the executable to run on a node.
    // This executable is our init-actions.sh, which will stand up our jupyter server and proxy.
    val initActions = Seq(new NodeInitializationAction().setExecutableFile(initScript.toUri))

    // Create a config for the master node, if properties are not specified in request, use defaults
    val masterConfig = new InstanceGroupConfig()
      .setMachineTypeUri(machineConfig.masterMachineType.get)
      .setDiskConfig(new DiskConfig().setBootDiskSizeGb(machineConfig.masterDiskSize.get))

    // Create a Cluster Config and give it the GceClusterConfig, the NodeInitializationAction and the InstanceGroupConfig
    createClusterConfig(machineConfig, credentialsFileName)
      .setGceClusterConfig(gceClusterConfig)
      .setInitializationActions(initActions.asJava)
      .setMasterConfig(masterConfig)
  }

  // Expects a Machine Config with master configs defined for a 0 worker cluster and both master and worker
  // configs defined for 2 or more workers.
  private def createClusterConfig(machineConfig: MachineConfig, credentialsFileName: Option[String]): DataprocClusterConfig = {

    val swConfig: SoftwareConfig = getSoftwareConfig(machineConfig.numberOfWorkers, credentialsFileName)

    // If the number of workers is zero, make a Single Node cluster, else make a Standard one
    if (machineConfig.numberOfWorkers.get == 0) {
      new DataprocClusterConfig().setSoftwareConfig(swConfig)
    }
    else // Standard, multi node cluster
      getMultiNodeClusterConfig(machineConfig).setSoftwareConfig(swConfig)
  }

  private def getSoftwareConfig(numWorkers: Option[Int], credentialsFileName: Option[String]) = {
    val authProps: Map[String, String] = credentialsFileName match {
      case None =>
        // If we're not using a notebook service account, no need to set Hadoop properties since
        // the SA credentials are on the metadata server.
        Map.empty

      case Some(fileName) =>
        // If we are using a notebook service account, set the necessary Hadoop properties
        // to specify the location of the notebook service account key file.
        Map(
          "core:google.cloud.auth.service.account.enable" -> "true",
          "core:google.cloud.auth.service.account.json.keyfile" -> fileName
        )
    }

    val dataprocProps: Map[String, String] = if (numWorkers.get == 0) {
      // Set a SoftwareConfig property that makes the cluster have only one node
      Map("dataproc:dataproc.allow.zero.workers" -> "true")
    }
    else
      Map.empty

    val yarnProps = Map(
      // Helps with debugging
      "yarn:yarn.log-aggregation-enable" -> "true",

      // Dataproc 1.1 sets this too high (5586m) which limits the number of Spark jobs that can be run at one time.
      // This has been reduced drastically in Dataproc 1.2. See:
      // https://stackoverflow.com/questions/41185599/spark-default-settings-on-dataproc-especially-spark-yarn-am-memory
      // GAWB-3080 is open for upgrading to Dataproc 1.2, at which point this line can be removed.
      "spark:spark.yarn.am.memory" -> "640m"
    )

    new SoftwareConfig().setProperties((authProps ++ dataprocProps ++ yarnProps).asJava)

      // This gives us Spark 2.0.2. See:
      //   https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-versions
      // Dataproc supports Spark 2.2.0, but there are no pre-packaged Hail distributions past 2.1.0. See:
      //   https://hail.is/docs/stable/getting_started.html
      .setImageVersion("1.1")
  }

  private def getMultiNodeClusterConfig(machineConfig: MachineConfig): DataprocClusterConfig = {
    // Set the configs of the non-preemptible, primary worker nodes
    val clusterConfigWithWorkerConfigs = new DataprocClusterConfig().setWorkerConfig(getPrimaryWorkerConfig(machineConfig))

    // If the number of preemptible workers is greater than 0, set a secondary worker config
    if (machineConfig.numberOfPreemptibleWorkers.get > 0) {
      val preemptibleWorkerConfig = new InstanceGroupConfig()
        .setIsPreemptible(true)
        .setNumInstances(machineConfig.numberOfPreemptibleWorkers.get)

      clusterConfigWithWorkerConfigs.setSecondaryWorkerConfig(preemptibleWorkerConfig)
    } else clusterConfigWithWorkerConfigs
  }

  private def getPrimaryWorkerConfig(machineConfig: MachineConfig): InstanceGroupConfig = {
    val workerDiskConfig = new DiskConfig()
      .setBootDiskSizeGb(machineConfig.workerDiskSize.get)
      .setNumLocalSsds(machineConfig.numberOfWorkerLocalSSDs.get)

    new InstanceGroupConfig()
      .setNumInstances(machineConfig.numberOfWorkers.get)
      .setMachineTypeUri(machineConfig.workerMachineType.get)
      .setDiskConfig(workerDiskConfig)
  }

  /**
    * Gets a dataproc Cluster from the API.
    */
  private def getCluster(googleProject: GoogleProject, clusterName: ClusterName): Future[Option[DataprocCluster]] = {
    val request = dataproc.projects().regions().clusters().get(googleProject.value, defaultRegion, clusterName.value)
    retryWithRecoverWhen500orGoogleError { () =>
      Option(executeGoogleRequest(request))
    } {
      case e: HttpResponseException if e.getStatusCode == StatusCodes.NotFound.intValue => None
    }
  }

  /**
    * Gets a compute Instance from the API.
    */
  private def getInstance(googleProject: GoogleProject, zone: ZoneUri, instanceName: InstanceName): Future[Option[Instance]] = {
    val request = compute.instances().get(googleProject.value, zone.value, instanceName.value)
    retryWithRecoverWhen500orGoogleError { () =>
      Option(executeGoogleRequest(request))
    } {
      case e: HttpResponseException if e.getStatusCode == StatusCodes.NotFound.intValue => None
    }
  }

  /**
    * Gets an Operation from the API.
    */
  private def getOperation(operationName: OperationName): Future[Option[DataprocOperation]] = {
    val request = dataproc.projects().regions().operations().get(operationName.value)
    retryWithRecoverWhen500orGoogleError { () =>
      Option(executeGoogleRequest(request))
    } {
      case e: HttpResponseException if e.getStatusCode == StatusCodes.NotFound.intValue => None
    }
  }

  private def getOperationUUID(dop: DataprocOperation): UUID = {
    UUID.fromString(dop.getMetadata.get("clusterUuid").toString)
  }

  /**
    * Gets the master instance name from a dataproc cluster, with error handling.
    * @param cluster the Google dataproc cluster
    * @return error or master instance name
    */
  private def getMasterInstanceName(cluster: DataprocCluster): Option[InstanceName] = {
    for {
      config <- Option(cluster.getConfig)
      masterConfig <- Option(config.getMasterConfig)
      instanceNames <- Option(masterConfig.getInstanceNames)
      masterInstance <- instanceNames.asScala.headOption
    } yield InstanceName(masterInstance)
  }

  /**
    * Gets the zone (not to be confused with region) of a dataproc cluster, with error handling.
    * @param cluster the Google dataproc cluster
    * @return error or the master instance zone
    */
  private def getZone(cluster: DataprocCluster): Option[ZoneUri] = {
    def parseZone(zoneUri: String): ZoneUri = {
      zoneUri.lastIndexOf('/') match {
        case -1 => ZoneUri(zoneUri)
        case n => ZoneUri(zoneUri.substring(n + 1))
      }
    }

    for {
      config <- Option(cluster.getConfig)
      gceConfig <- Option(config.getGceClusterConfig)
      zoneUri <- Option(gceConfig.getZoneUri)
    } yield parseZone(zoneUri)
  }

  /**
    * Gets the public IP from a google Instance, with error handling.
    * @param instance the Google instance
    * @return error or public IP, as a String
    */
  private def getInstanceIP(instance: Instance): Option[IP] = {
    for {
      interfaces <- Option(instance.getNetworkInterfaces)
      interface <- interfaces.asScala.headOption
      accessConfigs <- Option(interface.getAccessConfigs)
      accessConfig <- accessConfigs.asScala.headOption
    } yield IP(accessConfig.getNatIP)
  }


  private def getProjectNumber(googleProject: GoogleProject): Future[Option[Long]] = {
    val request = cloudResourceManager.projects().get(googleProject.value)
    retryWhen500orGoogleError(() => executeGoogleRequest(request)).map { project =>
      Option(project.getProjectId).map(_.toLong)
    } recover{
      case e: HttpResponseException if e.getStatusCode == StatusCodes.NotFound.intValue => None
    }
  }
}
