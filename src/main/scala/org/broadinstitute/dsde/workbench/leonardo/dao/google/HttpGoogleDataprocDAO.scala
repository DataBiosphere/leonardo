package org.broadinstitute.dsde.workbench.leonardo.dao.google

import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import cats.data.OptionT
import cats.implicits._
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.http.HttpResponseException
import com.google.api.services.compute.ComputeScopes
import com.google.api.services.dataproc.Dataproc
import com.google.api.services.dataproc.model.{Cluster => DataprocCluster, ClusterConfig => DataprocClusterConfig, ClusterStatus => DataprocClusterStatus, Operation => DataprocOperation, _}
import com.google.api.services.oauth2.Oauth2
import org.broadinstitute.dsde.workbench.google.AbstractHttpGoogleDAO
import org.broadinstitute.dsde.workbench.google.GoogleCredentialModes._
import org.broadinstitute.dsde.workbench.leonardo.model.google.DataprocRole.{Master, SecondaryWorker, Worker}
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.leonardo.service.{AuthorizationError, BucketObjectAccessException, DataprocDisabledException}
import org.broadinstitute.dsde.workbench.metrics.GoogleInstrumentedService
import org.broadinstitute.dsde.workbench.metrics.GoogleInstrumentedService.GoogleInstrumentedService
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsPath, GoogleProject}
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail, WorkbenchException, WorkbenchUserId}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class HttpGoogleDataprocDAO(appName: String,
                            googleCredentialMode: GoogleCredentialMode,
                            override val workbenchMetricBaseName: String,
                            networkTag: NetworkTag,
                            vpcNetwork: Option[VPCNetworkName],
                            vpcSubnet: Option[VPCSubnetName],
                            defaultRegion: String)
                           (implicit override val system: ActorSystem, override val executionContext: ExecutionContext)
  extends AbstractHttpGoogleDAO(appName, googleCredentialMode, workbenchMetricBaseName) with GoogleDataprocDAO {

  override implicit val service: GoogleInstrumentedService = GoogleInstrumentedService.Dataproc

  override val scopes: Seq[String] = Seq(ComputeScopes.CLOUD_PLATFORM)

  private lazy val dataproc = {
    new Dataproc.Builder(httpTransport, jsonFactory, googleCredential)
      .setApplicationName(appName).build()
  }

  // TODO move out of this DAO
  private lazy val oauth2 =
    new Oauth2.Builder(httpTransport, jsonFactory, null)
      .setApplicationName(appName).build()

  override def createCluster(googleProject: GoogleProject,
                             clusterName: ClusterName,
                             machineConfig: MachineConfig,
                             initScript: GcsPath,
                             clusterServiceAccount: Option[WorkbenchEmail],
                             credentialsFileName: Option[String],
                             stagingBucket: GcsBucketName,
                             serviceAccountScopes: Seq[String]): Future[Operation] = {
    val cluster = new DataprocCluster()
      .setClusterName(clusterName.value)
      .setConfig(getClusterConfig(machineConfig, initScript, clusterServiceAccount, credentialsFileName, stagingBucket, serviceAccountScopes))

    val request = dataproc.projects().regions().clusters().create(googleProject.value, defaultRegion, cluster)

    retryWithRecoverWhen500orGoogleError { () =>
      executeGoogleRequest(request)
    } {
      case e: GoogleJsonResponseException if e.getStatusCode == 403 &&
        (e.getDetails.getErrors.get(0).getReason == "accessNotConfigured") => throw DataprocDisabledException(e.getMessage)
    }.map { op =>
      Operation(OperationName(op.getName), getOperationUUID(op))
    }.handleGoogleException(googleProject, Some(clusterName.value))

  }

  override def deleteCluster(googleProject: GoogleProject, clusterName: ClusterName): Future[Unit] = {
    val request = dataproc.projects().regions().clusters().delete(googleProject.value, defaultRegion, clusterName.value)
    retryWithRecoverWhen500orGoogleError { () =>
      executeGoogleRequest(request)
      ()
    } {
      case e: HttpResponseException if e.getStatusCode == StatusCodes.NotFound.intValue => ()
      case e: GoogleJsonResponseException if e.getStatusCode == StatusCodes.BadRequest.intValue &&
        e.getDetails.getMessage.contains("pending delete") => ()
    }.handleGoogleException(googleProject, clusterName)
  }

  override def getClusterStatus(googleProject: GoogleProject, clusterName: ClusterName): Future[ClusterStatus] = {
    val transformed = for {
      cluster <- OptionT(getCluster(googleProject, clusterName))
      status <- OptionT.pure[Future, ClusterStatus](
        Try(ClusterStatus.withNameInsensitive(cluster.getStatus.getState)).toOption.getOrElse(ClusterStatus.Unknown))
    } yield status

    transformed.value.map(_.getOrElse(ClusterStatus.Deleted)).handleGoogleException(googleProject, clusterName)
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
    transformed.value.map(_.getOrElse(List.empty)).handleGoogleException(googleProject)
  }

  override def getClusterMasterInstance(googleProject: GoogleProject, clusterName: ClusterName): Future[Option[InstanceKey]] = {
    val transformed = for {
      cluster <- OptionT(getCluster(googleProject, clusterName))
      masterInstanceName <- OptionT.fromOption[Future] { getMasterInstanceName(cluster) }
      masterInstanceZone <- OptionT.fromOption[Future] { getZone(cluster) }
    } yield InstanceKey(googleProject, masterInstanceZone, masterInstanceName)

    transformed.value.handleGoogleException(googleProject, clusterName)
  }

  override def getClusterInstances(googleProject: GoogleProject, clusterName: ClusterName): Future[Map[DataprocRole, Set[InstanceKey]]] = {
    val transformed = for {
      cluster <- OptionT(getCluster(googleProject, clusterName))
      instanceNames <- OptionT.fromOption[Future] { getAllInstanceNames(cluster) }
      clusterZone <- OptionT.fromOption[Future] { getZone(cluster) }
    } yield {
      instanceNames.mapValues(_.map(name => InstanceKey(googleProject, clusterZone, name)))
    }

    transformed.value.map(_.getOrElse(Map.empty)).handleGoogleException(googleProject, clusterName)
  }

  override def getClusterStagingBucket(googleProject: GoogleProject, clusterName: ClusterName): Future[Option[GcsBucketName]] = {
    // If an expression might be null, need to use `OptionT.fromOption(Option(expr))`.
    // `OptionT.pure(expr)` throws a NPE!
    val transformed = for {
      cluster <- OptionT(getCluster(googleProject, clusterName))
      config <- OptionT.fromOption[Future](Option(cluster.getConfig))
      bucket <- OptionT.fromOption[Future](Option(config.getConfigBucket))
    } yield GcsBucketName(bucket)

    transformed.value.handleGoogleException(googleProject, clusterName)
  }

  override def getClusterErrorDetails(operationName: Option[OperationName]): Future[Option[ClusterErrorDetails]] = {
    val errorOpt: OptionT[Future, ClusterErrorDetails] = for {
      operationName <- OptionT.fromOption[Future](operationName)
      operation <- OptionT(getOperation(operationName)) if operation.getDone
      error <- OptionT.fromOption[Future] { Option(operation.getError) }
      code <- OptionT.fromOption[Future] { Option(error.getCode) }
    } yield ClusterErrorDetails(code, Option(error.getMessage))

    errorOpt.value.handleGoogleException(GoogleProject(""), operationName.map(_.value))
  }

  override def resizeCluster(googleProject: GoogleProject, clusterName: ClusterName, numWorkers: Option[Int] = None, numPreemptibles: Option[Int] = None): Future[Unit] = {
    val workerMask = "config.worker_config.num_instances"
    val preemptibleMask = "config.secondary_worker_config.num_instances"

    val updateAndMask = (numWorkers, numPreemptibles) match {
      case (Some(nw), Some(np)) =>
        val mask = List(Some(workerMask), Some(preemptibleMask)).flatten.mkString(",")
        val update = new DataprocCluster().setConfig(new DataprocClusterConfig()
          .setWorkerConfig(new InstanceGroupConfig().setNumInstances(nw))
          .setSecondaryWorkerConfig(new InstanceGroupConfig().setNumInstances(np)))
        Some((update, mask))

      case (Some(nw), None) =>
        val mask = workerMask
        val update = new DataprocCluster().setConfig(new DataprocClusterConfig()
          .setWorkerConfig(new InstanceGroupConfig().setNumInstances(nw)))
        Some((update, mask))

      case (None, Some(np)) =>
        val mask = preemptibleMask
        val update = new DataprocCluster().setConfig(new DataprocClusterConfig()
          .setSecondaryWorkerConfig(new InstanceGroupConfig().setNumInstances(np)))
        Some((update, mask))

      case (None, None) =>
        None
    }

    updateAndMask match {
      case Some((update, mask)) =>
        val request = dataproc.projects().regions().clusters().patch(googleProject.value, defaultRegion, clusterName.value, update).setUpdateMask(mask)
        retryWhen500orGoogleError(() => executeGoogleRequest(request)).void
      case None => Future.successful(())
    }
  }

  override def getUserInfoAndExpirationFromAccessToken(accessToken: String): Future[(UserInfo, Instant)] = {
    val request = oauth2.tokeninfo().setAccessToken(accessToken)
    retryWhen500orGoogleError(() => executeGoogleRequest(request)).map { tokenInfo =>
      (UserInfo(OAuth2BearerToken(accessToken), WorkbenchUserId(tokenInfo.getUserId), WorkbenchEmail(tokenInfo.getEmail), tokenInfo.getExpiresIn.toInt), Instant.now().plusSeconds(tokenInfo.getExpiresIn.toInt))
    } recover {
        case e: GoogleJsonResponseException =>
          val msg = s"Call to Google OAuth API failed. Status: ${e.getStatusCode}. Message: ${e.getDetails.getMessage}"
          logger.error(msg, e)
          throw new WorkbenchException(msg, e)
        // Google throws IllegalArgumentException when passed an invalid token. Handle this case and rethrow a 401.
        case e: IllegalArgumentException =>
          throw AuthorizationError()
      }
  }

  private def getClusterConfig(machineConfig: MachineConfig,
                               initScript: GcsPath,
                               clusterServiceAccount: Option[WorkbenchEmail],
                               credentialsFileName: Option[String],
                               stagingBucket: GcsBucketName,
                               serviceAccountScopes: Seq[String]): DataprocClusterConfig = {
    // Create a GceClusterConfig, which has the common config settings for resources of Google Compute Engine cluster instances,
    // applicable to all instances in the cluster.
    // Set the network tag, network, and subnet. This allows the created GCE instances to be exposed by Leo's firewall rule.
    val gceClusterConfig = {
      val baseConfig = new GceClusterConfig()
        .setTags(List(networkTag.value).asJava)

      (vpcNetwork, vpcSubnet) match {
        case (_, Some(subnet)) =>
          baseConfig.setSubnetworkUri(subnet.value)
        case (Some(network), _) =>
          baseConfig.setNetworkUri(network.value)
        case _ =>
          baseConfig
      }
    }

    // Set the cluster service account, if present.
    // This is the service account passed to the create cluster API call.
    clusterServiceAccount.foreach { serviceAccountEmail =>
      gceClusterConfig.setServiceAccount(serviceAccountEmail.value).setServiceAccountScopes(serviceAccountScopes.asJava)
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
      .setConfigBucket(stagingBucket.value)
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

  private def getAllInstanceNames(cluster: DataprocCluster): Option[Map[DataprocRole, Set[InstanceName]]] = {
    def getFromGroup(key: DataprocRole)(group: InstanceGroupConfig): Option[Map[DataprocRole, Set[InstanceName]]] = {
      Option(group.getInstanceNames).map(_.asScala.toSet.map(InstanceName)).map(ins => Map(key -> ins))
    }

    Option(cluster.getConfig).flatMap { config =>
      val masters = Option(config.getMasterConfig).flatMap(getFromGroup(Master))
      val workers = Option(config.getWorkerConfig).flatMap(getFromGroup(Worker))
      val secondaryWorkers = Option(config.getSecondaryWorkerConfig).flatMap(getFromGroup(SecondaryWorker))

      masters |+| workers |+| secondaryWorkers
    }
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

  private implicit class GoogleExceptionSupport[A](future: Future[A]) {
    def handleGoogleException(project: GoogleProject, context: Option[String] = None): Future[A] = {
      future.recover {
        case e: GoogleJsonResponseException =>
          val msg = s"Call to Google API failed for ${project.value} ${context.map(c => s"/ $c").getOrElse("")}. Status: ${e.getStatusCode}. Message: ${e.getDetails.getMessage}"
          logger.error(msg, e)
          throw new WorkbenchException(msg, e)
        case e: IllegalArgumentException =>
          val msg = s"Illegal argument passed to Google request for ${project.value} ${context.map(c => s"/ $c").getOrElse("")}. Message: ${e.getMessage}"
          logger.error(msg, e)
          throw new WorkbenchException(msg, e)
      }
    }

    def handleGoogleException(project: GoogleProject, clusterName: ClusterName): Future[A] = {
      handleGoogleException(project, Some(clusterName.value))
    }
  }
}
