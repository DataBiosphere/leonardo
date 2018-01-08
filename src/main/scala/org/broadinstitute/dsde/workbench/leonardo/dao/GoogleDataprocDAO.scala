package org.broadinstitute.dsde.workbench.leonardo.dao

import java.io.{ByteArrayInputStream, File}
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import cats.data.OptionT
import cats.instances.future._
import cats.syntax.functor._
import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.googleapis.services.AbstractGoogleClientRequest
import com.google.api.client.http.{AbstractInputStreamContent, FileContent, InputStreamContent}
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.cloudresourcemanager.{CloudResourceManager, CloudResourceManagerScopes}
import com.google.api.services.compute.model.Firewall.Allowed
import com.google.api.services.compute.model.{Firewall, Instance}
import com.google.api.services.compute.{Compute, ComputeScopes}
import com.google.api.services.dataproc.Dataproc
import com.google.api.services.dataproc.model.{NodeInitializationAction, Cluster => DataprocCluster, Operation => DataprocOperation, _}
import com.google.api.services.oauth2.Oauth2Scopes
import com.google.api.services.oauth2.Oauth2.Builder
import com.google.api.services.plus.PlusScopes
import com.google.api.services.storage.model.Bucket.Lifecycle
import com.google.api.services.storage.model.Bucket.Lifecycle.Rule.{Action, Condition}
import com.google.api.services.storage.model.{Bucket, BucketAccessControl, ObjectAccessControl, StorageObject}
import com.google.api.services.storage.{Storage, StorageScopes}
import org.broadinstitute.dsde.workbench.google.GoogleUtilities
import org.broadinstitute.dsde.workbench.google.gcs.{GcsBucketName, GcsPath, GcsRelativePath}
import org.broadinstitute.dsde.workbench.leonardo.config.{ClusterDefaultsConfig, ClusterFilesConfig, ClusterResourcesConfig, DataprocConfig, ProxyConfig}
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterStatus.{ClusterStatus => LeoClusterStatus}
import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterErrorDetails, ClusterInitValues, ClusterName, ClusterRequest, FirewallRuleName, IP, InstanceName, LeoException, MachineConfig, OperationName, ServiceAccountInfo, ServiceAccountProvider, ZoneUri, Cluster => LeoCluster, ClusterStatus => LeoClusterStatus}
import org.broadinstitute.dsde.workbench.leonardo.service.AuthorizationError
import org.broadinstitute.dsde.workbench.metrics.GoogleInstrumentedService
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail, WorkbenchUserId}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future, blocking}


case class CallToGoogleApiFailedException(googleProject: GoogleProject, context: String, exceptionStatusCode: Int, errorMessage: String)
  extends LeoException(s"Call to Google API failed for ${googleProject.value} / $context. Message: $errorMessage", exceptionStatusCode)

case class FirewallRuleNotFoundException(googleProject: GoogleProject, firewallRuleName: FirewallRuleName)
  extends LeoException(s"Firewall rule ${firewallRuleName.string} not found in project ${googleProject.value}", StatusCodes.NotFound)

case class GoogleProjectNotFoundException(googleProject: GoogleProject)
  extends LeoException(s"Google project ${googleProject.value} not found", StatusCodes.NotFound)

class GoogleDataprocDAO(protected val leoServiceAccountEmail: WorkbenchEmail,
                        protected val leoServiceAccountPemFile: File,
                        protected val dataprocConfig: DataprocConfig,
                        protected val proxyConfig: ProxyConfig,
                        protected val clusterDefaultsConfig: ClusterDefaultsConfig,
                        protected val clusterFilesConfig: ClusterFilesConfig,
                        protected val clusterResourcesConfig: ClusterResourcesConfig)
                       (implicit val system: ActorSystem, val executionContext: ExecutionContext)
  extends DataprocDAO with GoogleUtilities {

  // TODO pass as constructor arg when we add metrics
  override protected val workbenchMetricBaseName: String = ""
  implicit val service = GoogleInstrumentedService.Dataproc

  private val httpTransport = GoogleNetHttpTransport.newTrustedTransport
  private val jsonFactory = JacksonFactory.getDefaultInstance
  private lazy val cloudPlatformScopes = List(ComputeScopes.CLOUD_PLATFORM)
  private lazy val storageScopes = List(StorageScopes.DEVSTORAGE_FULL_CONTROL, ComputeScopes.COMPUTE, PlusScopes.USERINFO_EMAIL, PlusScopes.USERINFO_PROFILE)
  private lazy val vmScopes = List(ComputeScopes.COMPUTE, ComputeScopes.CLOUD_PLATFORM)
  private lazy val oauth2Scopes = List(Oauth2Scopes.USERINFO_EMAIL, Oauth2Scopes.USERINFO_PROFILE)
  private lazy val cloudResourceManagerScopes = List(CloudResourceManagerScopes.CLOUD_PLATFORM)

  private lazy val oauth2 =
    new Builder(httpTransport, jsonFactory, null)
      .setApplicationName(dataprocConfig.applicationName).build()

  private lazy val dataproc = {
    new Dataproc.Builder(httpTransport, jsonFactory, getServiceAccountCredential(cloudPlatformScopes))
      .setApplicationName(dataprocConfig.applicationName).build()
  }

  private lazy val storage = {
    new Storage.Builder(httpTransport, jsonFactory, getServiceAccountCredential(storageScopes))
      .setApplicationName(dataprocConfig.applicationName).build()
  }

  private lazy val compute = {
    new Compute.Builder(httpTransport,jsonFactory,getServiceAccountCredential(vmScopes))
      .setApplicationName(dataprocConfig.applicationName).build()
  }

  private lazy val cloudResourceManager = {
    new CloudResourceManager.Builder(httpTransport, jsonFactory, getServiceAccountCredential(cloudResourceManagerScopes))
      .setApplicationName(dataprocConfig.applicationName).build()
  }

  private def getServiceAccountCredential(scopes: List[String]): Credential = {
    new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountId(leoServiceAccountEmail.value)
      .setServiceAccountScopes(scopes.asJava)
      .setServiceAccountPrivateKeyFromPemFile(leoServiceAccountPemFile)
      .build()
  }

  private def getOperationUUID(dop: DataprocOperation): UUID = {
    UUID.fromString(dop.getMetadata.get("clusterUuid").toString)
  }

  // Using the given access token, look up the corresponding UserInfo of the user
  def getUserInfoAndExpirationFromAccessToken(accessToken: String)(implicit executionContext: ExecutionContext): Future[(UserInfo, Instant)] = {
    val request = oauth2.tokeninfo().setAccessToken(accessToken)
    executeGoogleRequestAsync(GoogleProject(""), "cookie auth", request).map { tokenInfo =>
      (UserInfo(OAuth2BearerToken(accessToken), WorkbenchUserId(tokenInfo.getUserId), WorkbenchEmail(tokenInfo.getEmail), tokenInfo.getExpiresIn.toInt), Instant.now().plusSeconds(tokenInfo.getExpiresIn.toInt))
    }.recover { case CallToGoogleApiFailedException(_, _, _, _) => {
      logger.error(s"Unable to authorize token: $accessToken")
      throw AuthorizationError()
    }}
  }

  private lazy val googleFirewallRule = {
    // Create an Allowed object that specifies the port and protocol of rule
    val allowed = new Allowed().setIPProtocol(proxyConfig.jupyterProtocol).setPorts(List(proxyConfig.jupyterPort.toString).asJava)
    // Create a firewall rule that takes an Allowed object, a name, and the network tag a cluster must have to be accessed through the rule
    new Firewall()
      .setName(proxyConfig.firewallRuleName)
      .setTargetTags(List(proxyConfig.networkTag).asJava)
      .setAllowed(List(allowed).asJava)
  }

  override def createCluster(userEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName, clusterRequest: ClusterRequest, initBucketName: GcsBucketName, serviceAccountInfo: ServiceAccountInfo)(implicit executionContext: ExecutionContext): Future[LeoCluster] = {
    buildCluster(googleProject, clusterName, clusterRequest, initBucketName, clusterDefaultsConfig, serviceAccountInfo).map { operation =>
      //Make a Leo cluster from the Google operation details
      LeoCluster.create(clusterRequest, userEmail, clusterName, googleProject, getOperationUUID(operation), OperationName(operation.getName), serviceAccountInfo, clusterDefaultsConfig)
    }
  }


  /* Kicks off building the cluster. This will return before the cluster finishes creating. */
  private def buildCluster(googleProject: GoogleProject, clusterName: ClusterName, clusterRequest: ClusterRequest, initBucketName: GcsBucketName, clusterDefaultsConfig: ClusterDefaultsConfig, serviceAccountInfo: ServiceAccountInfo)(implicit executionContext: ExecutionContext): Future[DataprocOperation] = {
    // Create a Cluster and give it a name and a Cluster Config
    val cluster = new DataprocCluster()
      .setClusterName(clusterName.string)
      .setConfig(getClusterConfig(googleProject, clusterName, clusterRequest, initBucketName, clusterDefaultsConfig, serviceAccountInfo))

    // Create a dataproc create request and give it the google project, a zone, and the Cluster
    val request = dataproc.projects().regions().clusters().create(googleProject.value, dataprocConfig.dataprocDefaultRegion, cluster)

    executeGoogleRequestAsync(googleProject, clusterName.toString, request)  // returns a Future[DataprocOperation]
  }

  private def getClusterConfig(googleProject: GoogleProject, clusterName: ClusterName, clusterRequest: ClusterRequest, initBucketName: GcsBucketName, clusterDefaultsConfig: ClusterDefaultsConfig, serviceAccountInfo: ServiceAccountInfo): ClusterConfig = {
    // Create a GceClusterConfig, which has the common config settings for resources of Google Compute Engine cluster instances,
    // applicable to all instances in the cluster.
    // Set the network tag, which is needed by the firewall rule that allows leo to talk to the cluster
    val gceClusterConfig = new GceClusterConfig().setTags(List(proxyConfig.networkTag).asJava)

    // Set the cluster service account, if present.
    // This is the service account passed to the create cluster API call.
    serviceAccountInfo.clusterServiceAccount.foreach { serviceAccountEmail =>
      gceClusterConfig.setServiceAccount(serviceAccountEmail.value).setServiceAccountScopes(oauth2Scopes.asJava)
    }

    // Create a NodeInitializationAction, which specifies the executable to run on a node.
    // This executable is our init-actions.sh, which will stand up our jupyter server and proxy.
    val initActions = Seq(new NodeInitializationAction().setExecutableFile(GcsPath(initBucketName, GcsRelativePath(clusterResourcesConfig.initActionsScript.string)).toUri))

    val machineConfig = MachineConfig(clusterRequest.machineConfig, clusterDefaultsConfig)

    // Create a config for the master node, if properties are not specified in request, use defaults
    val masterConfig = new InstanceGroupConfig()
      .setMachineTypeUri(machineConfig.masterMachineType.get)
      .setDiskConfig(new DiskConfig().setBootDiskSizeGb(machineConfig.masterDiskSize.get))

    // Create a Cluster Config and give it the GceClusterConfig, the NodeInitializationAction and the InstanceGroupConfig
    createClusterConfig(machineConfig, serviceAccountInfo)
      .setGceClusterConfig(gceClusterConfig)
      .setInitializationActions(initActions.asJava)
      .setMasterConfig(masterConfig)
  }

  // Expects a Machine Config with master configs defined for a 0 worker cluster and both master and worker
  // configs defined for 2 or more workers.
  private def createClusterConfig(machineConfig: MachineConfig, serviceAccountInfo: ServiceAccountInfo): ClusterConfig = {

    val swConfig: SoftwareConfig = getSoftwareConfig(machineConfig.numberOfWorkers, serviceAccountInfo)

    // If the number of workers is zero, make a Single Node cluster, else make a Standard one
    if (machineConfig.numberOfWorkers.get == 0) {
      new ClusterConfig().setSoftwareConfig(swConfig)
    }
    else // Standard, multi node cluster
      getMultiNodeClusterConfig(machineConfig).setSoftwareConfig(swConfig)
  }

  private def getSoftwareConfig(numWorkers: Option[Int], serviceAccountInfo: ServiceAccountInfo) = {
    val authProps: Map[String, String] = serviceAccountInfo.notebookServiceAccount match {
      case None =>
        // If we're not using a notebook service account, no need to set Hadoop properties since
        // the SA credentials are on the metadata server.
        Map.empty

      case Some(_) =>
        // If we are using a notebook service account, set the necessary Hadoop properties
        // to specify the location of the notebook service account key file.
        Map(
          "core:google.cloud.auth.service.account.enable" -> "true",
          "core:google.cloud.auth.service.account.json.keyfile" -> s"/etc/${ClusterInitValues.serviceAccountCredentialsFilename}"
        )
    }

    val workerProps: Map[String, String] = if (numWorkers.get == 0) {
      // Set a SoftwareConfig property that makes the cluster have only one node
      Map("dataproc:dataproc.allow.zero.workers" -> "true")
    }
    else
      Map.empty

    new SoftwareConfig().setProperties((authProps ++ workerProps).asJava)
  }

  private def getMultiNodeClusterConfig(machineConfig: MachineConfig): ClusterConfig = {
    // Set the configs of the non-preemptible, primary worker nodes
    val clusterConfigWithWorkerConfigs = new ClusterConfig().setWorkerConfig(getPrimaryWorkerConfig(machineConfig))

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

  /* Check if the given google project has a cluster firewall rule. If not, add the rule to the project*/
  override def updateFirewallRule(googleProject: GoogleProject): Future[Unit] = {
    checkFirewallRule(googleProject, FirewallRuleName(proxyConfig.firewallRuleName)).recoverWith {
      case _: FirewallRuleNotFoundException => addFirewallRule(googleProject)
    }
  }

  private def checkFirewallRule(googleProject: GoogleProject, firewallRuleName: FirewallRuleName): Future[Unit] = {
    val request = compute.firewalls().get(googleProject.value, firewallRuleName.string)
    executeGoogleRequestAsync(googleProject, firewallRuleName.toString, request).recover {
      case CallToGoogleApiFailedException(_, _, 404, _) =>
        // throw FirewallRuleNotFoundException in case of 404 errors
        throw FirewallRuleNotFoundException(googleProject, firewallRuleName)
    }.void
  }

  /* Adds a firewall rule in the given google project. This firewall rule allows ingress traffic through a specified port for all
     VMs with the network tag "leonardo". This rule should only be added once per project.
    To think about: do we want to remove this rule if a google project no longer has any clusters? */
  private def addFirewallRule(googleProject: GoogleProject): Future[Unit] = {
    val request = compute.firewalls().insert(googleProject.value, googleFirewallRule)
    executeGoogleRequestAsync(googleProject, proxyConfig.firewallRuleName, request).void
  }

  /* Create a bucket in the given google project for the initialization files when creating a cluster */
  override def createBucket(bucketGoogleProject: GoogleProject, clusterGoogleProject: GoogleProject, initBucketName: GcsBucketName, serviceAccountInfo: ServiceAccountInfo): Future[GcsBucketName] = {
    // Create lifecycle rule for the bucket that will delete the bucket after 1 day.
    //
    // Note that the init buckets are explicitly deleted by the ClusterMonitor once the cluster
    // initializes. However we still keep the lifecycle rule as a defensive check to ensure we
    // don't leak buckets in case something goes wrong.
    val lifecycleRule = new Lifecycle.Rule()
      .setAction(new Action().setType("Delete"))
      .setCondition(new Condition().setAge(1))

    // Create lifecycle for the bucket with a list of rules
    val lifecycle = new Lifecycle().setRule(List(lifecycleRule).asJava)

    // The Leo service account
    val leoServiceAccountEntityString = s"user-${leoServiceAccountEmail.value}"

    val clusterServiceAccountEntityStringFuture: Future[String] = serviceAccountInfo.clusterServiceAccount match {
      case Some(serviceAccountEmail) =>
        // If passing a service account to the create cluster command, grant bucket access to that service account.
        Future.successful(s"user-${serviceAccountEmail.value}")
      case None =>
        // Otherwise, grant bucket access to the Google compute engine default service account.
        getComputeEngineDefaultServiceAccount(clusterGoogleProject).map {
          case Some(serviceAccount) => s"user-${serviceAccount.value}"
          case None => throw GoogleProjectNotFoundException(clusterGoogleProject)
        }
    }

    clusterServiceAccountEntityStringFuture.flatMap { clusterServiceAccountEntityString =>
      //Add the Leo SA and the cluster's SA to the ACL list for the bucket
      val bucketAcls = List(
        new BucketAccessControl().setEntity(leoServiceAccountEntityString).setRole("OWNER"),
        new BucketAccessControl().setEntity(clusterServiceAccountEntityString).setRole("READER")
      )

      //Bucket ACL != the ACL given to individual objects inside the bucket
      val defObjectAcls = List(
        new ObjectAccessControl().setEntity(leoServiceAccountEntityString).setRole("OWNER"),
        new ObjectAccessControl().setEntity(clusterServiceAccountEntityString).setRole("READER")
      )

      // Create the bucket object
      val bucket = new Bucket()
        .setName(initBucketName.name)
        .setLifecycle(lifecycle)
        .setAcl(bucketAcls.asJava)
        .setDefaultObjectAcl(defObjectAcls.asJava)

      val bucketInserter = storage.buckets().insert(bucketGoogleProject.value, bucket)

      executeGoogleRequestAsync(bucketGoogleProject, s"Bucket ${initBucketName.toString}", bucketInserter) map { _ => initBucketName }
    }
  }

  override def deleteBucket(googleProject: GoogleProject, bucketName: GcsBucketName)(implicit executionContext: ExecutionContext): Future[Unit] = {
    // Delete all objects in the bucket then delete the bucket itself
    val listObjectsRequest = storage.objects().list(bucketName.name)
    executeGoogleRequestAsync(googleProject, bucketName.name, listObjectsRequest).flatMap { objects =>
      Future.traverse(objects.getItems.asScala) { item =>
        val deleteObjectRequest = storage.objects().delete(bucketName.name, item.getName)
        executeGoogleRequestAsync(googleProject, bucketName.name, deleteObjectRequest)
      }
    } flatMap { _ =>
      val deleteBucketRequest = storage.buckets().delete(bucketName.name)
      executeGoogleRequestAsync(googleProject, bucketName.name, deleteBucketRequest).void
    }
  }

  /* Upload a file to a bucket as FileContent */
  override def uploadToBucket(googleProject: GoogleProject, bucketPath: GcsPath, content: File): Future[Unit] = {
    val fileContent = new FileContent(null, content)
    uploadToBucket(googleProject, bucketPath, fileContent)
  }

  /* Upload a string to a bucket as InputStreamContent */
  override def uploadToBucket(googleProject: GoogleProject, bucketPath: GcsPath, content: String): Future[Unit] = {
    val inputStreamContent = new InputStreamContent(null, new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)))
    uploadToBucket(googleProject, bucketPath, inputStreamContent)
  }

  override def bucketObjectExists(googleProject: GoogleProject, bucketPath: GcsPath): Future[Boolean] = {
    val request = storage.objects().get(bucketPath.bucketName.name, bucketPath.relativePath.name)
    executeGoogleRequestAsync(googleProject, "Bucket Path " + bucketPath.toUri, request).map(_ => true).recover {
      case CallToGoogleApiFailedException(_, _, 404, _) => false
    }
  }

  /* Upload the given content into the given bucket */
  private def uploadToBucket(googleProject: GoogleProject, bucketPath: GcsPath, content: AbstractInputStreamContent): Future[Unit] = {
    // Create a storage object
    val storageObject = new StorageObject().setName(bucketPath.relativePath.name)
    // Create a storage object insertion request
    val fileInserter = storage.objects().insert(bucketPath.bucketName.name, storageObject, content)
    // Enable direct media upload
    fileInserter.getMediaHttpUploader().setDirectUploadEnabled(true)
    executeGoogleRequestAsync(googleProject, "Bucket Path " + bucketPath.toUri, fileInserter).void
  }

  /* set the notebook service account as the staging bucket owner */
  override def setStagingBucketOwnership(cluster: LeoCluster): Future[Unit] = {
    cluster.serviceAccountInfo.notebookServiceAccount match {
      case None =>
        // No need to do this if we're not using a notebook service account
        Future.successful(())

      case Some(serviceAccountEmail) =>
        val entity: String = s"user-${serviceAccountEmail.value}"

        // set owner access for the bucket itself
        def bacInsert(bucket: GcsBucketName) = {
          val acl = new BucketAccessControl().setEntity(entity).setRole("OWNER")
          storage.bucketAccessControls().insert(bucket.name, acl)
        }

        // set default owner access for objects created in the bucket
        def doacInsert(bucket: GcsBucketName) = {
          val acl = new ObjectAccessControl().setEntity(entity).setRole("OWNER")
          storage.defaultObjectAccessControls().insert(bucket.name, acl)
        }

        val transformed: OptionT[Future, Unit] = for {
          dCluster <- OptionT.liftF[Future, DataprocCluster] { getCluster(cluster.googleProject, cluster.clusterName) }
          stagingBucket <- OptionT.fromOption { getStagingBucket(dCluster) }
          _ <- OptionT.liftF[Future, BucketAccessControl] { executeGoogleRequestAsync(cluster.googleProject, s"Bucket ${stagingBucket.name}", bacInsert(stagingBucket)) }
          _ <- OptionT.liftF[Future, ObjectAccessControl] { executeGoogleRequestAsync(cluster.googleProject, s"Bucket ${stagingBucket.name}", doacInsert(stagingBucket)) }
        } yield ()

        transformed.value.void
    }
  }

  /* Delete a cluster within the google project */
  override def deleteCluster(googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Unit] = {
    val request = dataproc.projects().regions().clusters().delete(googleProject.value, dataprocConfig.dataprocDefaultRegion, clusterName.string)
    executeGoogleRequestAsync(googleProject, clusterName.toString, request).recover {
      // treat a 404 error as a successful deletion
      case CallToGoogleApiFailedException(_, _, 404, _) => ()
      case CallToGoogleApiFailedException(_, _, 400, msg) if msg.contains("it has other pending delete operations against it") => ()
    }.void
  }

  override def getClusterStatus(googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[LeoClusterStatus] = {
    getCluster(googleProject, clusterName).map { cluster =>
      LeoClusterStatus.withNameIgnoreCase(cluster.getStatus.getState)
    }
  }

  override def getClusterMasterInstanceIp(googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Option[IP]] = {
    // OptionT is handy when you potentially want to deal with Future[A], Option[A],
    // Future[Option[A]], and A all in the same flatMap!
    //
    // Legend:
    // - OptionT.pure turns an A into an OptionT[F, A]
    // - OptionT.liftF turns an F[A] into an OptionT[F, A]
    // - OptionT.fromOption turns an Option[A] into an OptionT[F, A]

    val ipOpt: OptionT[Future, IP] = for {
      cluster <- OptionT.liftF[Future, DataprocCluster] { getCluster(googleProject, clusterName) }
      masterInstanceName <- OptionT.fromOption { getMasterInstanceName(cluster) }
      masterInstanceZone <- OptionT.fromOption { getZone(cluster) }
      masterInstance <- OptionT.liftF[Future, Instance] { getInstance(googleProject, masterInstanceZone, masterInstanceName) }
      masterInstanceIp <- OptionT.fromOption { getInstanceIP(masterInstance) }
    } yield masterInstanceIp

    // OptionT[Future, String] is simply a case class wrapper for Future[Option[String]].
    // So this just grabs the inner value and returns it.
    ipOpt.value
  }

  override def getClusterErrorDetails(operationName: OperationName)(implicit executionContext: ExecutionContext): Future[Option[ClusterErrorDetails]] = {
    val errorOpt: OptionT[Future, ClusterErrorDetails] = for {
      operation <- OptionT.liftF[Future, DataprocOperation] { getOperation(operationName) } if operation.getDone
      error <- OptionT.pure { operation.getError }
      code <- OptionT.pure { error.getCode }
    } yield ClusterErrorDetails(code, Option(error.getMessage))

    errorOpt.value
  }

  override def listClusters(googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[List[UUID]] = {
    val request = dataproc.projects().regions().clusters().list(googleProject.value, dataprocConfig.dataprocDefaultRegion)
    executeGoogleRequestAsync(googleProject, "", request).map { result =>
      // handle nulls in the Google response
      (for {
        r <- Option(result)
        clusters <- Option(r.getClusters)
      } yield {
        clusters.asScala.toList.map(c => UUID.fromString(c.getClusterUuid))
      }).getOrElse(List.empty)
    }
  }

  /**
    * Gets a dataproc Cluster from the API.
    */
  private def getCluster(googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[DataprocCluster] = {
    val request = dataproc.projects().regions().clusters().get(googleProject.value, dataprocConfig.dataprocDefaultRegion, clusterName.string)
    executeGoogleRequestAsync(googleProject, clusterName.toString, request)
  }

  /**
    * Gets a compute Instance from the API.
    */
  private def getInstance(googleProject: GoogleProject, zone: ZoneUri, instanceName: InstanceName)(implicit executionContext: ExecutionContext): Future[Instance] = {
    val request = compute.instances().get(googleProject.value, zone.string, instanceName.string)
    executeGoogleRequestAsync(googleProject, instanceName.toString, request)
  }

  /**
    * Gets an Operation from the API.
    */
  private def getOperation(operationName: OperationName)(implicit executionContext: ExecutionContext): Future[DataprocOperation] = {
    val request = dataproc.projects().regions().operations().get(operationName.string)
    executeGoogleRequestAsync(GoogleProject("operation"), operationName.toString, request)
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

  /**
    * Gets the staging bucket from a dataproc cluster, with error handling.
    * @param cluster the Google dataproc cluster
    * @return error or staging bucket
    */
  private def getStagingBucket(cluster: DataprocCluster): Option[GcsBucketName] = {
    for {
      config <- Option(cluster.getConfig)
      bucket <- Option(config.getConfigBucket)
    } yield GcsBucketName(bucket)
  }

  private def getInitBucketName(cluster: DataprocCluster): Option[GcsBucketName] = {
    for {
      config <- Option(cluster.getConfig)
      initAction <- config.getInitializationActions.asScala.headOption
      bucketPath <- Option(initAction.getExecutableFile)
      parsedBucketPath <- GcsPath.parse(bucketPath).toOption
    } yield parsedBucketPath.bucketName
  }

  private def executeGoogleRequestAsync[A](googleProject: GoogleProject, context: String, request: AbstractGoogleClientRequest[A])(implicit executionContext: ExecutionContext): Future[A] = {
    Future {
      blocking(executeGoogleRequest(request))
    } recover {
      case e: GoogleJsonResponseException =>
        logger.error(s"Error occurred executing Google request for ${googleProject.value} / $context", e)
        throw CallToGoogleApiFailedException(googleProject, context, e.getStatusCode, e.getDetails.getMessage)
      case illegalArgumentException: IllegalArgumentException =>
        logger.error(s"Illegal argument passed to Google request for ${googleProject.value} / $context", illegalArgumentException)
        throw CallToGoogleApiFailedException(googleProject, context, StatusCodes.BadRequest.intValue, illegalArgumentException.getMessage)
    }
  }

  private def getProjectNumber(googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Option[Long]] = {
    val request = cloudResourceManager.projects().get(googleProject.value)
    executeGoogleRequestAsync(googleProject, "", request).map { project =>
      Option(project.getProjectNumber).map(_.toLong)
    }.recover { case CallToGoogleApiFailedException(_, _, 404, _) =>
      None
    }
  }

  private def getComputeEngineDefaultServiceAccount(googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Option[WorkbenchEmail]] = {
    getProjectNumber(googleProject).map { numberOpt =>
      numberOpt.map { number =>
        // Service account email format documented in:
        // https://cloud.google.com/compute/docs/access/service-accounts#compute_engine_default_service_account
        WorkbenchEmail(s"$number-compute@developer.gserviceaccount.com")
      }
    }
  }
}
