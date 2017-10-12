package org.broadinstitute.dsde.workbench.leonardo.dao

import java.io.{ByteArrayInputStream, File}
import java.nio.charset.StandardCharsets
import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
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
import com.google.api.services.compute.model.Firewall.Allowed
import com.google.api.services.compute.model.{Firewall, Instance}
import com.google.api.services.compute.{Compute, ComputeScopes}
import com.google.api.services.dataproc.Dataproc
import com.google.api.services.dataproc.model.{Cluster => DataprocCluster, Operation => DataprocOperation, _}
import com.google.api.services.plus.PlusScopes
import com.google.api.services.storage.model.Bucket.Lifecycle
import com.google.api.services.storage.model.Bucket.Lifecycle.Rule.{Action, Condition}
import com.google.api.services.storage.model.{Bucket, StorageObject}
import com.google.api.services.storage.{Storage, StorageScopes}
import org.broadinstitute.dsde.workbench.google.GoogleUtilities
import org.broadinstitute.dsde.workbench.google.gcs.{GcsBucketName, GcsPath, GcsRelativePath}
import org.broadinstitute.dsde.workbench.leonardo.config.{ClusterResourcesConfig, DataprocConfig, ProxyConfig}
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterStatus.{ClusterStatus => LeoClusterStatus}
import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster => LeoCluster, ClusterErrorDetails, ClusterName, ClusterRequest, FirewallRuleName, GoogleProject, IP, InstanceName, LeoException, OperationName, ZoneUri, ClusterStatus => LeoClusterStatus}
import org.broadinstitute.dsde.workbench.metrics.GoogleInstrumentedService

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future, blocking}


case class CallToGoogleApiFailedException(googleProject: GoogleProject, context: String, exceptionStatusCode: Int, errorMessage: String)
  extends LeoException(s"Call to Google API failed for ${googleProject.string} / $context. Message: $errorMessage", exceptionStatusCode)

case class FirewallRuleNotFoundException(googleProject: GoogleProject, firewallRuleName: FirewallRuleName)
  extends LeoException(s"Firewall rule ${firewallRuleName.string} not found in project ${googleProject.string}", StatusCodes.NotFound)

class GoogleDataprocDAO(protected val dataprocConfig: DataprocConfig, protected val proxyConfig: ProxyConfig, protected val clusterResourcesConfig: ClusterResourcesConfig)(implicit val system: ActorSystem, val executionContext: ExecutionContext)
  extends DataprocDAO with GoogleUtilities {

  // TODO pass as constructor arg when we add metrics
  override protected val workbenchMetricBaseName: String = ""
  implicit val service = GoogleInstrumentedService.Dataproc

  private val httpTransport = GoogleNetHttpTransport.newTrustedTransport
  private val jsonFactory = JacksonFactory.getDefaultInstance
  private lazy val cloudPlatformScopes = List(ComputeScopes.CLOUD_PLATFORM)
  private lazy val storageScopes = List(StorageScopes.DEVSTORAGE_FULL_CONTROL, ComputeScopes.COMPUTE, PlusScopes.USERINFO_EMAIL, PlusScopes.USERINFO_PROFILE)
  private lazy val vmScopes = List(ComputeScopes.COMPUTE, ComputeScopes.CLOUD_PLATFORM)
  private lazy val serviceAccountPemFile = new File(clusterResourcesConfig.configFolderPath, clusterResourcesConfig.leonardoServicePem)

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

  private def getServiceAccountCredential(scopes: List[String]): Credential = {
    new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountId(dataprocConfig.serviceAccount.string)
      .setServiceAccountScopes(scopes.asJava)
      .setServiceAccountPrivateKeyFromPemFile(serviceAccountPemFile)
      .build()
  }

  private def getOperationUUID(dop: DataprocOperation): UUID = {
    UUID.fromString(dop.getMetadata.get("clusterUuid").toString)
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

  override def createCluster(googleProject: GoogleProject, clusterName: ClusterName, clusterRequest: ClusterRequest, bucketName: GcsBucketName)(implicit executionContext: ExecutionContext): Future[LeoCluster] = {
    buildCluster(googleProject, clusterName, clusterRequest, bucketName).map { operation =>
      //Make a Leo cluster from the Google operation details
      LeoCluster.create(clusterRequest, clusterName, googleProject, getOperationUUID(operation), OperationName(operation.getName))
    }
  }


  /* Kicks off building the cluster. This will return before the cluster finishes creating. */
  private def buildCluster(googleProject: GoogleProject, clusterName: ClusterName, clusterRequest: ClusterRequest, bucketName: GcsBucketName)(implicit executionContext: ExecutionContext): Future[DataprocOperation] = {
    // Create a GceClusterConfig, which has the common config settings for resources of Google Compute Engine cluster instances,
    //   applicable to all instances in the cluster. Give it the user's service account.
    //   Set the network tag, which is needed by the firewall rule that allows leo to talk to the cluster
    val gce = new GceClusterConfig()
      .setServiceAccount(clusterRequest.serviceAccount.string)
      .setTags(List(proxyConfig.networkTag).asJava)

    // Create a NodeInitializationAction, which specifies the executable to run on a node.
    //    This executable is our init-actions.sh, which will stand up our jupyter server and proxy.
    val initActions = Seq(new NodeInitializationAction().setExecutableFile(GcsPath(bucketName, GcsRelativePath(clusterResourcesConfig.initActionsScript)).toUri))

    // Create a SoftwareConfig and set a property that makes the cluster have only one node
    val softwareConfig = new SoftwareConfig().setProperties(Map("dataproc:dataproc.allow.zero.workers" -> "true").asJava)

    // Create a Cluster Config and give it the GceClusterConfig, the NodeInitializationAction and the SoftwareConfig
    val clusterConfig = new ClusterConfig()
      .setGceClusterConfig(gce)
      .setInitializationActions(initActions.asJava).setSoftwareConfig(softwareConfig)

    // Create a Cluster and give it a name and a Cluster Config
    val cluster = new DataprocCluster()
      .setClusterName(clusterName.string)
      .setConfig(clusterConfig)

    // Create a dataproc create request and give it the google project, a zone, and the Cluster
    val request = dataproc.projects().regions().clusters().create(googleProject.string, dataprocConfig.dataprocDefaultRegion, cluster)

    executeGoogleRequestAsync(googleProject, clusterName.toString, request)  // returns a Future[DataprocOperation]
  }

  /* Check if the given google project has a cluster firewall rule. If not, add the rule to the project*/
  override def updateFirewallRule(googleProject: GoogleProject): Future[Unit] = {
    checkFirewallRule(googleProject, FirewallRuleName(proxyConfig.firewallRuleName)).recoverWith {
      case _: FirewallRuleNotFoundException => addFirewallRule(googleProject)
    }
  }

  private def checkFirewallRule(googleProject: GoogleProject, firewallRuleName: FirewallRuleName): Future[Unit] = {
    val request = compute.firewalls().get(googleProject.string, firewallRuleName.string)
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
    val request = compute.firewalls().insert(googleProject.string, googleFirewallRule)
    executeGoogleRequestAsync(googleProject, proxyConfig.firewallRuleName, request).void
  }

  /* Create a bucket in the given google project for the initialization files when creating a cluster */
  override def createBucket(googleProject: GoogleProject, bucketName: GcsBucketName): Future[GcsBucketName] = {
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

    // Create a bucket object and set it's name and lifecycle
    val bucket = new Bucket().setName(bucketName.name).setLifecycle(lifecycle)

    val bucketInserter = storage.buckets().insert(googleProject.string, bucket)

    executeGoogleRequestAsync(googleProject, "Bucket " + bucketName.toString, bucketInserter) map { _ => bucketName }
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

  /* Delete a cluster within the google project */
  override def deleteCluster(googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Unit] = {
    val request = dataproc.projects().regions().clusters().delete(googleProject.string, dataprocConfig.dataprocDefaultRegion, clusterName.string)
    executeGoogleRequestAsync(googleProject, clusterName.toString, request).recover {
      // treat a 404 error as a successful deletion
      case CallToGoogleApiFailedException(_, _, 404, _) => ()
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

  /**
    * Gets a dataproc Cluster from the API.
    */
  private def getCluster(googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[DataprocCluster] = {
    val request = dataproc.projects().regions().clusters().get(googleProject.string, dataprocConfig.dataprocDefaultRegion, clusterName.string)
    executeGoogleRequestAsync(googleProject, clusterName.toString, request)
  }

  /**
    * Gets a compute Instance from the API.
    */
  private def getInstance(googleProject: GoogleProject, zone: ZoneUri, instanceName: InstanceName)(implicit executionContext: ExecutionContext): Future[Instance] = {
    val request = compute.instances().get(googleProject.string, zone.string, instanceName.string)
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
        logger.error(s"Error occurred executing Google request for ${googleProject.string} / $context", e)
        throw CallToGoogleApiFailedException(googleProject, context, e.getStatusCode, e.getDetails.getMessage)
    }
  }
}
