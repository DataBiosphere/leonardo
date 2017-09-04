package org.broadinstitute.dsde.workbench.leonardo.dao

import com.google.api.services.dataproc.model._
import com.google.api.services.dataproc.model.{Cluster => GoogleCluster}
import com.google.api.services.dataproc.model.{Operation => DataprocOperation}
import com.google.api.services.compute.model.{Operation => ComputeOperation}
import org.broadinstitute.dsde.workbench.leonardo.config.{DataprocConfig, ProxyConfig}
import org.broadinstitute.dsde.workbench.google.GoogleUtilities
import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.http.{AbstractInputStreamContent, FileContent, InputStreamContent}
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.compute.ComputeScopes
import com.google.api.services.compute.Compute
import com.google.api.services.compute.model.Firewall
import com.google.api.services.compute.model.Firewall.Allowed
import com.google.api.services.dataproc.Dataproc
import com.google.api.services.plus.PlusScopes
import com.google.api.services.storage.{Storage, StorageScopes}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.LeonardoJsonSupport._
import spray.json._
import com.google.api.services.storage.model.{Bucket, StorageObject}
import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterInitValues, ClusterRequest, ClusterResponse, LeoException}
import java.io.{ByteArrayInputStream, File}
import java.nio.charset.StandardCharsets
import java.util.UUID

import org.broadinstitute.dsde.workbench.leonardo.model.ModelTypes.GoogleProject

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

case class BucketNotCreatedException(googleProject: GoogleProject, clusterName: String) extends LeoException(s"Cluster $googleProject/$clusterName failed to create required resource.", StatusCodes.InternalServerError)
case class BucketInsertionException(googleProject: GoogleProject, clusterName: String) extends LeoException(s"Cluster $googleProject/$clusterName failed to upload Jupyter certificates", StatusCodes.InternalServerError)
case class ClusterNotCreatedException(googleProject: GoogleProject, clusterName: String) extends LeoException(s"Failed to create cluster $googleProject/$clusterName", StatusCodes.InternalServerError)
case class FirewallRuleInaccessibleException(googleProject: GoogleProject, firewallRule: String) extends LeoException(s"Unable to access firewall rule $googleProject/$firewallRule", StatusCodes.InternalServerError)
case class FirewallRuleInsertionException(googleProject: GoogleProject, firewallRule: String) extends LeoException(s"Unable to insert new firewall rule $googleProject/$firewallRule", StatusCodes.InternalServerError)
case class CallToGoogleApiFailedException(googleProject: GoogleProject, clusterName:String, exceptionStatusCode: Int, errorMessage:String) extends LeoException(s"Call to Google API failed for $googleProject/$clusterName. Message: $errorMessage",exceptionStatusCode)

class GoogleDataprocDAO(protected val dataprocConfig: DataprocConfig, protected val proxyConfig: ProxyConfig)(implicit val system: ActorSystem, val executionContext: ExecutionContext)
  extends DataprocDAO with GoogleUtilities {

  private val httpTransport = GoogleNetHttpTransport.newTrustedTransport
  private val jsonFactory = JacksonFactory.getDefaultInstance
  private lazy val cloudPlatformScopes = List(ComputeScopes.CLOUD_PLATFORM)
  private lazy val storageScopes = List(StorageScopes.DEVSTORAGE_FULL_CONTROL, ComputeScopes.COMPUTE, PlusScopes.USERINFO_EMAIL, PlusScopes.USERINFO_PROFILE)
  private lazy val vmScopes = List(ComputeScopes.COMPUTE, ComputeScopes.CLOUD_PLATFORM)
  private lazy val serviceAccountPemFile = new File(dataprocConfig.configFolderPath, dataprocConfig.serviceAccountPemName)

  private lazy val dataproc = {
    new Dataproc.Builder(httpTransport, jsonFactory, getServiceAccountCredential(cloudPlatformScopes))
      .setApplicationName("dataproc").build()
  }

  private lazy val storage = {
    new Storage.Builder(httpTransport, jsonFactory, getServiceAccountCredential(storageScopes))
      .setApplicationName("storage").build()
  }


  private def getServiceAccountCredential(scopes: List[String]): Credential = {
    new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountId(dataprocConfig.serviceAccount)
      .setServiceAccountScopes(scopes.asJava)
      .setServiceAccountPrivateKeyFromPemFile(serviceAccountPemFile)
      .build()
  }

  /* Creates a cluster in the given google project:
       - Add a firewall rule to the google project if it doesn't exist, so we can access the cluster
       - Create the initialization bucket for the cluster
       - Upload all the necessary initialization files to the bucket
       - Create the cluster in the google project
     Currently, the bucketPath of the clusterRequest is not used - it will be used later as a place to store notebook results */
  def createCluster(googleProject: GoogleProject, clusterName: String, clusterRequest: ClusterRequest)(implicit executionContext: ExecutionContext): Future[ClusterResponse] = {
    // Create the firewall rule in the google project if it doesn't already exist, so we can access the cluster
    updateFirewallRules(googleProject)

    val bucketName = s"${clusterName}-${UUID.randomUUID.toString}"
    // Create the bucket and populate with initialization files
    val bucketResponse = initializeBucket(googleProject, clusterName, bucketName)
    // Once the bucket is ready, build the cluster
    bucketResponse.flatMap[ClusterResponse] { _ =>
      val operation = build(googleProject, clusterName, clusterRequest, bucketName)
      // Once the cluster creation request is sent to Google, it returns a DataprocOperation, which we transform into a ClusterResponse
      operation.map { op =>
        val metadata = op.getMetadata
        ClusterResponse(clusterName, googleProject, metadata.get("clusterUuid").toString, metadata.get("status").toString, metadata.get("description").toString, op.getName)
      }
    }
  }

  /* Adds a firewall rule in the given google project if it does not already exist. */
  private def updateFirewallRules(googleProject: String) = {
    val request = new Compute(httpTransport, jsonFactory, getServiceAccountCredential(vmScopes)).firewalls().get(googleProject, dataprocConfig.clusterFirewallRuleName)
    try {
      executeGoogleRequest(request) // returns a Firewall
    } catch {
      case t: GoogleJsonResponseException if t.getStatusCode == 404 => addFirewallRule(googleProject)
      case e: GoogleJsonResponseException => throw new FirewallRuleInaccessibleException(googleProject, dataprocConfig.clusterFirewallRuleName)
    }
  }

  /* Adds a firewall rule in the given google project. This firewall rule allows ingress traffic through a specified port for all
     VMs with the network tag "leonardo". This rule should only be added once per project.
    To think about: do we want to remove this rule if a google project no longer has any clusters? */
  private def addFirewallRule(googleProject: String): Future[ComputeOperation] = {
    Future {
      val allowed = new Allowed().setIPProtocol(proxyConfig.jupyterProtocol).setPorts(List(proxyConfig.jupyterPort.toString).asJava)

      val firewallRule = new Firewall()
        .setName(dataprocConfig.clusterFirewallRuleName)
        .setTargetTags(List("leonardo").asJava) // put this in config
        .setAllowed(List(allowed).asJava)

      val request = new Compute(httpTransport, jsonFactory, getServiceAccountCredential(vmScopes)).firewalls().insert(googleProject, firewallRule)

      try {
        executeGoogleRequest(request) // returns a ComputeOperation
      } catch {
        case e: GoogleJsonResponseException => throw new FirewallRuleInsertionException(googleProject, dataprocConfig.clusterFirewallRuleName)
      }
    }
  }

  /* Create init bucket for the cluster and populate it with the cluster initialization files */
  private def initializeBucket(googleProject: GoogleProject, clusterName: String, bucketName: String) = {
    val bucketResponse = createBucket(googleProject, clusterName, bucketName)
    // Add initialization files after the bucket has been created
    bucketResponse.map { _ =>
      val objectResponse = initializeBucketObjects(googleProject, clusterName, bucketName)
    }
  }

  /* Create a bucket in the given google project for the initialization files when creating a cluster */
  private def createBucket(googleProject: GoogleProject, clusterName: String, bucketName:String): Future[Bucket] = {
    Future {
      val bucket = new Bucket().setName(bucketName)
      val bucketInserter = storage.buckets().insert(googleProject, bucket)
      try {
        executeGoogleRequest(bucketInserter) // returns a Bucket
      } catch {
        case e: GoogleJsonResponseException => throw new BucketNotCreatedException(googleProject, clusterName)
      }
    }
  }

  /* Process the templated cluster init script and put all initialization files in the init bucket */
  private def initializeBucketObjects(googleProject: GoogleProject, clusterName: String, bucketName: String): Future[Array[StorageObject]] = {
    Future {
      // Get the init script
      val initScriptRaw = scala.io.Source.fromFile(dataprocConfig.configFolderPath + dataprocConfig.initActionsScriptName).mkString
      // Get a map of the template values to be replaced
      val replacements = ClusterInitValues(googleProject, clusterName, bucketName, dataprocConfig).toJson.asJsObject.fields
      // Replace templated values in the raw init script with actual values before populating the bucket
      val initScript = replacements.foldLeft(initScriptRaw)((a, b) => a.replaceAllLiterally("$(" + b._1 + ")", b._2.toString()))
      // Create InputStreamContent using the new init script
      val content = new InputStreamContent(null, new ByteArrayInputStream(initScript.getBytes(StandardCharsets.UTF_8)))
      // Put init script in bucket
      val initScriptStorageObject = populateBucket(googleProject, bucketName, dataprocConfig.initActionsScriptName, content)

      // Create an array of all the certs, cluster docker compose file, and the site.conf for the apache proxy
      val certs = Array(dataprocConfig.jupyterServerCrtName, dataprocConfig.jupyterServerKeyName, dataprocConfig.jupyterRootCaPemName,
        dataprocConfig.clusterDockerComposeName, dataprocConfig.jupyterProxySiteConfName)
      // Put the rest of the initialization files in the init bucket
      val storageObjects = certs.map { certName => populateBucket(googleProject, bucketName, certName, new FileContent(null, new File(dataprocConfig.configFolderPath, certName))) }
      // Return an array of all the Storage Objects
      storageObjects :+ initScriptStorageObject
    }
  }

  /* Upload the given file into the given bucket */
  private def populateBucket(googleProject: GoogleProject, bucketName: String, fileName: String, content: AbstractInputStreamContent): StorageObject = {
    // Create a storage object
    val storageObject = new StorageObject().setName(fileName)
    // Create a storage object insertion request
    val fileInserter = storage.objects().insert(bucketName, storageObject, content)
    // Enable direct media upload
    fileInserter.getMediaHttpUploader().setDirectUploadEnabled(true)
    try {
      executeGoogleRequest(fileInserter) //returns a StorageObject
    } catch {
      case e: GoogleJsonResponseException => throw new BucketInsertionException(googleProject, bucketName)
    }
  }

  /* Kicks off building the cluster. This will return before the cluster finishes creating. */
  private def build(googleProject: GoogleProject, clusterName: String, clusterRequest: ClusterRequest, bucketName: String)(implicit executionContext: ExecutionContext): Future[DataprocOperation] = {
    Future {
      // Create a GceClusterConfig, which has the common config settings for resources of Google Compute Engine cluster instances,
      //   applicable to all instances in the cluster. Give it the user's service account.
      //   Set the network tag, which is needed by the firewall rule that allows leo to talk to the cluster
      val gce = new GceClusterConfig()
        .setServiceAccount(clusterRequest.serviceAccount)
        .setTags(List("leonardo").asJava)

      // Create a NodeInitializationAction, which specifies the executable to run on a node.
      //    This executable is our init-actions.sh, which will stand up our jupyter server and proxy.
      val initActions = Seq(new NodeInitializationAction().setExecutableFile(GoogleBucketUri(bucketName, dataprocConfig.initActionsScriptName)))

      // Create a SoftwareConfig and set a property that makes the cluster have only one node
      val softwareConfig = new SoftwareConfig().setProperties(Map("dataproc:dataproc.allow.zero.workers" -> "true").asJava)

      // Create a Cluster Config and give it the GceClusterConfig, the NodeInitializationAction and the SoftwareConfig
      val clusterConfig = new ClusterConfig()
        .setGceClusterConfig(gce)
        .setInitializationActions(initActions.asJava).setSoftwareConfig(softwareConfig)

      // Create a Cluster and give it a name and a Cluster Config
      val cluster = new GoogleCluster()
        .setClusterName(clusterName)
        .setConfig(clusterConfig)

      // Create a dataproc create request and give it the google project, a zone. and the Cluster
      val request = dataproc.projects().regions().clusters().create(googleProject, dataprocConfig.dataprocDefaultZone, cluster)

      try {
        executeGoogleRequest(request) // returns a DataprocOperation
      } catch {
        case e: GoogleJsonResponseException => throw new ClusterNotCreatedException(googleProject, clusterName)
      }
    }
  }

  /* Delete a cluster within the google project */
  def deleteCluster(googleProject: String, clusterName: String)(implicit executionContext: ExecutionContext): Future[Unit] = {
    Future {
      val request = dataproc.projects().regions().clusters().delete(googleProject, dataprocConfig.dataprocDefaultZone, clusterName)
      try {
        executeGoogleRequest(request)
      } catch {
        case e:GoogleJsonResponseException =>
          if(e.getStatusCode!=404)
            throw CallToGoogleApiFailedException(googleProject, clusterName, e.getStatusCode, e.getDetails.getMessage)
      }
    }
  }

}
