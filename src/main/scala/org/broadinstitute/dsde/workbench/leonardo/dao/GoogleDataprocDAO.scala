package org.broadinstitute.dsde.workbench.leonardo.dao

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.http.{AbstractInputStreamContent, FileContent, InputStreamContent}
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.compute.{Compute, ComputeScopes}
import com.google.api.services.compute.model.{Operation => ComputeOperation, Firewall}
import com.google.api.services.compute.model.Firewall.Allowed
import com.google.api.services.dataproc.Dataproc
import com.google.api.services.dataproc.model.{Cluster => GoogleCluster, Operation => DataprocOperation, _}
import com.google.api.services.plus.PlusScopes
import com.google.api.services.storage.{Storage, StorageScopes}
import com.google.api.services.storage.model.{Bucket, StorageObject}
import java.io.{ByteArrayInputStream, File}
import java.nio.charset.StandardCharsets
import org.broadinstitute.dsde.workbench.google.GoogleUtilities
import org.broadinstitute.dsde.workbench.leonardo.config.{DataprocConfig, ProxyConfig}
import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterRequest, ClusterResponse, LeoException, _}
import org.broadinstitute.dsde.workbench.leonardo.model.ModelTypes.GoogleProject
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

case class CallToGoogleApiFailedException(googleProject: GoogleProject, clusterName:String, exceptionStatusCode: Int, errorMessage: String)
  extends LeoException(s"Call to Google API failed for $googleProject/$clusterName. Message: $errorMessage",exceptionStatusCode)

case class FirewallRuleNotFoundException(googleProject: GoogleProject, firewallRuleName: String)
  extends LeoException(s"Firewall rule $firewallRuleName not found in project $googleProject", StatusCodes.NotFound)

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
      .setServiceAccountId(dataprocConfig.serviceAccount)
      .setServiceAccountScopes(scopes.asJava)
      .setServiceAccountPrivateKeyFromPemFile(serviceAccountPemFile)
      .build()
  }

  def createCluster(googleProject: GoogleProject, clusterName: String, clusterRequest: ClusterRequest, bucketName: String)(implicit executionContext: ExecutionContext): Future[ClusterResponse] = {
    buildCluster(googleProject, clusterName, clusterRequest, bucketName).map { operation =>
      // Once the cluster creation request is sent to Google, it returns a DataprocOperation, which we transform into a ClusterResponse
      val metadata = operation.getMetadata()
      ClusterResponse(clusterName, googleProject, metadata.get("clusterUuid").toString, metadata.get("status").toString, metadata.get("description").toString, operation.getName)
    }
  }

  /* Kicks off building the cluster. This will return before the cluster finishes creating. */
  private def buildCluster(googleProject: GoogleProject, clusterName: String, clusterRequest: ClusterRequest, bucketName: String)(implicit executionContext: ExecutionContext): Future[DataprocOperation] = {
    Future {
      // Create a GceClusterConfig, which has the common config settings for resources of Google Compute Engine cluster instances,
      //   applicable to all instances in the cluster. Give it the user's service account.
      //   Set the network tag, which is needed by the firewall rule that allows leo to talk to the cluster
      val gce = new GceClusterConfig()
        .setServiceAccount(clusterRequest.serviceAccount)
        .setTags(List(dataprocConfig.clusterNetworkTag).asJava)

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

      // Create a dataproc create request and give it the google project, a zone, and the Cluster
      val request = dataproc.projects().regions().clusters().create(googleProject, dataprocConfig.dataprocDefaultZone, cluster)

      try {
        executeGoogleRequest(request) // returns a DataprocOperation
      } catch {
        case e: GoogleJsonResponseException => throw CallToGoogleApiFailedException(googleProject, clusterName, e.getStatusCode, e.getDetails.getMessage)
      }
    }
  }


  def updateFirewallRule(googleProject: GoogleProject): Future[Unit] = {
    getFirewallRule(googleProject, dataprocConfig.clusterFirewallRuleName).recoverWith {
      case _: FirewallRuleNotFoundException => addFirewallRule(googleProject)
    }.mapTo[Unit]
  }


  private def getFirewallRule(googleProject: String, firewallRuleName: String): Future[Firewall] = {
    Future {
      val request = compute.firewalls().get(googleProject, firewallRuleName)
      executeGoogleRequest(request) // returns a Firewall
    } recoverWith {
      case e: GoogleJsonResponseException =>
        if (e.getStatusCode == 404)
          throw FirewallRuleNotFoundException(googleProject, firewallRuleName)
        throw CallToGoogleApiFailedException(googleProject, firewallRuleName, e.getStatusCode, e.getDetails.getMessage)
    }
  }


  /* Adds a firewall rule in the given google project. This firewall rule allows ingress traffic through a specified port for all
     VMs with the network tag "leonardo". This rule should only be added once per project.
    To think about: do we want to remove this rule if a google project no longer has any clusters? */
  private def addFirewallRule(googleProject: GoogleProject): Future[ComputeOperation] = {
    Future {
      // Create an Allowed object that specifies the port and protocol of rule
     //val allowedList = firewallRule.allowed.map(allowedPair => new GoogleAllowed().setIPProtocol(allowedPair.protocol).setPorts(allowedPair.port.asJava))
      val allowed = new Allowed().setIPProtocol(proxyConfig.jupyterProtocol).setPorts(List(proxyConfig.jupyterPort.toString).asJava)
      // Create a firewall rule that takes an Allowed object, a name, and the network tag a cluster must have to be accessed through the rule
      val googleFirewallRule = new Firewall()
        .setName(dataprocConfig.clusterFirewallRuleName)
        .setTargetTags(List(dataprocConfig.clusterNetworkTag).asJava)
        .setAllowed(List(allowed).asJava)

      val request = compute.firewalls().insert(googleProject, googleFirewallRule)

      executeGoogleRequest(request) // returns a ComputeOperation
    } recoverWith {
      case e: GoogleJsonResponseException =>
        throw CallToGoogleApiFailedException(googleProject, dataprocConfig.clusterFirewallRuleName, e.getStatusCode, e.getDetails.getMessage)
    }
  }
  // ??? Should the configs be in gdDAO? Should we just pass stuff to the functions?


  /* Create a bucket in the given google project for the initialization files when creating a cluster */
  def createBucket(googleProject: GoogleProject, bucketName: String): Future[Unit] = {
    Future {
      val bucket = new Bucket().setName(bucketName)
      val bucketInserter = storage.buckets().insert(googleProject, bucket)
      executeGoogleRequest(bucketInserter) // returns a Bucket
    } recoverWith {
      case e: GoogleJsonResponseException => throw CallToGoogleApiFailedException(googleProject, bucketName, e.getStatusCode, e.getDetails.getMessage)
    } map { bucket =>
      BucketResponse(bucket.getName, bucket.getTimeCreated.toString)
    }
  }

  def uploadToBucket(googleProject: GoogleProject, bucketName: String, fileName: String, content: File): Future[Unit] = {
    val fileContent = new FileContent(null, content)
    uploadToBucket(googleProject, bucketName, fileName, fileContent).mapTo[Unit]
  }

  def uploadToBucket(googleProject: GoogleProject, bucketName: String, fileName: String, content: String): Future[Unit] = {
    val inputStreamContent = new InputStreamContent(null, new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)))
    uploadToBucket(googleProject, bucketName, fileName, inputStreamContent).mapTo[Unit]
  }

  /* Upload the given file into the given bucket */
  private def uploadToBucket(googleProject: GoogleProject, bucketName: String, fileName: String, content: AbstractInputStreamContent): Future[StorageObjectResponse] = {
    Future {
      // Create a storage object
      val storageObject = new StorageObject().setName(fileName)
      // Create a storage object insertion request
      val fileInserter = storage.objects().insert(bucketName, storageObject, content)
      // Enable direct media upload
      fileInserter.getMediaHttpUploader().setDirectUploadEnabled(true)
      executeGoogleRequest(fileInserter) //returns a StorageObject
    }.recoverWith {
      case e: GoogleJsonResponseException =>  throw CallToGoogleApiFailedException(googleProject, bucketName, e.getStatusCode, e.getDetails.getMessage)
    } map { storageObject =>
      StorageObjectResponse(storageObject.getName, storageObject.getBucket, storageObject.getTimeCreated.toString)
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
