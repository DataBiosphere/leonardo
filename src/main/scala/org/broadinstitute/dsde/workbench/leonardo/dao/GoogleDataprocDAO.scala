package org.broadinstitute.dsde.workbench.leonardo.dao

import java.io.{ByteArrayInputStream, File, InputStream, PrintWriter}
import java.nio.charset.StandardCharsets
import java.util.UUID

import com.google.api.services.dataproc.model._
import org.broadinstitute.dsde.workbench.leonardo.config.DataprocConfig
import org.broadinstitute.dsde.workbench.google.GoogleUtilities
import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes


import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.http.{FileContent, InputStreamContent}
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.compute.ComputeScopes
import com.google.api.services.compute.Compute
import com.google.api.services.compute.model.{Firewall, Project}
import com.google.api.services.compute.model.Firewall.Allowed
import com.google.api.services.dataproc.Dataproc
import com.google.api.services.plus.PlusScopes
import com.google.api.services.storage.{Storage, StorageScopes}
import com.google.api.services.storage.model.{Bucket, ComposeRequest, StorageObject}
import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterInitValues, ClusterRequest, ClusterResponse}
import com.google.api.services.pubsub.PubsubScopes
import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterRequest, ClusterResponse, LeoException}
import org.broadinstitute.dsde.workbench.model.{ErrorReport, WorkbenchExceptionWithErrorReport}
import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterInitValues, ClusterRequest, ClusterResponse, LeoException}
import org.broadinstitute.dsde.workbench.model.ErrorReport
import org.broadinstitute.dsde.workbench.leonardo.errorReportSource
import org.broadinstitute.dsde.workbench.leonardo.model.ModelTypes.GoogleProject

import org.broadinstitute.dsde.workbench.leonardo.model.ModelTypes.GoogleProject


import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

case class BucketNotCreatedException(googleProject: GoogleProject, clusterName: String) extends LeoException(s"Cluster $googleProject/$clusterName failed to create required resource.", StatusCodes.InternalServerError)
case class BucketInsertionException(googleProject: GoogleProject, clusterName: String) extends LeoException(s"Cluster $googleProject/$clusterName failed to upload Jupyter certificates", StatusCodes.InternalServerError)
case class ClusterNotCreatedException(googleProject: GoogleProject, clusterName: String) extends LeoException(s"Failed to create cluster $googleProject/$clusterName", StatusCodes.InternalServerError)
case class FirewallRuleInaccessibleException(googleProject: GoogleProject, firewallRule: String) extends LeoException(s"Unable to access firewall rule $googleProject/$firewallRule", StatusCodes.InternalServerError)
case class FirewallRuleInsertionException(googleProject: GoogleProject, firewallRule: String) extends LeoException(s"Unable to insert new firewall rule $googleProject/$firewallRule", StatusCodes.InternalServerError)
case class CallToGoogleApiFailedException(googleProject: GoogleProject, clusterName:String, exceptionStatusCode: Int, errorMessage:String) extends LeoException(s"Call to Google API failed for $googleProject/$clusterName. Message: $errorMessage",exceptionStatusCode)

class GoogleDataprocDAO(protected val dc: DataprocConfig)(implicit val system: ActorSystem, val executionContext: ExecutionContext)
  extends DataprocDAO with GoogleUtilities {

  private val httpTransport = GoogleNetHttpTransport.newTrustedTransport
  private val jsonFactory = JacksonFactory.getDefaultInstance
  private val cloudPlatformScopes = List(ComputeScopes.CLOUD_PLATFORM)
  private val storageScopes = Seq(StorageScopes.DEVSTORAGE_FULL_CONTROL, ComputeScopes.COMPUTE, PlusScopes.USERINFO_EMAIL, PlusScopes.USERINFO_PROFILE)
  private val vmScopes = List(ComputeScopes.COMPUTE, ComputeScopes.CLOUD_PLATFORM)
  private val serviceAccountPemFile = new File(dc.configFolderPath, dc.serviceAccountPemName)

  private lazy val dataproc = {
    new Dataproc.Builder(GoogleNetHttpTransport.newTrustedTransport,
      JacksonFactory.getDefaultInstance, getDataProcServiceAccountCredential)
      .setApplicationName("dataproc").build()
  }

  private lazy val getDataProcServiceAccountCredential: Credential = {
    new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountId(dc.serviceAccount)
      .setServiceAccountScopes(cloudPlatformScopes.asJava)
      .setServiceAccountPrivateKeyFromPemFile(serviceAccountPemFile)
      .build()
  }

  private lazy val getBucketServiceAccountCredential: Credential = {
    new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountId(dc.serviceAccount)
      .setServiceAccountScopes(storageScopes.asJava) // grant bucket-creation powers
      .setServiceAccountPrivateKeyFromPemFile(serviceAccountPemFile)
      .build()
  }

  private lazy val getVmServiceAccountCredential: Credential = {
    new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountId(dc.serviceAccount)
      .setServiceAccountScopes(vmScopes.asJava)
      .setServiceAccountPrivateKeyFromPemFile(serviceAccountPemFile)
      .build()
  }

  def createCluster(googleProject: GoogleProject, clusterName: String, clusterRequest: ClusterRequest)(implicit executionContext: ExecutionContext): Future[ClusterResponse] = {
    val bucketName = s"${clusterName}-${UUID.randomUUID.toString}"

    updateFirewallRules(googleProject)  //when should this rule be created? Before or after cluster creation?

    val bucketResponse = initializeBucket(googleProject, clusterName, bucketName, clusterRequest.serviceAccount)
    bucketResponse.flatMap[ClusterResponse]{ Bucket =>
      val op = build(googleProject, clusterName, clusterRequest, bucketName)
      op.map { op =>
        val metadata = op.getMetadata
        ClusterResponse(clusterName, googleProject, metadata.get("clusterUuid").toString, metadata.get("status").toString, metadata.get("description").toString, op.getName)
      }
    }
  }

  private def createBucketUri(bucketName: String, fileName: String): String = {
    s"gs://${bucketName}/${fileName}"
  }

  private def initializeBucket(googleProject: GoogleProject, clusterName: String, bucketName:String, serviceAccount: String): Future[Bucket] = {
    import org.broadinstitute.dsde.workbench.leonardo.model.LeonardoJsonSupport._
    import spray.json._
    Future {
      val bucket = new Bucket().setName(bucketName)
      val bucketInserter = getStorage(getBucketServiceAccountCredential).buckets().insert(googleProject, bucket)
      try {
       executeGoogleRequest(bucketInserter) //returns a Bucket
      } catch {
        case e: GoogleJsonResponseException => throw new BucketNotCreatedException(googleProject, clusterName)
      } finally {
        val initScriptRaw = scala.io.Source.fromFile(dc.configFolderPath + dc.initActionsScriptName).mkString
        val replacements =  ClusterInitValues(clusterName, googleProject, dc.dataprocDockerImage,
                  dc.jupyterProxyDockerImage, createBucketUri(bucketName, dc.jupyterServerCrtName), createBucketUri(bucketName, dc.jupyterServerKeyName),
                  createBucketUri(bucketName, dc.jupyterRootCaPemName), createBucketUri(bucketName, dc.clusterDockerComposeName), dc.jupyterServerName, dc.proxyServerName).toJson.asJsObject.fields
        val initScript = replacements.foldLeft(initScriptRaw)((a, b) => a.replaceAllLiterally("$(" + b._1 +")", b._2.toString()))

        val jupyterServerCrtFile = new File(dc.configFolderPath + dc.jupyterServerCrtName)
        val jupyterServerKeyFile = new File(dc.configFolderPath + dc.jupyterServerKeyName)
        val jupyterRootCaPemFile = new File(dc.configFolderPath + dc.jupyterRootCaPemName)
        val clusterDockerCompose = new File(dc.configFolderPath + dc.clusterDockerComposeName)

        populateInitBucket(googleProject, bucketName, dc.initActionsScriptName, Right(initScript))
        populateInitBucket(googleProject, bucketName, dc.clusterDockerComposeName, Left(clusterDockerCompose))
        populateInitBucket(googleProject, bucketName, dc.jupyterServerCrtName, Left(jupyterServerCrtFile))
        populateInitBucket(googleProject, bucketName, dc.jupyterServerKeyName, Left(jupyterServerKeyFile))
        populateInitBucket(googleProject, bucketName, dc.jupyterRootCaPemName, Left(jupyterRootCaPemFile))
      }
    }
  }

  private def populateInitBucket(googleProject: GoogleProject, bucketName: String, fileName: String, content: Either[File, String]): Future[StorageObject] = {
    Future {
      val so = new StorageObject().setName(fileName)

      val bucketContent = content match {
        case Left(file) => new FileContent(null, file)
        case Right(str) => new InputStreamContent(null, new ByteArrayInputStream(str.getBytes(StandardCharsets.UTF_8)))
      }

      val fileInserter = getStorage(getBucketServiceAccountCredential).objects().insert(bucketName, so, bucketContent)
      fileInserter.getMediaHttpUploader().setDirectUploadEnabled(true)

      try {
        executeGoogleRequest(fileInserter) //returns a StorageObject
      } catch {
        case e: GoogleJsonResponseException => throw new BucketInsertionException()
      }
    }
  }

  private def getStorage(credential: Credential) = {
    new Storage.Builder(httpTransport, jsonFactory, credential).setApplicationName("leonardo").build()
  }



  private def build(googleProject: GoogleProject, clusterName: String, clusterRequest: ClusterRequest, bucketName: String)(implicit executionContext: ExecutionContext): Future[Operation] = {
    Future {
      //currently, the bucketPath of the clusterRequest is not used - it will be used later as a place to store notebooks and results
      val dataproc = new Dataproc.Builder(GoogleNetHttpTransport.newTrustedTransport,
        JacksonFactory.getDefaultInstance, getDataProcServiceAccountCredential)
        .setApplicationName("dataproc").build()

      val gce = new GceClusterConfig()
        .setServiceAccount(clusterRequest.serviceAccount)
        .setTags(List("leonardo").asJava)

      val initActions = Seq(new NodeInitializationAction().setExecutableFile(createBucketUri(bucketName, dc.initActionsScriptName)))

      val worksInstancConfig = new InstanceGroupConfig().setNumInstances(0)

      val clusterConfig = new ClusterConfig()
        .setGceClusterConfig(gce)
        .setInitializationActions(initActions.asJava)
        .setWorkerConfig(worksInstancConfig)

      val cluster = new Cluster()
        .setClusterName(clusterName)
        .setConfig(clusterConfig)

      val request = dataproc.projects().regions().clusters().create(googleProject, dc.dataprocDefaultZone, cluster)

      try {
        executeGoogleRequest(request)
      } catch {
        case e: GoogleJsonResponseException => throw new ClusterNotCreatedException(googleProject, clusterName)
      }
    }
  }

  private def updateFirewallRules(googleProject: String) = {
    val request = new Compute(httpTransport, jsonFactory, getVmServiceAccountCredential).firewalls().get(googleProject, dc.clusterFirewallRuleName)
    try {
      executeGoogleRequest(request)
    } catch {
      case t: GoogleJsonResponseException if t.getStatusCode == 404 => addFirewallRule(googleProject)
      case e: GoogleJsonResponseException => throw new FirewallRuleInaccessibleException(googleProject, dc.clusterFirewallRuleName)
    }
  }

  private def addFirewallRule(googleProject: String) = {
    val allowed = new Allowed().set("tcp", "443").setIPProtocol("tcp").setPorts(List("443").asJava)

    val firewallRule = new Firewall()
      .setName("leonardo-notebooks-rule")
      .setTargetTags(List("leonardo").asJava)
      .setAllowed(List(allowed).asJava)

    val request = new Compute(httpTransport, jsonFactory, getVmServiceAccountCredential).firewalls().insert(googleProject, firewallRule)

    try {
      executeGoogleRequest(request)
    } catch {
      case e: GoogleJsonResponseException => throw new FirewallRuleInsertionException(googleProject, dc.clusterFirewallRuleName)
    }
  }


  def deleteCluster(googleProject: String, clusterName: String)(implicit executionContext: ExecutionContext): Future[Unit] = {
    Future {
      //currently, the bucketPath of the clusterRequest are not used - it will be used later as a place to store notebooks and results
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
