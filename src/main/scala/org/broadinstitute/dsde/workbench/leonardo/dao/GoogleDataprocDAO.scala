package org.broadinstitute.dsde.workbench.leonardo.dao

import com.google.api.services.dataproc.model._
import org.broadinstitute.dsde.workbench.leonardo.config.DataprocConfig
import org.broadinstitute.dsde.workbench.google.GoogleUtilities
import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.compute.Compute
import com.google.api.services.compute.model.Instance
import com.google.api.services.dataproc.Dataproc
import com.google.api.services.pubsub.PubsubScopes
import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterRequest, ClusterResponse, LeoException}
import org.broadinstitute.dsde.workbench.model.{ErrorReport, WorkbenchExceptionWithErrorReport}
import org.broadinstitute.dsde.workbench.leonardo.errorReportSource
import org.broadinstitute.dsde.workbench.leonardo.model.ModelTypes.GoogleProject

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

case class CallToGoogleApiFailedException(googleProject: GoogleProject, clusterName:String, exceptionStatusCode: Int, errorMessage:String) extends LeoException(s"Call to Google API failed for $googleProject/$clusterName. Message: $errorMessage",exceptionStatusCode)

class GoogleDataprocDAO(protected val dataprocConfig: DataprocConfig)(implicit val system: ActorSystem, val executionContext: ExecutionContext)
  extends DataprocDAO with GoogleUtilities {

  private val httpTransport = GoogleNetHttpTransport.newTrustedTransport
  private val jsonFactory = JacksonFactory.getDefaultInstance
  private val cloudPlatformScopes = List(PubsubScopes.CLOUD_PLATFORM)

  private lazy val dataproc = {
    new Dataproc.Builder(GoogleNetHttpTransport.newTrustedTransport,
      JacksonFactory.getDefaultInstance, getDataProcServiceAccountCredential)
      .setApplicationName("dataproc").build()
  }

  private lazy val compute = {
    new Compute.Builder(GoogleNetHttpTransport.newTrustedTransport,
      JacksonFactory.getDefaultInstance, getDataProcServiceAccountCredential)
      .setApplicationName("firecloud:leonardo").build()
  }

  private def getDataProcServiceAccountCredential: Credential = {
    new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountId(dataprocConfig.serviceAccount)
      .setServiceAccountScopes(cloudPlatformScopes.asJava)
      .setServiceAccountPrivateKeyFromPemFile(new java.io.File(dataprocConfig.serviceAccountPemPath))
      .build()
  }


  def createCluster(googleProject: String, clusterName: String, clusterRequest: ClusterRequest)(implicit executionContext: ExecutionContext): Future[ClusterResponse] = {
    val op = build(googleProject, clusterName, clusterRequest)
    op.map{op =>
      val metadata = op.getMetadata
      ClusterResponse(clusterName, googleProject, metadata.get("clusterUuid").toString, metadata.get("status").toString, metadata.get("description").toString, op.getName)}
  }

  private def build(googleProject: String, clusterName: String, clusterRequest: ClusterRequest)(implicit executionContext: ExecutionContext): Future[Operation] = {
    Future {
      //currently, the bucketPath of the clusterRequest are not used - it will be used later as a place to store notebooks and results
      val metadata = clusterRequest.labels + ("docker-image" -> dataprocConfig.dataprocDockerImage)
      val gce = new GceClusterConfig().setMetadata(metadata.asJava).setServiceAccount(clusterRequest.serviceAccount)
      val initActions = Seq(new NodeInitializationAction().setExecutableFile(dataprocConfig.dataprocInitScriptURI))
      val clusterConfig = new ClusterConfig().setGceClusterConfig(gce).setInitializationActions(initActions.asJava)
      val cluster = new Cluster().setClusterName(clusterName).setConfig(clusterConfig)
      val request = dataproc.projects().regions().clusters().create(googleProject, dataprocConfig.dataprocDefaultZone, cluster)
      try {
        executeGoogleRequest(request)
      } catch {
        case e: GoogleJsonResponseException => throw new WorkbenchExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"Google Request Failed: ${e.getDetails.getMessage}"))
      }
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

  override def getCluster(googleProject: GoogleProject, clusterName: String)(implicit executionContext: ExecutionContext): Future[Cluster] = {
    Future {
      val request = dataproc.projects().regions().clusters().get(googleProject, dataprocConfig.dataprocDefaultZone, clusterName)
      try {
        executeGoogleRequest(request)
      } catch {
        case e: GoogleJsonResponseException =>
          throw CallToGoogleApiFailedException(googleProject, clusterName, e.getStatusCode, e.getDetails.getMessage)
      }
    }
  }

  override def getOperation(operationName: String): Future[Operation] = {
    Future {
      val request = dataproc.projects().regions().operations().get(operationName)
      try {
        executeGoogleRequest(request)
      } catch {
        case e: GoogleJsonResponseException =>
          throw CallToGoogleApiFailedException("", "", e.getStatusCode, e.getDetails.getMessage)
      }
    }
  }

  override def getInstance(googleProject: GoogleProject, instanceName: String): Future[Instance] = {
    Future {
      val request = compute.instances().get(googleProject, dataprocConfig.dataprocDefaultZone, instanceName)
      try {
        executeGoogleRequest(request)
      } catch {
        case e: GoogleJsonResponseException =>
          throw CallToGoogleApiFailedException(googleProject, instanceName, e.getStatusCode, e.getDetails.getMessage)
      }
    }
  }
}
