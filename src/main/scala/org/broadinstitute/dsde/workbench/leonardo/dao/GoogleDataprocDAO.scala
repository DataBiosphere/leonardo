package org.broadinstitute.dsde.workbench.leonardo.dao

import java.security.interfaces.DSAPrivateKey
import java.util
import javax.naming.Context
import javax.naming.directory.InitialDirContext

import com.google.api.services.dataproc.model._
import org.broadinstitute.dsde.workbench.leonardo.config.DataprocConfig
import org.broadinstitute.dsde.workbench.google.GoogleUtilities
import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.stream.Materializer
import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.dataproc.Dataproc
import com.google.api.services.pubsub.PubsubScopes
import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterRequest, ClusterResponse}
import org.broadinstitute.dsde.workbench.model.{ErrorReport, WorkbenchExceptionWithErrorReport}
import org.broadinstitute.dsde.workbench.leonardo.errorReportSource

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try


class GoogleDataprocDAO(protected val dataprocConfig: DataprocConfig)(implicit val system: ActorSystem, val materializer: Materializer, val executionContext: ExecutionContext)
  extends DataprocDAO with GoogleUtilities {

  private val httpTransport = GoogleNetHttpTransport.newTrustedTransport
  private val jsonFactory = JacksonFactory.getDefaultInstance
  private val cloudPlatformScopes = List(PubsubScopes.CLOUD_PLATFORM)

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
    op.map{op => {
      val metadata = op.getMetadata
      new ClusterResponse(clusterName, googleProject, metadata.get("clusterUuid").toString, metadata.get("status").toString, metadata.get("description").toString, op.getName)}}
  }


  private def build(googleProject: String, clusterName: String, clusterRequest: ClusterRequest)(implicit executionContext: ExecutionContext): Future[Operation] = {
    Future {
      //currently, the bucketPath and the labels of the clusterRequest are not used
      val dataproc = new Dataproc.Builder(GoogleNetHttpTransport.newTrustedTransport,
        JacksonFactory.getDefaultInstance, getDataProcServiceAccountCredential)
        .setApplicationName("dataproc").build()
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
}
