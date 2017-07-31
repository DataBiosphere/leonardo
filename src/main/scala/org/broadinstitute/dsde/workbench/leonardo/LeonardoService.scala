package org.broadinstitute.dsde.workbench.leonardo
import akka.actor.ActorSystem
import akka.stream.Materializer
import org.broadinstitute.dsde.workbench.google.GoogleUtilities
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterRequest
import com.typesafe.config.ConfigFactory
import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.http.HttpResponseException
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.dataproc.Dataproc
import com.google.api.services.dataproc.model._
import com.google.api.services.pubsub.PubsubScopes
import com.google.api.services.pubsub.model.Topic
import com.typesafe.scalalogging.LazyLogging
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.ExceptionHandler
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class LeonardoService(implicit val system: ActorSystem, val materializer: Materializer, val executionContext: ExecutionContext) extends GoogleUtilities with LazyLogging{

  private val httpTransport = GoogleNetHttpTransport.newTrustedTransport
  private val jsonFactory = JacksonFactory.getDefaultInstance
  private val cloudPlatformScopes = List(PubsubScopes.CLOUD_PLATFORM)
  private val cloudResourcesScopes = List("https://www.googleapis.com/auth/compute", "https://www.googleapis.com/auth/bigquery",
  "https://www.googleapis.com/auth/bigtable.admin.table",
  "https://www.googleapis.com/auth/bigtable.data",
    "https://www.googleapis.com/auth/devstorage.full_control")
  private val conf = ConfigFactory.load()
  private val gcsConf = conf.getConfig("gcs")


  private def getDataProcServiceAccountCredential: Credential = {
    new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountId(gcsConf.getString("serviceAccount"))
      .setServiceAccountScopes(cloudPlatformScopes.asJava)
      .setServiceAccountPrivateKeyFromP12File(new java.io.File("leonardo-account.p12"))
      .build()
  }

  def createCluster(googleProject: String, clusterName: String, clusterRequest: ClusterRequest) = {
//     try {
//       val op = build(googleProject, clusterName, clusterRequest)
//       if (op.getDone()) {
//         if (op.getError != None) {
//         }
//       } else {
//
//       }
//     } catch {
//       case t: HttpResponseException if t.getStatusCode == StatusCodes.NotFound.intValue => None
//     }
  }

  def build(googleProject: String, clusterName: String, clusterRequest: ClusterRequest) = {
    val dataproc = new Dataproc.Builder(GoogleNetHttpTransport.newTrustedTransport,
      JacksonFactory.getDefaultInstance, getDataProcServiceAccountCredential)
      .setApplicationName("dataproc").build()
    val metadata = Map[String, String](("docker-image", "jmthibault79/jupyter-hail-proto:dydshj-1"))
    val gce = new GceClusterConfig().setMetadata(metadata.asJava).setServiceAccount(clusterRequest.serviceAccount)
    val initActions = Seq(new NodeInitializationAction().setExecutableFile("gs://fc-8a820408-6d54-4fde-8780-9f547e07b275/init-action.sh"))
    val clusterConfig = new ClusterConfig().setGceClusterConfig(gce).setInitializationActions(initActions.asJava)
    val cluster= new Cluster().setClusterName(clusterName).setConfig(clusterConfig)
    val request = dataproc.projects().regions().clusters().create(googleProject, "us-central1", cluster)
    val op = executeGoogleRequest(request)
    val x = 1
    op
//    retryWithRecoverWhen500orGoogleError(() => {
//      executeGoogleRequest(request)
//      true
//    }) {
//      case t: HttpResponseException => throw new Exception(t.getStatusMessage,t.getCause)
////      case m: HttpResponseException if m.getStatusCode == 403 => false
//    }

  }
}
