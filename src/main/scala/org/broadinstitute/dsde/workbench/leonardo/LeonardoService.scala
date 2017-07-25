package org.broadinstitute.dsde.workbench.leonardo
import akka.actor.ActorSystem
import akka.stream.Materializer
import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterRequest, GoogleUtilities}
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

import scala.collection.JavaConversions._
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
//  private val conf = ConfigFactory.load()
 // private val gcsConf = conf.getConfig("gcs")


  private def getDataProcServiceAccountCredential: Credential = {
    new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      //.setServiceAccountId(gcsConf.getString("serviceAccount"))
      .setServiceAccountScopes(cloudPlatformScopes)
      .setServiceAccountPrivateKeyFromP12File(new java.io.File("leonardo-account.p12"))
      .build()
  }

  def createCluster() = {
//    build().onComplete {
//      case Success(op) => "It worked!"
//      case Failure(e) => e.getMessage
//    }
//    build() match {
//      case Operation =>
//    }
  }

  def build(googleProject: String, clusterName: String/*, clusterRequest: ClusterRequest*/) = {
    val dataproc = new Dataproc.Builder(GoogleNetHttpTransport.newTrustedTransport,
      JacksonFactory.getDefaultInstance, getDataProcServiceAccountCredential)
      .setApplicationName("dataproc").build()
    val metadata = Map[String, String]()  //Are there changes needed to this docker image?
    val gce = new GceClusterConfig().setMetadata(metadata)
    val initActions = Seq(new NodeInitializationAction().
      setExecutableFile(""))  //Where do we want this init script to reside?
    val clusterConfig = new ClusterConfig().setGceClusterConfig(gce).setInitializationActions(initActions)
    val cluster= new Cluster().setClusterName(clusterName).setConfig(clusterConfig)
    val request = dataproc.projects().regions().clusters().create(googleProject, "us-central1", cluster)
    executeGoogleRequest(request)

//    retryWithRecoverWhen500orGoogleError(() => {
//      executeGoogleRequest(request)
//      true
//    }) {
//      case t: HttpResponseException => throw new Exception(t.getStatusMessage,t.getCause)
////      case m: HttpResponseException if m.getStatusCode == 403 => false
//    }

  }
}
