package org.broadinstitute.dsde.workbench.leonardo

import akka.http.scaladsl.model.headers.{Cookie, HttpCookiePair}
import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.ResourceFile
import org.broadinstitute.dsde.workbench.service.RestClient
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.config.LeoAuthToken
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterStatus.ClusterStatus
import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, ClusterRequest}
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.model.{ValueObject, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google._
import org.openqa.selenium.WebDriver

import scala.concurrent.Future
import scala.io.Source

/**
  * Leonardo API service client.
  */
object Leonardo extends RestClient with LazyLogging {

  class ValueObjectSerializer[T <: ValueObject] extends JsonSerializer[T] {
    override def serialize(obj: T, gen: JsonGenerator, serializers: SerializerProvider): Unit = {
      gen.writeString(obj.value)
    }
  }

  case class ValueObjectDeserializer[T <: ValueObject](create: String => T) extends JsonDeserializer[T] {
    override def deserialize(parser: JsonParser, ctx: DeserializationContext): T = {
      create(parser.getValueAsString)
    }
  }

  case object ClusterStatusSerializer extends JsonSerializer[ClusterStatus] {
    override def serialize(status: ClusterStatus, gen: JsonGenerator, serializers: SerializerProvider): Unit = {
      gen.writeString(status.toString)
    }
  }

  case object ClusterStatusDeserializer extends JsonDeserializer[ClusterStatus] {
    override def deserialize(parser: JsonParser, ctx: DeserializationContext): ClusterStatus = {
      ClusterStatus.withName(parser.getValueAsString)
    }
  }
  /*
   class ClusterDeserializer extends JsonDeserializer[Cluster] {
     override def deserialize(p: JsonParser, ctxt: DeserializationContext): Cluster = {


       val jsonNode: com.fasterxml.jackson.core.TreeNode = p.readValueAsTree

       jsonNode.get(0).traverse()

       // TODO: custom JSON deserializer
       -    // the default doesn't handle some fields correctly so here they're strings
         -    private case class ClusterKluge(clusterName: ClusterName,
                                              -                                    googleId: UUID,
       -                                    googleProject: String,
       -                                    serviceAccountInfo: Map[String, String],
       -                                    machineConfig: Map[String, String],
       -                                    clusterUrl: URL,
       -                                    operationName: OperationName,
       -                                    status: String,
       -                                    hostIp: Option[IP],
       -                                    creator: String,
       -                                    createdDate: String,
       -                                    destroyedDate: Option[String],
       -                                    labels: LabelMap,
       -                                    jupyterExtensionUri: Option[String],
       -                                    jupyterUserScriptUri: Option[String],
       -                                    stagingBucket: String,
       -                                    errors:List[ClusterError]) {
         -
           -      def toCluster = Cluster(clusterName,
           -        googleId,
           -        GoogleProject(googleProject),
           -        ServiceAccountInfo(serviceAccountInfo),
           -        MachineConfig(machineConfig),
           -        clusterUrl,
           -        operationName,
           -        ClusterStatus.withName(status),
           -        hostIp,
           -        WorkbenchEmail(creator),
           -        Instant.parse(createdDate),
           -        destroyedDate map Instant.parse,
           -        labels,
           -        jupyterExtensionUri map (parseGcsPath(_).right.get),
           -        jupyterUserScriptUri map (parseGcsPath(_).right.get),
           -        Some(GcsBucketName(stagingBucket)),
           -        errors
             -      )  }
   }
   */

  private val url = LeonardoConfig.Leonardo.apiUrl

  object test {
    def ping()(implicit token: AuthToken): String = {
      logger.info(s"Pinging: GET /ping")
      parseResponse(getRequest(url + "ping"))
    }
  }

  object cluster {

    lazy val clusterSerializationModule: Module = {
      // for Instant
      val module: SimpleModule = new JavaTimeModule

      module.addSerializer(classOf[ClusterName], new ValueObjectSerializer[ClusterName])
      module.addDeserializer(classOf[ClusterName], ValueObjectDeserializer(ClusterName))

      module.addSerializer(classOf[GoogleProject], new ValueObjectSerializer[GoogleProject])
      module.addDeserializer(classOf[GoogleProject], ValueObjectDeserializer(GoogleProject))

      module.addSerializer(classOf[WorkbenchEmail], new ValueObjectSerializer[WorkbenchEmail])
      module.addDeserializer(classOf[WorkbenchEmail], ValueObjectDeserializer(WorkbenchEmail))

      module.addSerializer(classOf[OperationName], new ValueObjectSerializer[OperationName])
      module.addDeserializer(classOf[OperationName], ValueObjectDeserializer(OperationName))

      module.addSerializer(classOf[IP], new ValueObjectSerializer[IP])
      module.addDeserializer(classOf[IP], ValueObjectDeserializer(IP))

      module.addSerializer(classOf[GcsBucketName], new ValueObjectSerializer[GcsBucketName])
      module.addDeserializer(classOf[GcsBucketName], ValueObjectDeserializer(GcsBucketName))

      module.addSerializer(classOf[ClusterStatus], ClusterStatusSerializer)
      module.addDeserializer(classOf[ClusterStatus], ClusterStatusDeserializer)

      module
    }

    def handleClusterResponse(response: String): Cluster = {
      mapper.registerModule(clusterSerializationModule)
      mapper.readValue(response, classOf[Cluster])
    }

    def handleClusterSeqResponse(response: String): List[Cluster] = {
      // this does not work, due to type erasure
      // mapper.readValue(response, classOf[List[Cluster]])

      mapper.registerModule(clusterSerializationModule)
      mapper.readValue(response, classOf[List[_]]).map { clusterAsAny =>
        val clusterAsJson = mapper.writeValueAsString(clusterAsAny)
        mapper.readValue(clusterAsJson, classOf[Cluster])
      }
    }

    def clusterPath(googleProject: GoogleProject, clusterName: ClusterName): String =
      s"api/cluster/${googleProject.value}/${clusterName.value}"

    def list()(implicit token: AuthToken): Seq[Cluster] = {
      logger.info(s"Listing all active clusters: GET /api/clusters")
      handleClusterSeqResponse(parseResponse(getRequest(url + "api/clusters")))
    }

    def listIncludingDeleted()(implicit token: AuthToken): Seq[Cluster] = {
      logger.info(s"Listing all clusters including deleted: GET /api/clusters?includeDeleted=true")
      handleClusterSeqResponse(parseResponse(getRequest(url + "api/clusters?includeDeleted=true")))
    }

    def create(googleProject: GoogleProject, clusterName: ClusterName, clusterRequest: ClusterRequest)(implicit token: AuthToken): Cluster = {
      val path = clusterPath(googleProject, clusterName)
      logger.info(s"Create cluster: PUT /$path")
      handleClusterResponse(putRequest(url + path, clusterRequest))
//      handleClusterResponse(putRequest(url + path, toJsonString(clusterRequest)))
    }

    def get(googleProject: GoogleProject, clusterName: ClusterName)(implicit token: AuthToken): Cluster = {
      val path = clusterPath(googleProject, clusterName)
      logger.info(s"Get details for cluster: GET /$path")
      handleClusterResponse(parseResponse(getRequest(url + path)))
    }

    def delete(googleProject: GoogleProject, clusterName: ClusterName)(implicit token: AuthToken): String = {
      val path = clusterPath(googleProject, clusterName)
      logger.info(s"Delete cluster: DELETE /$path")
      deleteRequest(url + path)
    }

  }

  object notebooks {
    def handleContentItemResponse(response: String): ContentItem = {
      mapper.readValue(response, classOf[ContentItem])
    }

    def notebooksPath(googleProject: GoogleProject, clusterName: ClusterName): String =
      s"notebooks/${googleProject.value}/${clusterName.value}"

    def contentsPath(googleProject: GoogleProject, clusterName: ClusterName, contentPath: String): String =
      s"${notebooksPath(googleProject, clusterName)}/api/contents/$contentPath"

    def localizePath(googleProject: GoogleProject, clusterName: ClusterName): String =
      s"${notebooksPath(googleProject, clusterName)}/api/localize"

    def get(googleProject: GoogleProject, clusterName: ClusterName)(implicit token: AuthToken, webDriver: WebDriver): NotebooksListPage = {
      val path = notebooksPath(googleProject, clusterName)
      logger.info(s"Get notebook: GET /$path")
      new NotebooksListPage(url + path)
    }

    def localize(googleProject: GoogleProject, clusterName: ClusterName, locMap: Map[String, String])(implicit token: AuthToken): String = {
      val path = localizePath(googleProject, clusterName)
      logger.info(s"Localize notebook files: POST /$path")
      val cookie = Cookie(HttpCookiePair("LeoToken", token.value))
      postRequest(url + path, locMap, httpHeaders = List(cookie))
    }

    def getContentItem(googleProject: GoogleProject, clusterName: ClusterName, contentPath: String, includeContent: Boolean = true)(implicit token: AuthToken): ContentItem = {
      val path = contentsPath(googleProject, clusterName, contentPath) + (if(includeContent) "?content=1" else "")
      logger.info(s"Get notebook contents: GET /$path")
      val cookie = Cookie(HttpCookiePair("LeoToken", token.value))
      handleContentItemResponse(parseResponse(getRequest(url + path, httpHeaders = List(cookie))))
    }
  }

  object dummyClient {
    import akka.http.scaladsl.Http
    import akka.http.scaladsl.model._
    import akka.http.scaladsl.server.Directives.{get => httpGet, _}

    def get(googleProject: GoogleProject, clusterName: ClusterName)(implicit token: AuthToken, webDriver: WebDriver) = {
      val localhost = java.net.InetAddress.getLocalHost().getHostName()
      val url = s"http://${localhost}:9090/${googleProject.value}/${clusterName.value}/client?token=${token.value}"
      logger.info(s"Get dummy client: $url")
      new DummyClientPage(url).open
    }

    def startServer: Future[Http.ServerBinding] = {
      logger.info("Starting local server on port 9090")
      Http().bindAndHandle(route, "0.0.0.0", 9090)
    }

    def stopServer(bindingFuture: Future[Http.ServerBinding]): Future[Unit] = {
      logger.info("Stopping local server")
      bindingFuture.flatMap(_.unbind())
    }

    val route =
      path(Segment / Segment / "client") { (googleProject, clusterName) =>
        httpGet {
          parameter('token.as[String]) { token =>
            complete {
              logger.info(s"Serving dummy client for $googleProject/$clusterName")
              HttpEntity(ContentTypes.`text/html(UTF-8)`, getContent(GoogleProject(googleProject), ClusterName(clusterName), LeoAuthToken(token)))
            }
          }
        }
      }

    private def getContent(googleProject: GoogleProject, clusterName: ClusterName, token: LeoAuthToken) = {
      val resourceFile = ResourceFile("dummy-notebook-client.html")
      val raw = Source.fromFile(resourceFile).mkString
      val replacementMap = Map(
        "leoBaseUrl" -> url,
        "googleProject" -> googleProject.value,
        "clusterName" -> clusterName.value,
        "token" -> token.value,
        "googleClientId" -> "some-client"
      )
      replacementMap.foldLeft(raw) { case (source, (key, replacement)) =>
        source.replaceAllLiterally("$("+key+")", s"""'$replacement'""")
      }
    }
  }
}
