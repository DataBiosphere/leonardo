package org.broadinstitute.dsde.workbench.leonardo.service

import java.io.InputStream
import java.security.{KeyStore, SecureRandom}
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}

import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.headers.{`Access-Control-Allow-Origin`, ContentDispositionTypes, `Content-Disposition`}
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.RouteTest
import akka.http.scaladsl.{ConnectionContext, Http, HttpsConnectionContext}
import akka.stream.scaladsl.{Flow, Sink, Source}
import org.broadinstitute.dsde.workbench.leonardo.config.ProxyConfig
import org.broadinstitute.dsde.workbench.leonardo.service.TestProxy.Data
import org.scalatest.concurrent.ScalaFutures
import scala.collection.immutable
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat


trait TestProxy { this: ScalaFutures =>
  val googleProject: String
  val clusterName: String
  val proxyConfig: ProxyConfig
  val routeTest: RouteTest

  import routeTest._

  // The backend server behind the proxy
  var serverBinding: ServerBinding = _

  def startProxyServer() = {
    val password = "leo-test".toCharArray

    val ks: KeyStore = KeyStore.getInstance("PKCS12")
    val keystore: InputStream = getClass.getClassLoader.getResourceAsStream("test-jupyter-server.p12")

    require(keystore != null, "Keystore required!")
    ks.load(keystore, password)

    val keyManagerFactory: KeyManagerFactory = KeyManagerFactory.getInstance("SunX509")
    keyManagerFactory.init(ks, password)

    val tmf: TrustManagerFactory = TrustManagerFactory.getInstance("SunX509")
    tmf.init(ks)

    val sslContext: SSLContext = SSLContext.getInstance("TLS")
    sslContext.init(keyManagerFactory.getKeyManagers, tmf.getTrustManagers, new SecureRandom)
    val https: HttpsConnectionContext = ConnectionContext.https(sslContext)

    serverBinding = Http().bindAndHandle(backendRoute, "0.0.0.0", proxyConfig.jupyterPort, https).futureValue
  }

  def shutdownProxyServer() = {
    serverBinding.unbind().futureValue
  }

  // The backend route (i.e. the route behind the proxy)
  def backendRoute: Route =
    pathPrefix("notebooks" / googleProject / clusterName) {
      extractRequest { request =>
        // Jupyter sets Access-Control-Allow-Origin = *, so simulate that here
        respondWithHeader(`Access-Control-Allow-Origin`.*) {
          path("websocket") {
            handleWebSocketMessages(greeter)
          } ~
          // this path is so that we can test our fix for LEO-214 - cleaning "utf-8''" out of
          //  notebook download names. Jupyter usually adds the Content-Disposition header to the response.
          path("content-disposition-test") {
            complete{
              HttpResponse(headers = immutable.Seq(`Content-Disposition`(ContentDispositionTypes.attachment, Map("filename" -> "utf-8''notebook.ipynb"))))
            }
          } ~
          complete {
            Data(
              request.method.value,
              request.uri.path.toString,
              request.uri.queryString(),
              request.headers.map(h => h.name -> h.value).toMap
            )
          }
        }
      }
    }

  // A simple websocket handler
  def greeter: Flow[Message, Message, Any] =
    Flow[Message].mapConcat {
      case TextMessage.Strict(text) =>
        TextMessage.Strict("Hello " + text + "!") :: Nil

      case text: TextMessage =>
        TextMessage(Source.single("Hello ") ++ text.textStream ++ Source.single("!")) :: Nil

      case bm: BinaryMessage =>
        // ignore binary messages but drain content to avoid the stream being clogged
        bm.dataStream.runWith(Sink.ignore)
        Nil
    }

}

object TestProxy {
  // Convenience class for capturing HTTP data sent to the backend server and returning it back to the caller
  case class Data(method: String, path: String, qs: Option[String], headers: Map[String, String])
  implicit val DataFormat: RootJsonFormat[Data] = jsonFormat4(Data)
}
