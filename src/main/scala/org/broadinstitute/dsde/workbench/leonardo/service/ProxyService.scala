package org.broadinstitute.dsde.workbench.leonardo.service

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri.Host
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.errorReportSource
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.ModelTypes.GoogleProject
import org.broadinstitute.dsde.workbench.model.{ErrorReport, WorkbenchExceptionWithErrorReport}

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by rtitle on 8/15/17.
  */
class ProxyService(dbRef: DbReference)(implicit val system: ActorSystem, materializer: ActorMaterializer, executionContext: ExecutionContext) extends LazyLogging {
  private final val JupyterPort = 8000

  def proxy(googleProject: GoogleProject, clusterName: String, request: HttpRequest): Future[HttpResponse] = {
    getTargetHost(googleProject, clusterName).flatMap {
      case Some(targetHost) =>
        request.header[UpgradeToWebSocket] match {
          case Some(upgrade) => handleWebSocketRequest(targetHost, request, upgrade)
          case None => handleHttpRequest(targetHost, request)
        }
      case None =>
        println("in none")
        throw new WorkbenchExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, s"Could not find notebook server for $googleProject/$clusterName"))
    }
  }

  private def handleHttpRequest(targetHost: String, request: HttpRequest): Future[HttpResponse] = {
    logger.debug(s"Opening http connection to $targetHost")
    val flow = Http(system).outgoingConnection(targetHost, JupyterPort)
    val newHeaders = filterHeaders(request.headers)
    val newUri = Uri(path = request.uri.path, queryString = request.uri.queryString())
    val newRequest = request.copy(headers = newHeaders, uri = newUri)
    val handler = Source.single(newRequest)
      .via(flow)
      .runWith(Sink.head)
      .flatMap(_.toStrict(5 seconds))
    handler
  }

  private def handleWebSocketRequest(targetHost: String, request: HttpRequest, upgrade: UpgradeToWebSocket): Future[HttpResponse] = {
    logger.debug(s"Opening websocket connection to $targetHost")
    val flow = Flow.fromSinkAndSourceMat(Sink.asPublisher[Message](fanout = false), Source.asSubscriber[Message])(Keep.both)

    val (responseFuture, (publisher, subscriber)) = Http().singleWebSocketRequest(
      WebSocketRequest(request.uri.copy(authority = request.uri.authority.copy(host = Host(targetHost), port = JupyterPort)), extraHeaders = filterHeaders(request.headers),
        upgrade.requestedProtocols.headOption),
      flow
    )

    responseFuture.map {
      case ValidUpgrade(response, chosenSubprotocol) =>
        val webSocketResponse = upgrade.handleMessages(
          Flow.fromSinkAndSource(Sink.fromSubscriber(subscriber), Source.fromPublisher(publisher)),
          chosenSubprotocol
        )
        webSocketResponse.withHeaders(webSocketResponse.headers ++ filterHeaders(response.headers))

      case InvalidUpgradeResponse(response, cause) =>
        logger.warn("WebSocket upgrade response was invalid: {}", cause)
        response
    }
  }

  private def getTargetHost(googleProject: GoogleProject, clusterName: String): Future[Option[String]] = {
    dbRef.inTransaction { components =>
      components.clusterQuery.getByName(googleProject, clusterName)
    }.map(_.flatMap(_.hostIp))
  }

  private def filterHeaders(headers: immutable.Seq[HttpHeader]) = {
    headers.filterNot(header => HeadersToFilter(header.lowercaseName()))
  }

  private val HeadersToFilter = Set(
    "Timeout-Access",
    "Sec-WebSocket-Accept",
    "Sec-WebSocket-Version",
    "Sec-WebSocket-Key",
    "Sec-WebSocket-Extensions",
    "UpgradeToWebSocket",
    "Upgrade",
    "Connection"
  ).map(_.toLowerCase)
}
