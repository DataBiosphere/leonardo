package org.broadinstitute.dsde.workbench.leonardo.api

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import akka.stream.scaladsl._
import com.typesafe.scalalogging.LazyLogging

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by rtitle on 8/4/17.
  */
trait ProxyRoutes { self: LazyLogging =>
  implicit val system: ActorSystem
  implicit val materializer: Materializer
  implicit val executionContext: ExecutionContext

  val jupyterPort = 8000

  val proxyRoutes: Route =
    pathPrefix(Segment) { notebookId =>  // TODO notebookId not currently used
      extractRequest { request =>
        complete {
          request.header[UpgradeToWebSocket] match {
            case Some(upgrade) => handleWebSocketRequest(request, upgrade)
            case None => handleHttpRequest(request)
          }
        }
      }
    }

  private def handleHttpRequest(request: HttpRequest): Future[HttpResponse] = {
    logger.info(s"Opening http connection to ${request.uri.authority.host.address}")
    // TODO eventually we will lookup the outgoing address from the notebook ID
    val flow = Http(system).outgoingConnection(request.uri.authority.host.address, jupyterPort)
    val newHeaders = filterHeaders(request.headers)
    val newUri = Uri(path = request.uri.path, queryString = request.uri.queryString())
    val newRequest = request.copy(headers = newHeaders, uri = newUri)
    val handler = Source.single(newRequest)
      .via(flow)
      .runWith(Sink.head)
      .flatMap(_.toStrict(5 seconds))
    handler
  }

  private def handleWebSocketRequest(request: HttpRequest, upgrade: UpgradeToWebSocket): Future[HttpResponse] = {
    logger.info(s"Opening websocket connection to ${request.uri.authority.host.address}")
    val flow = Flow.fromSinkAndSourceMat(Sink.asPublisher[Message](fanout = false), Source.asSubscriber[Message])(Keep.both)

    // TODO eventually will lookup the outgoing address from the notebook ID
    val (responseFuture, (publisher, subscriber)) = Http().singleWebSocketRequest(
      WebSocketRequest(request.uri.copy(authority = request.uri.authority.copy(port = jupyterPort)), extraHeaders = filterHeaders(request.headers),
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
