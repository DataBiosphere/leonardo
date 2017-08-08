package org.broadinstitute.dsde.workbench.leonardo.api

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri.Host
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws._
import akka.http.scaladsl.server.Route
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl._
import com.typesafe.scalalogging.LazyLogging

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by rtitle on 8/4/17.
  */
trait ProxyRoutes { self: LazyLogging =>
  implicit val system: ActorSystem
  implicit val materializer: Materializer
  implicit val executionContext: ExecutionContext

  val target = "try.jupyter.org"

  val proxyRoutes = Route { context =>
    val request = context.request
    context.complete {
      request.header[UpgradeToWebSocket] match {
        case Some(upgrade) => handleWebSocketRequest(request, upgrade)
        case None => handleHttpRequest(request)
      }
    }
  }

  private def handleHttpRequest(request: HttpRequest): Future[HttpResponse] = {
    logger.debug("Opening connection to " + request.uri.authority.host.address)
    val newAuthority = request.uri.authority.copy(host = Host(target))
    val newUri = request.uri.copy(authority = newAuthority)
    val newRequest = request.copy(uri = newUri)
    val flow = Http(system).outgoingConnection(newAuthority.host.address, 80)
    Source.single(newRequest)
      .via(flow)
      .runWith(Sink.head)
  }

  private def handleWebSocketRequest(request: HttpRequest, upgrade: UpgradeToWebSocket): Future[HttpResponse] = {
    val flow = Flow.fromSinkAndSourceMat(Sink.asPublisher[Message](fanout = false), Source.asSubscriber[Message])(Keep.both)

    val (responseFuture, (publisher, subscriber)) = Http().singleWebSocketRequest(
      WebSocketRequest(target, extraHeaders = filterHeaders(request.headers),
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
        logger.debug("WebSocket upgrade response was invalid: {}", cause)
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
    "UpgradeToWebSocket",
    "Upgrade",
    "Connection"
  ).map(_.toLowerCase)


}
