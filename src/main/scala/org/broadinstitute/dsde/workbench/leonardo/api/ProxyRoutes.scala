package org.broadinstitute.dsde.workbench.leonardo.api

import akka.actor.ActorSystem
import akka.http.javadsl.model
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri.{Host, Path}
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.model.ws._
import akka.http.scaladsl.server.{RequestContext, Route}
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl._
import com.typesafe.scalalogging.LazyLogging
import com.typesafe.sslconfig.akka.AkkaSSLConfig

import scala.concurrent.duration._
import scala.collection.immutable
import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext, Future}

/**
  * Created by rtitle on 8/4/17.
  */
trait ProxyRoutes { self: LazyLogging =>
  implicit val system: ActorSystem
  implicit val materializer: Materializer
  implicit val executionContext: ExecutionContext

  //val target = "try.jupyter.org"
  //val target = "tmp59.tmpnb.org"
  val target = "jupyter.local.com"

  val proxyRoutes = Route { context =>
    val request = context.request
    logger.info("context.request is " + request.uri.path)
    context.complete {
      request.header[UpgradeToWebSocket] match {
        case Some(upgrade) => handleWebSocketRequest(request, upgrade)
        case None => handleHttpRequest(request)
      }
    }
  }

  private def handleHttpRequest(request: HttpRequest): Future[HttpResponse] = {
  //  logger.info(s"Opening http connection to ${request.uri.authority.host.address}:${request.uri.authority.port}${request.uri.path}")
    val flow = Http(system).outgoingConnection(target, port = 8888)

//    val newHeaders = filterHeaders(request.getHeaders().asScala.map {
//      case akka.http.scaladsl.model.headers.Host(_, _) => akka.http.scaladsl.model.headers.Host(target)
//      case h@HttpHeader(_) => h
//    }.to[immutable.Seq])
//    val newRequest = request.copy(headers = newHeaders)
//    println("headers = " + newRequest.headers)
    //val newRequest = request.copy(headers = filterHeaders(request.headers), uri = request.uri.copy(authority = request.uri.authority.copy(port = 8888)))
    //val newRequest = HttpRequest(uri = request.uri.copy(authority = request.uri.authority.copy(port = 8888)))
    //logger.info(s"newRequest = $newRequest")
    val newRequest = request.copy(headers = filterHeaders(request.headers), uri = Uri(path = request.uri.path, queryString = request.uri.queryString()))
    val handler = Source.single(newRequest)
      .via(flow)
      .runWith(Sink.head)
      .flatMap(_.toStrict(5 seconds))
    handler
  }

  private def handleWebSocketRequest(request: HttpRequest, upgrade: UpgradeToWebSocket): Future[HttpResponse] = {
    logger.info("Opening websocket connection to " + request.uri.authority.host.address)
    val flow = Flow.fromSinkAndSourceMat(Sink.asPublisher[Message](fanout = false), Source.asSubscriber[Message])(Keep.both)

    val (responseFuture, (publisher, subscriber)) = Http().singleWebSocketRequest(
      WebSocketRequest(request.uri.copy(authority = request.uri.authority.copy(port = 8888)), extraHeaders = filterHeaders(request.headers),
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
        logger.info("WebSocket upgrade response was invalid: {}", cause)
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
