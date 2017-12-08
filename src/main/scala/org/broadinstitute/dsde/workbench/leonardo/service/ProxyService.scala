package org.broadinstitute.dsde.workbench.leonardo.service

import java.time.Instant

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri.Host
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.HttpCookiePair
import akka.http.scaladsl.model.ws._
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.Timeout
import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.typesafe.scalalogging.LazyLogging
import java.util.concurrent.TimeUnit

import cats.data.OptionT
import cats.implicits._
import org.broadinstitute.dsde.workbench.leonardo.config.ProxyConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.DataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.NotebookClusterActions._
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

case class ClusterNotReadyException(googleProject: GoogleProject, clusterName: ClusterName) extends LeoException(s"Cluster ${googleProject.value}/${clusterName.string} is not ready yet, chill out and try again later", StatusCodes.EnhanceYourCalm)
case class ProxyException(googleProject: GoogleProject, clusterName: ClusterName) extends LeoException(s"Unable to proxy connection to Jupyter notebook on ${googleProject.value}/${clusterName.string}", StatusCodes.InternalServerError)
case class AccessTokenExpiredException() extends LeoException(s"Your access token is expired. Try logging in again", StatusCodes.Unauthorized)
/**
  * Created by rtitle on 8/15/17.
  */
class ProxyService(proxyConfig: ProxyConfig,
                   gdDAO: DataprocDAO,
                   dbRef: DbReference,
                   clusterDnsCache: ActorRef,
                   authProvider: LeoAuthProvider)(implicit val system: ActorSystem, materializer: ActorMaterializer, executionContext: ExecutionContext) extends LazyLogging {

  /* Cache for the bearer token and corresponding google user email */
  private val cachedAuth = CacheBuilder.newBuilder()
    .expireAfterWrite(proxyConfig.cacheExpiryTime, TimeUnit.MINUTES)
    .maximumSize(proxyConfig.cacheMaxSize)
    .build(
      new CacheLoader[String, Future[(UserInfo, Instant)]] {
        def load(key: String) = {
          gdDAO.getUserInfoAndExpirationFromAccessToken(key)
        }
      }
    )

  /* Ask the cache for the corresponding user info given a token */
  def getCachedUserInfoFromToken(token: String): Future[UserInfo] = {
    cachedAuth.get(token).map {
      case (userInfo, expireTime) =>
        if (expireTime.isAfter(Instant.now))
          userInfo.copy(tokenExpiresIn = expireTime.toEpochMilli - Instant.now.toEpochMilli)
        else
          throw AccessTokenExpiredException() }
  }

  def proxyLocalize(userInfo: UserInfo, googleProject: GoogleProject, clusterName: ClusterName, request: HttpRequest, token: HttpCookiePair): Future[HttpResponse] = {
    //check auth to see it
    val authCheck = for {
      hasViewPermission <- authProvider.hasNotebookClusterPermission(userInfo, GetClusterStatus, googleProject.value, clusterName.string)
      hasSyncPermission <- authProvider.hasNotebookClusterPermission(userInfo, SyncDataToCluster, googleProject.value, clusterName.string)
    } yield {
      if (!hasViewPermission) {
        throw ClusterNotFoundException(googleProject, clusterName)
      } else if (!hasSyncPermission) {
        throw AuthorizationError(userInfo.userEmail)
      } else {
        ()
      }
    }
    authCheck flatMap { _ => proxyInternal(userInfo, googleProject, clusterName, request, token) }
  }

  /**
    * Entry point to this class. Given a google project, cluster name, and HTTP request,
    * looks up the notebook server IP and proxies the HTTP request to the notebook server.
    * Returns NotFound if a notebook server IP could not be found for the project/cluster name.
    * @param userInfo the current user
    * @param googleProject the Google project
    * @param clusterName the cluster name
    * @param request the HTTP request to proxy
    * @param token the user access token
    * @return HttpResponse future representing the proxied response, or NotFound if a notebook
    *         server IP could not be found.
    */
  def proxyNotebook(userInfo: UserInfo, googleProject: GoogleProject, clusterName: ClusterName, request: HttpRequest, token: HttpCookiePair): Future[HttpResponse] = {
    //check auth to see it
    val authCheck = for {
      hasViewPermission <- authProvider.hasNotebookClusterPermission(userInfo, GetClusterStatus, googleProject.value, clusterName.string)
      hasConnectPermission <- authProvider.hasNotebookClusterPermission(userInfo, ConnectToCluster, googleProject.value, clusterName.string)
    } yield {
      if (!hasViewPermission) {
        throw ClusterNotFoundException(googleProject, clusterName)
      } else if (!hasConnectPermission) {
        throw AuthorizationError(userInfo.userEmail)
      } else {
        ()
      }
    }
    authCheck flatMap { _ => proxyInternal(userInfo, googleProject, clusterName, request, token) }
  }

  private def proxyInternal(userInfo: UserInfo, googleProject: GoogleProject, clusterName: ClusterName, request: HttpRequest, token: HttpCookiePair): Future[HttpResponse] = {
    logger.debug(s"Received proxy request with user token ${token.value}")
    getTargetHost(googleProject, clusterName) flatMap {
      case ClusterReady(targetHost) =>
        // If this is a WebSocket request (e.g. wss://leo:8080/...) then akka-http injects a
        // virtual UpgradeToWebSocket header which contains facilities to handle the WebSocket data.
        // The presence of this header distinguishes WebSocket from http requests.
        val responseFuture = request.header[UpgradeToWebSocket] match {
          case Some(upgrade) => handleWebSocketRequest(targetHost, request, upgrade)
          case None => handleHttpRequest(targetHost, request)
        }
        responseFuture recover { case e =>
          logger.error("Error occurred in Jupyter proxy", e)
          throw ProxyException(googleProject, clusterName)
        }
      case ClusterNotReady =>
        throw ClusterNotReadyException(googleProject, clusterName)
      case ClusterNotFound =>
        throw ClusterNotFoundException(googleProject, clusterName)
    }
  }

  private def handleHttpRequest(targetHost: Host, request: HttpRequest): Future[HttpResponse] = {
    logger.debug(s"Opening https connection to ${targetHost.address}:${proxyConfig.jupyterPort}")

    // A note on akka-http philosophy:
    // The Akka HTTP server is implemented on top of Streams and makes heavy use of it. Requests come
    // in as a Source[HttpRequest] and responses are returned as a Sink[HttpResponse]. The transformation
    // from Source to Sink is done via a Flow[HttpRequest, HttpResponse, _].
    // For more information, see: http://doc.akka.io/docs/akka-http/10.0.9/scala/http/introduction.html

    // Initializes a Flow representing a prospective connection to the given endpoint. The connection
    // is not made until a Source and Sink are plugged into the Flow (i.e. it is materialized).
    val flow = Http().outgoingConnectionHttps(targetHost.address, proxyConfig.jupyterPort)

    // Now build a Source[Request] out of the original HttpRequest. We need to make some modifications
    // to the original request in order for the proxy to work:

    // 1. filter out headers not needed for the backend server
    val newHeaders = filterHeaders(request.headers)
    // 2. strip out Uri.Authority:
    val newUri = Uri(path = request.uri.path, queryString = request.uri.queryString())
    // 3. build a new HttpRequest
    val newRequest = request.copy(headers = newHeaders, uri = newUri)

    // Plug a Source and Sink into our Flow. This materializes the Flow and initializes the HTTP connection
    // to the notebook server.
    // - the Source is the modified HttpRequest from above
    // - the Sink just takes the first element because we only expect 1 response

    // Note: we're calling toStrict here which forces the proxy to load the entire response into memory before
    // returning it to the caller. Technically, this should not be required: we should be able to stream all
    // data between client, proxy, and server. However Jupyter is doing something strange and the proxy only
    // works when toStrict is used. Luckily, it's only needed for HTTP requests (which are fairly small) and not
    // WebSocket requests (which could potentially be large).
    val handler: Future[HttpResponse] = Source.single(newRequest)
      .via(flow)
      .runWith(Sink.head)
      .flatMap(_.toStrict(5 seconds))

    // That's it! This is our whole HTTP proxy.
    handler
  }

  private def handleWebSocketRequest(targetHost: Host, request: HttpRequest, upgrade: UpgradeToWebSocket): Future[HttpResponse] = {
    logger.debug(s"Opening websocket connection to ${targetHost.address}")

    // This is a similar idea to handleHttpRequest(), we're just using WebSocket APIs instead of HTTP ones.
    // The basis for this method was lifted from https://github.com/akka/akka-http/issues/1289#issuecomment-316269886.

    // Initialize a Flow for the WebSocket conversation.
    // `Message` is the root of the ADT for WebSocket messages. A Message may be a TextMessage or a BinaryMessage.
    val flow = Flow.fromSinkAndSourceMat(Sink.asPublisher[Message](fanout = false), Source.asSubscriber[Message])(Keep.both)

    // Make a single WebSocketRequest to the notebook server, passing in our Flow. This returns a Future[WebSocketUpgradeResponse].
    // Keep our publisher/subscriber (e.g. sink/source) for use later. These are returned because we specified Keep.both above.
    val (responseFuture, (publisher, subscriber)) = Http().singleWebSocketRequest(
      WebSocketRequest(request.uri.copy(authority = request.uri.authority.copy(host = targetHost, port = proxyConfig.jupyterPort), scheme = "wss"), extraHeaders = filterHeaders(request.headers),
        upgrade.requestedProtocols.headOption),
      flow
    )

    // If we got a valid WebSocketUpgradeResponse, call handleMessages with our publisher/subscriber, which are
    // already materialized from the HttpRequest.
    // If we got an invalid WebSocketUpgradeResponse, simply return it without proxying any additional messages.
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

  /**
    * Gets the notebook server hostname from the database given a google project and cluster name.
    */
  protected def getTargetHost(googleProject: GoogleProject, clusterName: ClusterName): Future[GetClusterResponse] = {
    implicit val timeout: Timeout = Timeout(5 seconds)
    (clusterDnsCache ? GetByProjectAndName(googleProject, clusterName)).mapTo[GetClusterResponse]
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
