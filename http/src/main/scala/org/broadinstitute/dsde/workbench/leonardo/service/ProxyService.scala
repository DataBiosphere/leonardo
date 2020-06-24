package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import java.time.Instant
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri.Host
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.`Content-Disposition`
import akka.http.scaladsl.model.ws._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import cats.effect.{Blocker, ContextShift, IO, Timer}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.SamResource.RuntimeSamResource
import fs2.concurrent.InspectableQueue
import org.broadinstitute.dsde.workbench.leonardo.config.ProxyConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.Proxy
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, DbReference}
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache._
import org.broadinstitute.dsde.workbench.leonardo.http.service.ProxyService._
import org.broadinstitute.dsde.workbench.leonardo.model.RuntimeAction._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.UpdateDateAccessMessage
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo}
import org.broadinstitute.dsde.workbench.util.toScalaDuration

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

final case class RuntimeNotReadyException(googleProject: GoogleProject, runtimeName: RuntimeName)
    extends LeoException(
      s"Runtime ${googleProject.value}/${runtimeName.asString} is not ready yet. It may be updating, try again later",
      StatusCodes.Locked
    )

final case class RuntimePausedException(googleProject: GoogleProject, runtimeName: RuntimeName)
    extends LeoException(
      s"Runtime ${googleProject.value}/${runtimeName.asString} is stopped. Start your runtime before proceeding.",
      StatusCodes.UnprocessableEntity
    )

final case class ProxyException(googleProject: GoogleProject, runtimeName: RuntimeName)
    extends LeoException(s"Unable to proxy connection to tool on ${googleProject.value}/${runtimeName.asString}",
                         StatusCodes.InternalServerError)

final case object AccessTokenExpiredException
    extends LeoException(s"Your access token is expired. Try logging in again", StatusCodes.Unauthorized)

/**
 * Created by rtitle on 8/15/17.
 */
class ProxyService(
  proxyConfig: ProxyConfig,
  gdDAO: GoogleDataprocDAO,
  clusterDnsCache: ClusterDnsCache[IO],
  authProvider: LeoAuthProvider[IO],
  dateAccessUpdaterQueue: InspectableQueue[IO, UpdateDateAccessMessage],
  blocker: Blocker
)(implicit val system: ActorSystem,
  executionContext: ExecutionContext,
  timer: Timer[IO],
  cs: ContextShift[IO],
  dbRef: DbReference[IO])
    extends LazyLogging {

  final val requestTimeout = toScalaDuration(system.settings.config.getDuration("akka.http.server.request-timeout"))
  logger.info(s"Leo proxy request timeout is $requestTimeout")

  /* Cache for the bearer token and corresponding google user email */
  private[leonardo] val googleTokenCache = CacheBuilder
    .newBuilder()
    .expireAfterWrite(proxyConfig.tokenCacheExpiryTime.toSeconds, TimeUnit.SECONDS)
    .maximumSize(proxyConfig.tokenCacheMaxSize)
    .build(
      new CacheLoader[String, Future[(UserInfo, Instant)]] {
        def load(key: String): Future[(UserInfo, Instant)] =
          gdDAO.getUserInfoAndExpirationFromAccessToken(key)
      }
    )

  /* Ask the cache for the corresponding user info given a token */
  def getCachedUserInfoFromToken(token: String): IO[UserInfo] =
    for {
      now <- timer.clock.realTime(TimeUnit.MILLISECONDS)
      cache <- blocker.blockOn(IO.fromFuture(IO(googleTokenCache.get(token))))
      res <- cache match {
        case (userInfo, expireTime) =>
          if (expireTime.isAfter(Instant.ofEpochMilli(now)))
            IO.pure(userInfo.copy(tokenExpiresIn = expireTime.getEpochSecond - now / 1000))
          else
            IO.raiseError(AccessTokenExpiredException)
      }
    } yield res

  /* Cache for the runtime sam resource from the database */
  private[leonardo] val runtimeSamResourceCache = CacheBuilder
    .newBuilder()
    .expireAfterWrite(proxyConfig.internalIdCacheExpiryTime.toSeconds, TimeUnit.SECONDS)
    .maximumSize(proxyConfig.internalIdCacheMaxSize)
    .build(
      new CacheLoader[(GoogleProject, RuntimeName), Option[RuntimeSamResource]] {
        def load(key: (GoogleProject, RuntimeName)): Option[RuntimeSamResource] = {
          val (googleProject, runtimeName) = key
          clusterQuery
            .getActiveClusterInternalIdByName(googleProject, runtimeName)
            .transaction
            .unsafeRunSync()
        }
      }
    )

  def getCachedRuntimeSamResource(googleProject: GoogleProject, runtimeName: RuntimeName)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[RuntimeSamResource] =
    blocker.blockOn(IO(runtimeSamResourceCache.get((googleProject, runtimeName)))).flatMap {
      case Some(runtimeSamResource) => IO.pure(runtimeSamResource)
      case None =>
        IO(
          logger.error(
            s"${ev.ask.unsafeRunSync()} | Unable to look up sam resource for runtime ${googleProject.value} / ${runtimeName.asString}"
          )
        ) >> IO.raiseError[RuntimeSamResource](
          RuntimeNotFoundException(
            googleProject,
            runtimeName,
            s"Unable to look up sam resource for runtime ${googleProject.value} / ${runtimeName.asString}"
          )
        )
    }

  /*
   * Checks the user has the required notebook action, returning 401 or 404 depending on whether they can know the runtime exists
   */
  private[leonardo] def authCheck(
    userInfo: UserInfo,
    googleProject: GoogleProject,
    runtimeName: RuntimeName,
    notebookAction: RuntimeAction
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    for {
      samResource <- getCachedRuntimeSamResource(googleProject, runtimeName)
      hasViewPermission <- authProvider
        .hasRuntimePermission(samResource, userInfo, GetRuntimeStatus, googleProject)
      //TODO: combine the sam calls into one
      hasRequiredPermission <- authProvider
        .hasRuntimePermission(samResource, userInfo, notebookAction, googleProject)
      _ <- if (!hasViewPermission) {
        IO.raiseError(RuntimeNotFoundException(googleProject, runtimeName, s"${notebookAction} permission is required"))
      } else if (!hasRequiredPermission) {
        IO.raiseError(AuthorizationError(Some(userInfo.userEmail)))
      } else IO.unit
    } yield ()

  def proxyLocalize(userInfo: UserInfo, googleProject: GoogleProject, runtimeName: RuntimeName, request: HttpRequest)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[HttpResponse] =
    for {
      _ <- authCheck(userInfo, googleProject, runtimeName, SyncDataToRuntime)
      now <- timer.clock.realTime(TimeUnit.MILLISECONDS)
      r <- proxyInternal(googleProject, runtimeName, request, Instant.ofEpochMilli(now))
    } yield r

  def invalidateAccessToken(token: String): IO[Unit] =
    blocker.blockOn(IO(googleTokenCache.invalidate(token)))

  /**
   * Entry point to this class. Given a google project, cluster name, and HTTP request,
   * looks up the notebook server IP and proxies the HTTP request to the notebook server.
   * Returns NotFound if a notebook server IP could not be found for the project/cluster name.
   * @param userInfo the current user
   * @param googleProject the Google project
   * @param runtimeName the cluster name
   * @param request the HTTP request to proxy
   * @return HttpResponse future representing the proxied response, or NotFound if a notebook
   *         server IP could not be found.
   */
  def proxyRequest(userInfo: UserInfo, googleProject: GoogleProject, runtimeName: RuntimeName, request: HttpRequest)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[HttpResponse] =
    for {
      _ <- authCheck(userInfo, googleProject, runtimeName, ConnectToRuntime)
      now <- timer.clock.realTime(TimeUnit.MILLISECONDS)
      r <- proxyInternal(googleProject, runtimeName, request, Instant.ofEpochMilli(now))
    } yield r

  def getTargetHost(googleProject: GoogleProject, runtimeName: RuntimeName): IO[HostStatus] =
    Proxy.getTargetHost[IO](clusterDnsCache, googleProject, runtimeName)

  private def proxyInternal(googleProject: GoogleProject,
                            runtimeName: RuntimeName,
                            request: HttpRequest,
                            now: Instant): IO[HttpResponse] = {
    logger.debug(
      s"Received proxy request for ${googleProject}/${runtimeName}: ${clusterDnsCache.stats} / ${clusterDnsCache.size}"
    )
    getTargetHost(googleProject, runtimeName) flatMap {
      case HostReady(targetHost) =>
        // If this is a WebSocket request (e.g. wss://leo:8080/...) then akka-http injects a
        // virtual UpgradeToWebSocket header which contains facilities to handle the WebSocket data.
        // The presence of this header distinguishes WebSocket from http requests.
        val res = for {
          _ <- dateAccessUpdaterQueue.enqueue1(UpdateDateAccessMessage(runtimeName, googleProject, now))
          response <- request.header[UpgradeToWebSocket] match {
            case Some(upgrade) =>
              IO.fromFuture(IO(handleWebSocketRequest(targetHost, request, upgrade)))
            case None =>
              IO.fromFuture(IO(handleHttpRequest(targetHost, request)))
          }
          r <- if (response.status.isFailure())
            IO(
              logger.info(
                s"Error response for proxied request ${request.uri}: ${response.status}, ${Unmarshal(response.entity.withSizeLimit(1024))
                  .to[String]}"
              )
            ).as(response)
          else IO.pure(response)
        } yield r

        res.recoverWith {
          case e =>
            IO(logger.error("Error occurred in proxy", e)) >> IO.raiseError[HttpResponse](
              ProxyException(googleProject, runtimeName)
            )
        }
      case HostNotReady =>
        IO(logger.warn(s"proxy host not ready for ${googleProject}/${runtimeName}")) >> IO.raiseError(
          RuntimeNotReadyException(googleProject, runtimeName)
        )
      case HostPaused =>
        IO(logger.warn(s"proxy host paused for ${googleProject}/${runtimeName}")) >> IO.raiseError(
          RuntimePausedException(googleProject, runtimeName)
        )
      case HostNotFound =>
        IO(logger.warn(s"proxy host not found for ${googleProject}/${runtimeName}")) >> IO.raiseError(
          RuntimeNotFoundException(googleProject, runtimeName, "proxy host not found")
        )
    }
  }

  private def handleHttpRequest(targetHost: Host, request: HttpRequest): Future[HttpResponse] = {
    logger.debug(s"Opening https connection to ${targetHost.address}:${proxyConfig.proxyPort}")

    // A note on akka-http philosophy:
    // The Akka HTTP server is implemented on top of Streams and makes heavy use of it. Requests come
    // in as a Source[HttpRequest] and responses are returned as a Sink[HttpResponse]. The transformation
    // from Source to Sink is done via a Flow[HttpRequest, HttpResponse, _].
    // For more information, see: http://doc.akka.io/docs/akka-http/10.0.9/scala/http/introduction.html

    // Initializes a Flow representing a prospective connection to the given endpoint. The connection
    // is not made until a Source and Sink are plugged into the Flow (i.e. it is materialized).
    val flow = Http().outgoingConnectionHttps(targetHost.address, proxyConfig.proxyPort)

    // Now build a Source[Request] out of the original HttpRequest. We need to make some modifications
    // to the original request in order for the proxy to work:

    // Rewrite the path if it is proxy/*/*/jupyter/, otherwise pass it through as is (see rewriteJupyterPath)
    val rewrittenPath = rewriteJupyterPath(request.uri.path)

    // 1. filter out headers not needed for the backend server
    val newHeaders = filterHeaders(request.headers)
    // 2. strip out Uri.Authority:
    val newUri = Uri(path = rewrittenPath, queryString = request.uri.queryString())
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
    Source
      .single(newRequest)
      .via(flow)
      .map(fixContentDisposition)
      .runWith(Sink.head)
      .flatMap(_.toStrict(requestTimeout))
  }

  // This is our current workaround for a bug that causes notebooks to download with "utf-8''" prepended to the file name
  // This is due to an akka-http bug currently being worked on here: https://github.com/playframework/playframework/issues/7719
  // For now, for each response that has a Content-Disposition header with a 'filename' in the header params, we remove any "utf-8''".
  // Should not affect any other responses.
  private def fixContentDisposition(httpResponse: HttpResponse): HttpResponse =
    httpResponse.header[`Content-Disposition`] match {
      case Some(header) => {
        val keyName = "filename"
        header.params.get(keyName) match {
          case Some(fileName) => {
            val newFileName = fileName.replace("utf-8''", "") // a filename that doesn't have utf-8'' shouldn't be affected
            val newParams = header.params + (keyName -> newFileName)
            val newHeader = `Content-Disposition`(header.dispositionType, newParams)
            val newHeaders = httpResponse.headers.filter(header => header.isNot("content-disposition")) ++ Seq(
              newHeader
            )
            httpResponse.copy(headers = newHeaders)
          }
          case None => httpResponse
        }
      }
      case None => httpResponse
    }

  private def handleWebSocketRequest(targetHost: Host,
                                     request: HttpRequest,
                                     upgrade: UpgradeToWebSocket): Future[HttpResponse] = {
    logger.info(s"Opening websocket connection to ${targetHost.address}")

    // This is a similar idea to handleHttpRequest(), we're just using WebSocket APIs instead of HTTP ones.
    // The basis for this method was lifted from https://github.com/akka/akka-http/issues/1289#issuecomment-316269886.

    // Initialize a Flow for the WebSocket conversation.
    // `Message` is the root of the ADT for WebSocket messages. A Message may be a TextMessage or a BinaryMessage.
    val flow =
      Flow.fromSinkAndSourceMat(Sink.asPublisher[Message](fanout = false), Source.asSubscriber[Message])(
        Keep.both
      )

    // Make a single WebSocketRequest to the notebook server, passing in our Flow. This returns a Future[WebSocketUpgradeResponse].
    // Keep our publisher/subscriber (e.g. sink/source) for use later. These are returned because we specified Keep.both above.
    // Note that we are rewriting the paths for any requests that are routed to /proxy/*/*/jupyter/
    val (responseFuture, (publisher, subscriber)) = Http().singleWebSocketRequest(
      WebSocketRequest(
        request.uri.copy(path = rewriteJupyterPath(request.uri.path),
                         authority = request.uri.authority.copy(host = targetHost, port = proxyConfig.proxyPort),
                         scheme = "wss"),
        extraHeaders = filterHeaders(request.headers),
        upgrade.requestedProtocols.headOption
      ),
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

  private def filterHeaders(headers: immutable.Seq[HttpHeader]) =
    headers.filterNot(header => HeadersToFilter(header.lowercaseName()))

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

object ProxyService {
  // From the outside, we are going to support TWO paths to access Jupyter:
  // 1)   /notebooks/{googleProject}/{clusterName}/
  // 2)   /proxy/{googleProject}/{clusterName}/jupyter/
  // 3)   /notebooks/{googleProject}/{clusterName}/jupyter/
  //
  // To greatly simplify things on the backend, we will funnel all requests into path #1. Eventually
  // we will remove that path and #2 will be the sole entry point for users. At that point, we can
  // update the code in here to not rewrite any paths for Jupyter. We will also need to update the
  // paths in related areas like jupyter_notebook_config.py
  def rewriteJupyterPath(path: Uri.Path): Uri.Path = {
    val proxyPattern = "\\/proxy\\/([^\\/]*)\\/([^\\/]*)\\/jupyter\\/?(.*)?".r
    val notebooksPattern = "\\/notebooks\\/([^\\/]*)\\/([^\\/]*)\\/jupyter\\/?(.*)?".r

    path.toString match {
      case proxyPattern(project, cluster, path) => {
        Uri.Path("/notebooks/" + project + "/" + cluster + "/" + path)
      }
      case notebooksPattern(project, cluster, path) => {
        Uri.Path("/notebooks/" + project + "/" + cluster + "/" + path)
      }
      case _ => path
    }
  }
}
