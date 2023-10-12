package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri.Host
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{`Content-Disposition`, OAuth2BearerToken}
import akka.http.scaladsl.model.ws._
import akka.http.scaladsl.settings.ClientConnectionSettings
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.http.scaladsl.{ClientTransport, ConnectionContext, Http}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import cats.effect.IO
import cats.effect.std.Queue
import cats.mtl.Ask
import cats.syntax.all._
import com.auth0.jwt.JWT
import com.auth0.jwt.exceptions.JWTDecodeException
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId._
import org.broadinstitute.dsde.workbench.leonardo.config.ProxyConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.HostStatus._
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleOAuth2Service
import org.broadinstitute.dsde.workbench.leonardo.dao.{HostStatus, JupyterDAO, Proxy, SamDAO, TerminalName}
import org.broadinstitute.dsde.workbench.leonardo.db.{appQuery, clusterQuery, DbReference, KubernetesServiceDbQueries}
import org.broadinstitute.dsde.workbench.leonardo.dns.{KubernetesDnsCache, ProxyResolver, RuntimeDnsCache}
import org.broadinstitute.dsde.workbench.leonardo.http.service.ProxyService._
import org.broadinstitute.dsde.workbench.leonardo.http.service.SamResourceCacheKey.{AppCacheKey, RuntimeCacheKey}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.{UpdateDateAccessedMessage, UpdateTarget}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail, WorkbenchUserId}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.broadinstitute.dsde.workbench.util.toScalaDuration
import org.typelevel.log4cats.StructuredLogger
import scalacache.Cache

import java.time.Instant
import javax.net.ssl.SSLContext
import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
final case class HostContext(status: HostStatus, description: String)

sealed trait SamResourceCacheKey extends Product with Serializable
object SamResourceCacheKey {
  final case class RuntimeCacheKey(cloudContext: CloudContext, name: RuntimeName) extends SamResourceCacheKey {}
  final case class AppCacheKey(cloudContext: CloudContext, name: AppName, workspaceId: Option[WorkspaceId])
      extends SamResourceCacheKey
}

final case class ProxyHostNotReadyException(context: HostContext, traceId: TraceId)
    extends LeoException(
      s"Proxy host ${context.description} is not ready yet. It may be updating, try again later",
      StatusCodes.Locked,
      traceId = Some(traceId)
    )

final case class ProxyHostPausedException(context: HostContext, traceId: TraceId)
    extends LeoException(
      s"Proxy host ${context.description} is stopped. Start your runtime before proceeding.",
      StatusCodes.UnprocessableEntity,
      traceId = Some(traceId)
    )

case class ProxyHostNotFoundException(context: HostContext, traceId: TraceId)
    extends LeoException(s"Proxy host ${context.description} not found", StatusCodes.NotFound, traceId = Some(traceId))

final case class ProxyException(context: HostContext, traceId: TraceId)
    extends LeoException(s"Unable to proxy connection to tool on ${context.description}",
                         StatusCodes.InternalServerError,
                         traceId = Some(traceId)
    )

final case object AccessTokenExpiredException
    extends LeoException(s"Your access token is expired. Try logging in again",
                         StatusCodes.Unauthorized,
                         traceId = None
    )

class ProxyService(
  sslContext: SSLContext,
  proxyConfig: ProxyConfig,
  jupyterDAO: JupyterDAO[IO],
  runtimeDnsCache: RuntimeDnsCache[IO],
  kubernetesDnsCache: KubernetesDnsCache[IO],
  authProvider: LeoAuthProvider[IO],
  dateAccessUpdaterQueue: Queue[IO, UpdateDateAccessedMessage],
  googleOauth2Service: GoogleOAuth2Service[IO],
  proxyResolver: ProxyResolver[IO],
  samDAO: SamDAO[IO],
  googleTokenCache: Cache[IO, String, (UserInfo, Instant)],
  samResourceCache: Cache[IO, SamResourceCacheKey, (Option[String], Option[AppAccessScope])]
)(implicit
  val system: ActorSystem,
  executionContext: ExecutionContext,
  dbRef: DbReference[IO],
  loggerIO: StructuredLogger[IO],
  metrics: OpenTelemetryMetrics[IO]
) extends LazyLogging {
  val httpsConnectionContext = ConnectionContext.httpsClient(sslContext)
  val clientConnectionSettings =
    ClientConnectionSettings(system).withTransport(ClientTransport.withCustomResolver(proxyResolver.resolveAkka))

  final val requestTimeout = toScalaDuration(system.settings.config.getDuration("akka.http.server.request-timeout"))
  logger.info(s"Leo proxy request timeout is $requestTimeout")

  private[leonardo] def getUserInfo(token: String, now: Instant, checkUserEnabled: Boolean)(implicit
    ev: Ask[IO, TraceId]
  ): IO[(UserInfo, Instant)] =
    decodeB2cToken(token, now) match {
      case Left(_: JWTDecodeException) =>
        for {
          userInfo <- googleOauth2Service.getUserInfoFromToken(token)
          _ <-
            if (checkUserEnabled) for {
              // TODO [IA-4131] this code seems to assume a non-enabled user will return an error or an empty response. However,
              // HttpSamDAO::getSamUserInfo calls /register/user/v2/self/info which
              // (per https://sam.dsde-dev.broadinstitute.org/#/Users/getUserStatusInfo)
              // returns a JSON body with Boolean property `enabled`.
              // See also SamAuthProvider::lookupOriginatingUserEmail which looks for `enabled` on the response.
              samUserInfo <- samDAO.getSamUserInfo(token)
              _ <- IO.fromOption(samUserInfo)(AuthenticationError(Some(userInfo.userEmail)))
            } yield ()
            else IO.unit
        } yield (userInfo, now.plusSeconds(userInfo.tokenExpiresIn.toInt))
      case Left(e) =>
        IO.raiseError(e)
      case Right(value) =>
        IO.pure(value)
    }

  /* Ask the cache for the corresponding user info given a token */
  def getCachedUserInfoFromToken(token: String, checkUserEnabled: Boolean)(implicit
    ev: Ask[IO, TraceId]
  ): IO[UserInfo] =
    for {
      ctx <- ev.ask[TraceId]
      now <- IO.realTimeInstant

      cache <- googleTokenCache.cachingF(token)(None)(getUserInfo(token, now, checkUserEnabled)).handleErrorWith {
        case e: AuthenticationError =>
          loggerIO.error(Map("traceId" -> ctx.asString), e)(s"${e.message} due to ${e.extraMessage}") >> IO
            .raiseError(e)
        case e =>
          // Rethrow AuthenticationError if unable to look up the token
          loggerIO.error(Map("traceId" -> ctx.asString), e)(e.getMessage) >> IO.raiseError(AuthenticationError())
      }
      res <- cache match {
        case (userInfo, expiresAt) =>
          if (expiresAt.isAfter(now))
            IO.pure(userInfo.copy(tokenExpiresIn = expiresAt.getEpochSecond - now.getEpochSecond))
          else
            IO.raiseError(AccessTokenExpiredException)
      }
    } yield res

  private[leonardo] def getSamResourceFromDb(
    samResourceCacheKey: SamResourceCacheKey
  ): IO[(Option[String], Option[AppAccessScope])] =
    for {
      resourceId <- samResourceCacheKey match {
        case RuntimeCacheKey(cloudContext, name) =>
          clusterQuery.getActiveClusterInternalIdByName(cloudContext, name).map(_.map(_.resourceId)).transaction
        case AppCacheKey(cloudContext, name, workspaceId) =>
          cloudContext match {
            case CloudContext.Gcp(_) =>
              KubernetesServiceDbQueries
                .getActiveFullAppByName(cloudContext, name)
                .map(_.map(_.app.samResourceId.resourceId))
                .transaction
            case CloudContext.Azure(_) =>
              workspaceId match {
                case Some(w) =>
                  KubernetesServiceDbQueries
                    .getActiveFullAppByWorkspaceIdAndAppName(w, name)
                    .map(_.map(_.app.samResourceId.resourceId))
                    .transaction
                case None => IO(None)
              }
          }
      }
      appAccessScope <- samResourceCacheKey match {
        case RuntimeCacheKey(_, _) => IO(None) // Runtimes do not have an AppAccessScope field
        case AppCacheKey(cloudContext, name, workspaceId) =>
          cloudContext match {
            case CloudContext.Gcp(_) =>
              KubernetesServiceDbQueries
                .getActiveFullAppByName(cloudContext, name)
                .map(_.map(_.app.appAccessScope))
                .transaction
            case CloudContext.Azure(_) =>
              workspaceId match {
                case Some(w) =>
                  KubernetesServiceDbQueries
                    .getActiveFullAppByWorkspaceIdAndAppName(w, name)
                    .map(_.map(_.app.appAccessScope))
                    .transaction
                case None => IO(None)
              }
          }
      }
    } yield (resourceId, appAccessScope.flatten)

  def getCachedRuntimeSamResource(key: RuntimeCacheKey)(implicit
    ev: Ask[IO, AppContext]
  ): IO[RuntimeSamResourceId] =
    for {
      ctx <- ev.ask[AppContext]
      cacheResult <- samResourceCache.cachingF(key)(None)(
        getSamResourceFromDb(key)
      )
      cacheResourceId = cacheResult._1
      resourceId = cacheResourceId.map(RuntimeSamResourceId)
      res <- resourceId match {
        case Some(samResource) => IO.pure(samResource)
        case None =>
          IO.raiseError(
            RuntimeNotFoundException(
              key.cloudContext,
              key.name,
              s"${ctx.traceId} | Unable to look up sam resource for runtime ${key.cloudContext.asStringWithProvider} / ${key.name.asString}. Request: ${ctx.requestUri}"
            )
          )
      }
    } yield res

  def getCachedAppSamResource(key: AppCacheKey)(implicit
    ev: Ask[IO, AppContext]
  ): IO[AppSamResourceId] =
    for {
      ctx <- ev.ask[AppContext]
      cacheResult <- samResourceCache.cachingF(key)(None)(
        getSamResourceFromDb(key)
      )

      cacheSamResourceId = cacheResult._1
      cacheAppAccessScope = cacheResult._2
      resourceId = cacheSamResourceId.map(s => AppSamResourceId(s, cacheAppAccessScope))

      res <- resourceId match {
        case Some(samResource) => IO.pure(samResource)
        case None =>
          IO.raiseError(
            AppNotFoundException(
              key.cloudContext,
              key.name,
              ctx.traceId,
              s"Unable to look up sam resource for ${key.toString}"
            )
          )
      }
    } yield res

  def invalidateAccessToken(token: String): IO[Unit] =
    googleTokenCache.remove(token)

  def proxyRequest(userInfo: UserInfo,
                   cloudContext: CloudContext,
                   runtimeName: RuntimeName,
                   workspaceId: Option[WorkspaceId],
                   request: HttpRequest
  )(implicit
    ev: Ask[IO, AppContext]
  ): IO[HttpResponse] =
    for {
      ctx <- ev.ask[AppContext]

      hasWorkspacePermission <- workspaceId match {
        case Some(wid) =>
          authProvider
            .isUserWorkspaceReader(
              WorkspaceResourceSamResourceId(wid),
              userInfo
            )
        case None => IO.pure(true)
      }

      _ <- IO.raiseUnless(hasWorkspacePermission)(ForbiddenError(userInfo.userEmail))

      samResource <- getCachedRuntimeSamResource(RuntimeCacheKey(cloudContext, runtimeName))
      // Note both these Sam actions are cached so it should be okay to call hasPermission twice
      hasViewPermission <- authProvider.hasPermission[RuntimeSamResourceId, RuntimeAction](
        samResource,
        RuntimeAction.GetRuntimeStatus,
        userInfo
      )
      _ <-
        if (!hasViewPermission) {
          IO.raiseError(RuntimeNotFoundException(cloudContext, runtimeName, ctx.traceId.asString))
        } else IO.unit
      hasConnectPermission <- authProvider.hasPermission[RuntimeSamResourceId, RuntimeAction](
        samResource,
        RuntimeAction.ConnectToRuntime,
        userInfo
      )
      _ <-
        if (!hasConnectPermission) {
          IO.raiseError(ForbiddenError(userInfo.userEmail))
        } else IO.unit
      hostStatus <- getRuntimeTargetHost(cloudContext, runtimeName)
      _ <- hostStatus match {
        case HostReady(_, _, _) =>
          dateAccessUpdaterQueue.offer(
            UpdateDateAccessedMessage(UpdateTarget.Runtime(runtimeName), cloudContext, ctx.now)
          )
        case _ => IO.unit
      }
      hostContext = HostContext(hostStatus, s"${cloudContext.asStringWithProvider}/${runtimeName.asString}")
      r <- proxyInternal(hostContext, request)
      result = if (r.status.isSuccess()) "success" else "failure"

      tool = RuntimeContainerServiceType.values
        .find(s => request.uri.toString.contains(s.proxySegment))
        .map(_.imageType.entryName)
        .getOrElse("other")

      _ <- metrics.incrementCounter(
        "proxyRequest",
        tags = Map("result" -> result, "action" -> "runtimeRequest", "tool" -> s"${tool}")
      )
    } yield r

  def openTerminal(userInfo: UserInfo,
                   googleProject: GoogleProject,
                   runtimeName: RuntimeName,
                   terminalName: TerminalName,
                   request: HttpRequest
  )(implicit ev: Ask[IO, AppContext]): IO[HttpResponse] =
    for {
      terminalExists <- jupyterDAO.terminalExists(googleProject, runtimeName, terminalName)
      _ <-
        if (terminalExists)
          IO.unit
        else
          jupyterDAO.createTerminal(googleProject, runtimeName)
      r <- proxyRequest(userInfo, CloudContext.Gcp(googleProject), runtimeName, None, request)
      result = if (r.status.isSuccess()) "success" else "failure"

      tool = RuntimeContainerServiceType.values
        .find(s => request.uri.toString.contains(s.proxySegment))
        .map(_.imageType.entryName)
        .getOrElse("other")

      _ <- metrics.incrementCounter(
        "proxyRequest",
        tags = Map("result" -> result, "action" -> "openTerminal", "tool" -> s"${tool}")
      )
    } yield r

  def proxyAppRequest(userInfo: UserInfo,
                      cloudContext: CloudContext,
                      appName: AppName,
                      workspaceId: Option[WorkspaceId],
                      serviceName: ServiceName,
                      request: HttpRequest
  )(implicit
    ev: Ask[IO, AppContext]
  ): IO[HttpResponse] =
    for {
      ctx <- ev.ask[AppContext]

      hasWorkspacePermission <- workspaceId match {
        case Some(wid) =>
          authProvider
            .isUserWorkspaceReader(
              WorkspaceResourceSamResourceId(wid),
              userInfo
            )
        case None => IO.pure(true)
      }

      _ <- IO.raiseUnless(hasWorkspacePermission)(ForbiddenError(userInfo.userEmail))

      samResource <- getCachedAppSamResource(AppCacheKey(cloudContext, appName, workspaceId))
      // Note both these Sam actions are cached so it should be okay to call hasPermission twice
      hasViewPermission <- authProvider.hasPermission[AppSamResourceId, AppAction](samResource,
                                                                                   AppAction.GetAppStatus,
                                                                                   userInfo
      )
      _ <-
        if (!hasViewPermission) {
          IO.raiseError(
            AppNotFoundException(cloudContext, appName, ctx.traceId, "no view permission")
          )
        } else IO.unit
      hasConnectPermission <- authProvider.hasPermission[AppSamResourceId, AppAction](samResource,
                                                                                      AppAction.ConnectToApp,
                                                                                      userInfo
      )
      _ <-
        if (!hasConnectPermission) {
          IO.raiseError(ForbiddenError(userInfo.userEmail))
        } else IO.unit
      hostStatus <- getAppTargetHost(cloudContext, appName)
      _ <- hostStatus match {
        case HostReady(_, _, _) =>
          dateAccessUpdaterQueue.offer(UpdateDateAccessedMessage(UpdateTarget.App(appName), cloudContext, ctx.now))
        case _ => IO.unit
      }
      hostContext = HostContext(hostStatus, s"${cloudContext.asString}/${appName.value}/${serviceName.value}")
      r <- proxyInternal(hostContext, request)
      appType <- appQuery.getAppType(appName).transaction
      result = if (r.status.isSuccess()) "success" else "failure"
      _ <- metrics.incrementCounter(
        "proxyRequest",
        tags = Map("result" -> result, "action" -> "appRequest", "tool" -> s"${appType.getOrElse("unknown")}")
      )
    } yield r

  private[service] def getRuntimeTargetHost(cloudContext: CloudContext, runtimeName: RuntimeName): IO[HostStatus] =
    Proxy.getRuntimeTargetHost[IO](runtimeDnsCache, cloudContext, runtimeName)

  private[service] def getAppTargetHost(cloudContext: CloudContext, appName: AppName): IO[HostStatus] =
    Proxy.getAppTargetHost[IO](kubernetesDnsCache, cloudContext, appName)

  private def proxyInternal(hostContext: HostContext, request: HttpRequest)(implicit
    ev: Ask[IO, AppContext]
  ): IO[HttpResponse] =
    for {
      ctx <- ev.ask[AppContext]
      res <- hostContext.status match {
        case HostReady(targetHost, _, _) =>
          // If this is a WebSocket request (e.g. wss://leo:8080/...) then akka-http injects a
          // virtual UpgradeToWebSocket header which contains facilities to handle the WebSocket data.
          // The presence of this header distinguishes WebSocket from http requests.
          val res = for {
            response <- request.attribute(AttributeKeys.webSocketUpgrade) match {
              case Some(upgrade) =>
                IO.fromFuture(IO(handleWebSocketRequest(targetHost, request, upgrade)))
              case None =>
                IO.fromFuture(IO(handleHttpRequest(targetHost, request)))
            }
            r <-
              if (response.status.isFailure())
                loggerIO
                  .info(ctx.loggingCtx)(
                    s"Error response for proxied request ${request.uri}: ${response.status}, ${Unmarshal(response.entity.withSizeLimit(1024))
                        .to[String]}"
                  )
                  .as(response)
              else IO.pure(response)
          } yield r

          res.recoverWith { case e =>
            loggerIO.error(ctx.loggingCtx, e)(s"Error occurred in proxy") >> IO.raiseError[HttpResponse](
              ProxyException(hostContext, ctx.traceId)
            )
          }
        case HostNotReady =>
          loggerIO.warn(ctx.loggingCtx)(s"proxy host not ready for ${hostContext.description}") >> IO.raiseError(
            ProxyHostNotReadyException(hostContext, ctx.traceId)
          )
        case HostPaused =>
          loggerIO.warn(ctx.loggingCtx)(s"proxy host paused for ${hostContext.description}") >> IO.raiseError(
            ProxyHostPausedException(hostContext, ctx.traceId)
          )
        case HostNotFound =>
          loggerIO.warn(ctx.loggingCtx)(s"proxy host not found for ${hostContext.description}") >> IO.raiseError(
            ProxyHostNotFoundException(hostContext, ctx.traceId)
          )
      }
    } yield res

  private def handleHttpRequest(targetHost: Host, request: HttpRequest): Future[HttpResponse] = {
    logger.debug(s"Opening https connection to ${targetHost.address}:${proxyConfig.proxyPort}")

    // A note on akka-http philosophy:
    // The Akka HTTP server is implemented on top of Streams and makes heavy use of it. Requests come
    // in as a Source[HttpRequest] and responses are returned as a Sink[HttpResponse]. The transformation
    // from Source to Sink is done via a Flow[HttpRequest, HttpResponse, _].
    // For more information, see: http://doc.akka.io/docs/akka-http/10.0.9/scala/http/introduction.html

    // Initializes a Flow representing a prospective connection to the given endpoint. The connection
    // is not made until a Source and Sink are plugged into the Flow (i.e. it is materialized).
    val flow = Http()
      .connectionTo(targetHost.address)
      .toPort(proxyConfig.proxyPort)
      .withCustomHttpsConnectionContext(httpsConnectionContext)
      .withClientConnectionSettings(clientConnectionSettings)
      .https()

    // Now build a Source[Request] out of the original HttpRequest. We need to make some modifications
    // to the original request in order for the proxy to work:

    // Rewrite the path if it is proxy/*/*/jupyter/, otherwise pass it through as is (see rewriteJupyterPath)
    val rewrittenPath = rewriteJupyterPath(request.uri.path)

    // 1. filter out headers not needed for the backend server
    val newHeaders = filterHeaders(request.headers)
    // 2. strip out Uri.Authority:
    val newUri = Uri(path = rewrittenPath, queryString = request.uri.rawQueryString)
    // 3. build a new HttpRequest
    val newRequest = request.withHeaders(headers = newHeaders).withUri(uri = newUri)

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
      case Some(header) =>
        val keyName = "filename"
        header.params.get(keyName) match {
          case Some(fileName) =>
            val newFileName =
              fileName.replace("utf-8''", "") // a filename that doesn't have utf-8'' shouldn't be affected
            val newParams = header.params + (keyName -> newFileName)
            val newHeader = `Content-Disposition`(header.dispositionType, newParams)
            val newHeaders = httpResponse.headers.filter(header => header.isNot("content-disposition")) ++ Seq(
              newHeader
            )
            httpResponse.withHeaders(headers = newHeaders)
          case None => httpResponse
        }
      case None => httpResponse
    }

  private def handleWebSocketRequest(targetHost: Host,
                                     request: HttpRequest,
                                     upgrade: WebSocketUpgrade
  ): Future[HttpResponse] = {
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
                         scheme = "wss"
        ),
        extraHeaders = filterHeaders(request.headers),
        upgrade.requestedProtocols.headOption
      ),
      flow,
      httpsConnectionContext,
      settings = clientConnectionSettings
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

  private def filterHeaders(headers: immutable.Seq[HttpHeader]): immutable.Seq[HttpHeader] =
    headers.filterNot(header => HeadersToFilter(header.lowercaseName()))

  private val HeadersToFilter = Set(
    "Host",
    "Timeout-Access",
    "Sec-WebSocket-Accept",
    "Sec-WebSocket-Version",
    "Sec-WebSocket-Key",
    "Sec-WebSocket-Extensions",
    "Sec-WebSocket-Protocol",
    "UpgradeToWebSocket",
    "Upgrade",
    "Connection"
  ).map(_.toLowerCase)
}

object ProxyService {
  val proxyPattern = "\\/proxy\\/([^\\/]*)\\/([^\\/]*)\\/jupyter\\/?(.*)?".r
  val notebooksPattern = "\\/notebooks\\/([^\\/]*)\\/([^\\/]*)\\/jupyter\\/?(.*)?".r

  // From the outside, we are going to support TWO paths to access Jupyter:
  // 1)   /notebooks/{googleProject}/{clusterName}/
  // 2)   /proxy/{googleProject}/{clusterName}/jupyter/
  //
  // To greatly simplify things on the backend, we will funnel all requests into path #1. Eventually
  // we will remove that path and #2 will be the sole entry point for users. At that point, we can
  // update the code in here to not rewrite any paths for Jupyter. We will also need to update the
  // paths in related areas like jupyter_notebook_config.py
  def rewriteJupyterPath(path: Uri.Path): Uri.Path =
    path.toString match {
      case proxyPattern(project, cluster, path) =>
        Uri.Path("/notebooks/" + project + "/" + cluster + "/" + path)
      case notebooksPattern(project, cluster, path) =>
        Uri.Path("/notebooks/" + project + "/" + cluster + "/" + path)
      case _ => path
    }

  def decodeB2cToken(token: String, now: Instant): Either[Throwable, (UserInfo, Instant)] =
    for {
      decoded <- Either.catchNonFatal(JWT.decode(token))
      nonNullDecoded <- Either.fromOption(Option(decoded), AuthenticationError(extraMessage = "get null decoded token"))
      email <- Either.fromOption(Option(nonNullDecoded.getClaim("email")),
                                 AuthenticationError(extraMessage = "no email claim found")
      )
      subjectId = Option(nonNullDecoded.getClaim("google_id"))
        .orElse(Option(nonNullDecoded.getClaim("sub")))
        .map(x => x.asString())
      userId <- Either.fromOption(subjectId, AuthenticationError(extraMessage = "no google_id nor sub claim found"))
      expiresAt = nonNullDecoded.getExpiresAt.toInstant
      expiresIn <-
        if (expiresAt.isAfter(now))
          (expiresAt.getEpochSecond - now.getEpochSecond).asRight[Exception]
        else
          AccessTokenExpiredException.asLeft[Long]
    } yield (UserInfo(OAuth2BearerToken(token), WorkbenchUserId(userId), WorkbenchEmail(email.asString()), expiresIn),
             expiresAt
    )
}
