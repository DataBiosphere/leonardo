package org.broadinstitute.dsde.workbench.leonardo.service

import java.time.Instant
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri.Host
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import cats.effect.{Async, Blocker, ContextShift, IO, Timer}
import cats.mtl.ApplicativeAsk
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.google2.GKEService
import org.broadinstitute.dsde.workbench.leonardo.RuntimeName
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.http.service.{AuthorizationError, ProxyException, RuntimeNotFoundException}
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoAuthProvider, LeoException, RuntimeAction}
import org.broadinstitute.dsde.workbench.leonardo.model.RuntimeAction.{ConnectToRuntime, GetRuntimeStatus}
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import cats.implicits._
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache.{HostNotFound, HostNotReady, HostPaused, HostReady, HostStatus}

import scala.concurrent.{ExecutionContext, Future}

class KubernetesProxyService[F[_]](
                              kubernetesProxyConfig: _,
                              gkeService: GKEService[F],
                              kubernetesDnsCache: KubernetesDnsCache[F],
authProvider: LeoAuthProvider[F],
//dateAccessUpdaterQueue: InspectableQueue[IO, UpdateDateAccessMessage],
blocker: Blocker)
                            (implicit val system: ActorSystem,
                             F: Async[F],
                             executionContext: ExecutionContext,
                             timer: Timer[F],
                             cs: ContextShift[F],
                             dbRef: DbReference[F])
  extends LazyLogging {

  /*
  * Checks the user has the required notebook action, returning 401 or 404 depending on whether they can know the runtime exists
  */
  private[leonardo] def authCheck(
                                   userInfo: UserInfo,
                                   googleProject: GoogleProject,
                                   appName: _,
                                   action: _
                                 )(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    for {
      x <- F.pure()
//      samResource <- getCachedRuntimeSamResource(googleProject, runtimeName)
//      hasViewPermission <- authProvider
//        .hasRuntimePermission(samResource, userInfo, GetRuntimeStatus, googleProject)
//      //TODO: combine the sam calls into one
//      hasRequiredPermission <- authProvider
//        .hasRuntimePermission(samResource, userInfo, notebookAction, googleProject)
//      _ <- if (!hasViewPermission) {
//        IO.raiseError(RuntimeNotFoundException(googleProject, runtimeName, s"${notebookAction} permission is required"))
//      } else if (!hasRequiredPermission) {
//        IO.raiseError(AuthorizationError(Some(userInfo.userEmail)))
//      } else IO.unit
    } yield ()

  def proxyRequest(userInfo: UserInfo, googleProject: GoogleProject, appName: _, request: HttpRequest)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[HttpResponse] =
    for {
      _ <- authCheck(userInfo, googleProject, appName, ConnectToRuntime)
      now <- timer.clock.realTime(TimeUnit.MILLISECONDS)
      r <- proxyInternal(googleProject, appName, request, Instant.ofEpochMilli(now))
    } yield r

  def proxyInternal(googleProject: GoogleProject, appName: _, request: HttpRequest, now: Instant): F[HttpResponse] = {
    logger.debug(
      s"Received proxy request for ${googleProject}/${appName}: ${kubernetesDnsCache.stats} / ${kubernetesDnsCache.size}"
    )
    getTargetHost(googleProject, appName) flatMap {
      case HostReady(targetHost) =>
        // If this is a WebSocket request (e.g. wss://leo:8080/...) then akka-http injects a
        // virtual UpgradeToWebSocket header which contains facilities to handle the WebSocket data.
        // The presence of this header distinguishes WebSocket from http requests.
        val res = for {
//          _ <- dateAccessUpdaterQueue.enqueue1(UpdateDateAccessMessage(runtimeName, googleProject, now))
          response <- handleHttpRequest(targetHost, request)
          r <- if (response.status.isFailure())
            F.delay(logger.info(s"Error response for proxied request ${request.uri}: ${response.status}")).as(response)
          else F.pure(response)
        } yield r

        res.recoverWith {
          case e =>
            F.delay(logger.error("Error occurred in proxy", e)) >> F.raiseError[HttpResponse](
              KubernetesProxyException(googleProject, appName)
            )
        }
      case HostNotReady =>
        F.delay(logger.warn(s"proxy host not ready for ${googleProject}/${appName}")) >> F.raiseError(
          AppNotReadyException(googleProject, appName)
        )
      case HostPaused =>
        F.delay(logger.warn(s"proxy host paused for ${googleProject}/${appName}")) >> F.raiseError(
          AppPausedException(googleProject, appName)
        )
      case HostNotFound =>
        F.delay(logger.warn(s"proxy host not found for ${googleProject}/${appName}")) >> F.raiseError(
//          AppNotFoundException(googleProject, appName, "proxy host not found")
        )
    }
  }

  def getTargetHost(googleProject: GoogleProject, appName: _): F[HostStatus] = ???

  private def handleHttpRequest(targetHost: Host, request: HttpRequest): F[HttpResponse] = ???

  final case class KubernetesProxyException(googleProject: GoogleProject, appName: _)
    extends LeoException(s"Unable to proxy connection to app on ${googleProject.value}/${appName.value}",
      StatusCodes.InternalServerError)
}

final case class AppNotReadyException(googleProject: GoogleProject, appName: _)
  extends LeoException(
    s"App ${googleProject.value}/${appName.value} is not ready yet. It may be creating, try again later",
    StatusCodes.Locked
  )

final case class AppPausedException(googleProject: GoogleProject, appName: _)
  extends LeoException(
    s"Runtime ${googleProject.value}/${appName.value} is stopped. Start your app before proceeding.",
    StatusCodes.UnprocessableEntity
  )

final case class ProxyException(googleProject: GoogleProject, appName: _)
  extends LeoException(s"Unable to proxy connection to app ${googleProject.value}/${appName.value}",
    StatusCodes.InternalServerError)
