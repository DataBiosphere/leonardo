package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.{Concurrent, ContextShift, Timer}
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.circe.Decoder
import org.broadinstitute.dsde.workbench.leonardo.RuntimeName
import org.broadinstitute.dsde.workbench.leonardo.dao.ExecutionState.{Idle, OtherState}
import org.broadinstitute.dsde.workbench.leonardo.dao.HttpJupyterDAO._
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.client.Client
import org.http4s.{Method, Request, Uri}

class HttpJupyterDAO[F[_]: Timer: ContextShift: Concurrent](val clusterDnsCache: ClusterDnsCache[F], client: Client[F])
    extends JupyterDAO[F]
    with LazyLogging {
  def isProxyAvailable(googleProject: GoogleProject, runtimeName: RuntimeName): F[Boolean] =
    Proxy.getTargetHost[F](clusterDnsCache, googleProject, runtimeName) flatMap {
      case HostReady(targetHost) =>
        client.successful(
          Request[F](
            method = Method.GET,
            uri = Uri.unsafeFromString(
              s"https://${targetHost.toString}/notebooks/${googleProject.value}/${runtimeName.asString}/api/status"
            )
          )
        )
      case _ => Concurrent[F].pure(false)
    }

  def isAllKernelsIdle(googleProject: GoogleProject, runtimeName: RuntimeName): F[Boolean] =
    for {
      hostStatus <- Proxy.getTargetHost[F](clusterDnsCache, googleProject, runtimeName)
      resp <- hostStatus match {
        case HostReady(host) =>
          for {
            res <- client.expect[List[Session]](
              Request[F](
                method = Method.GET,
                uri = Uri.unsafeFromString(
                  s"https://${host.toString}/notebooks/${googleProject.value}/${runtimeName.asString}/api/sessions"
                )
              )
            )
          } yield res.forall(k => k.kernel.executionState == Idle)
        case _ => Concurrent[F].pure(false)
      }
    } yield resp
}

object HttpJupyterDAO {
  implicit val executionStateDecoder: Decoder[ExecutionState] =
    Decoder.decodeString.map(s => if (s == Idle.toString) Idle else OtherState(s))
  implicit val kernalDecoder: Decoder[Kernel] = Decoder.forProduct1("execution_state")(Kernel)
  implicit val sessionDecoder: Decoder[Session] = Decoder.forProduct1("kernel")(Session)
}

trait JupyterDAO[F[_]] {
  def isAllKernelsIdle(googleProject: GoogleProject, runtimeName: RuntimeName): F[Boolean]
  def isProxyAvailable(googleProject: GoogleProject, runtimeName: RuntimeName): F[Boolean]
}

sealed abstract class ExecutionState
object ExecutionState {
  case object Idle extends ExecutionState {
    override def toString: String = "idle"
  }
  final case class OtherState(msg: String) extends ExecutionState {
    override def toString: String = msg
  }
}

final case class Session(kernel: Kernel)
final case class Kernel(executionState: ExecutionState)
final case class AllSessionsResponse(kernel: List[Kernel])
