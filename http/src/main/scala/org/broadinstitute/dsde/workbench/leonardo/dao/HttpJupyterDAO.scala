package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.{Concurrent, ContextShift, Timer}
import cats.syntax.all._
import io.circe.Decoder
import org.broadinstitute.dsde.workbench.leonardo.RuntimeName
import org.broadinstitute.dsde.workbench.leonardo.dao.ExecutionState.{Idle, OtherState}
import org.broadinstitute.dsde.workbench.leonardo.dao.HostStatus.HostReady
import org.broadinstitute.dsde.workbench.leonardo.dao.HttpJupyterDAO._
import org.broadinstitute.dsde.workbench.leonardo.dns.RuntimeDnsCache
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.client.Client
import org.http4s.{Method, Request, Uri}
import org.typelevel.log4cats.Logger

//Jupyter server API doc https://github.com/jupyter/jupyter/wiki/Jupyter-Notebook-Server-API
class HttpJupyterDAO[F[_]: Timer: ContextShift](val runtimeDnsCache: RuntimeDnsCache[F], client: Client[F])(
  implicit F: Concurrent[F],
  logger: Logger[F]
) extends JupyterDAO[F] {
  def isProxyAvailable(googleProject: GoogleProject, runtimeName: RuntimeName): F[Boolean] =
    Proxy.getRuntimeTargetHost[F](runtimeDnsCache, googleProject, runtimeName) flatMap {
      case HostReady(targetHost) =>
        client
          .successful(
            Request[F](
              method = Method.GET,
              uri = Uri.unsafeFromString(
                s"https://${targetHost.address}/notebooks/${googleProject.value}/${runtimeName.asString}/api/status"
              )
            )
          )
          .handleError(_ => false)
      case _ => F.pure(false)
    }

  def isAllKernelsIdle(googleProject: GoogleProject, runtimeName: RuntimeName): F[Boolean] =
    for {
      hostStatus <- Proxy.getRuntimeTargetHost[F](runtimeDnsCache, googleProject, runtimeName)
      resp <- hostStatus match {
        case HostReady(targetHost) =>
          for {
            res <- client.expect[List[Session]](
              Request[F](
                method = Method.GET,
                uri = Uri.unsafeFromString(
                  s"https://${targetHost.address}/notebooks/${googleProject.value}/${runtimeName.asString}/api/sessions"
                )
              )
            )
          } yield res.forall(k => k.kernel.executionState == Idle)
        case _ => F.pure(true)
      }
    } yield resp

  override def createTerminal(googleProject: GoogleProject, runtimeName: RuntimeName): F[Unit] =
    Proxy.getRuntimeTargetHost[F](runtimeDnsCache, googleProject, runtimeName) flatMap {
      case HostReady(targetHost) =>
        client
          .successful(
            Request[F](
              method = Method.POST,
              uri = Uri.unsafeFromString(
                s"https://${targetHost.address}/notebooks/${googleProject.value}/${runtimeName.asString}/api/terminals"
              )
            )
          )
          .flatMap(res => if (res) F.unit else logger.error("Fail to create new terminal"))
      case _ => F.unit
    }

  override def terminalExists(googleProject: GoogleProject,
                              runtimeName: RuntimeName,
                              terminalName: TerminalName): F[Boolean] =
    Proxy.getRuntimeTargetHost[F](runtimeDnsCache, googleProject, runtimeName) flatMap {
      case HostReady(targetHost) =>
        client
          .successful(
            Request[F](
              method = Method.GET,
              uri = Uri.unsafeFromString(
                s"https://${targetHost.address}/notebooks/${googleProject.value}/${runtimeName.asString}/api/terminals/${terminalName.asString}" // this returns 404 if the terminal doesn't exist
              )
            )
          )
      case _ => F.pure(false)
    }

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
  def createTerminal(googleProject: GoogleProject, runtimeName: RuntimeName): F[Unit]
  def terminalExists(googleProject: GoogleProject, runtimeName: RuntimeName, terminalName: TerminalName): F[Boolean]
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

final case class TerminalName(asString: String) extends AnyVal
final case class Session(kernel: Kernel)
final case class Kernel(executionState: ExecutionState)
final case class AllSessionsResponse(kernel: List[Kernel])
