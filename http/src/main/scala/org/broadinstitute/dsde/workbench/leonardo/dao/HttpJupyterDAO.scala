package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.Async
import cats.syntax.all._
import io.circe.Decoder
import org.broadinstitute.dsde.workbench.leonardo.dao.ExecutionState.{Idle, OtherState}
import org.broadinstitute.dsde.workbench.leonardo.dao.HostStatus.HostReady
import org.broadinstitute.dsde.workbench.leonardo.dao.HttpJupyterDAO._
import org.broadinstitute.dsde.workbench.leonardo.dns.RuntimeDnsCache
import org.broadinstitute.dsde.workbench.leonardo.{CloudContext, RuntimeName}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.client.Client
import org.http4s.{Header, Headers, Method, Request}
import org.typelevel.ci.CIString
import org.typelevel.log4cats.Logger

//Jupyter server API doc https://github.com/jupyter/jupyter/wiki/Jupyter-Notebook-Server-API
class HttpJupyterDAO[F[_]](val runtimeDnsCache: RuntimeDnsCache[F], client: Client[F], samDAO: SamDAO[F])(implicit
  F: Async[F],
  logger: Logger[F]
) extends JupyterDAO[F] {
  private val SETDATEACCESSEDINSPECTOR_HEADER_IGNORE: Header.Raw =
    Header.Raw(CIString("X-SetDateAccessedInspector-Action"), "ignore")

  def isProxyAvailable(cloudContext: CloudContext, runtimeName: RuntimeName, tokenOpt: Option[String]): F[Boolean] =
    for {
      hostStatus <- Proxy.getRuntimeTargetHost[F](runtimeDnsCache, cloudContext, runtimeName)
      headers <- cloudContext match {
        case _: CloudContext.Azure =>
          F.pure(Headers(Header("Authorization", s"Bearer ${tokenOpt.get}")) ++ Headers(SETDATEACCESSEDINSPECTOR_HEADER_IGNORE))
          //samDAO.getLeoAuthToken.map(x => Headers(x) ++ Headers(SETDATEACCESSEDINSPECTOR_HEADER_IGNORE))
        case _: CloudContext.Gcp =>
          F.pure(Headers.empty)
      }
      res <- hostStatus match {
        case x: HostReady =>
          client
            .successful(
              Request[F](
                method = Method.GET,
                uri = x.toNotebooksUri / "api" / "status",
                headers = headers
              )
            )
            .handleError(_ => false)
        case _ => F.pure(false)
      }
    } yield res

  def isAllKernelsIdle(cloudContext: CloudContext, runtimeName: RuntimeName): F[Boolean] =
    for {
      hostStatus <- Proxy.getRuntimeTargetHost[F](runtimeDnsCache, cloudContext, runtimeName)
      headers <- cloudContext match {
        case _: CloudContext.Azure =>
          samDAO.getLeoAuthToken.map(x => Headers(x) ++ Headers(SETDATEACCESSEDINSPECTOR_HEADER_IGNORE))
        case _: CloudContext.Gcp =>
          F.pure(Headers.empty)
      }
      resp <- hostStatus match {
        case x: HostReady =>
          for {
            res <- client.expect[List[Session]](
              Request[F](
                method = Method.GET,
                uri = x.toNotebooksUri / "api" / "sessions",
                headers = headers
              )
            )
          } yield res.forall(k => k.kernel.executionState == Idle)
        case _ => F.pure(true)
      }
    } yield resp

  override def createTerminal(googleProject: GoogleProject, runtimeName: RuntimeName): F[Unit] =
    Proxy.getRuntimeTargetHost[F](runtimeDnsCache, CloudContext.Gcp(googleProject), runtimeName) flatMap {
      case x: HostReady =>
        client
          .successful(
            Request[F](
              method = Method.POST,
              uri = x.toNotebooksUri / "api" / "terminals"
            )
          )
          .flatMap(res => if (res) F.unit else logger.error("Fail to create new terminal"))
      case _ => F.unit
    }

  override def terminalExists(googleProject: GoogleProject,
                              runtimeName: RuntimeName,
                              terminalName: TerminalName
  ): F[Boolean] =
    Proxy.getRuntimeTargetHost[F](runtimeDnsCache, CloudContext.Gcp(googleProject), runtimeName) flatMap {
      case x: HostReady =>
        client
          .successful(
            Request[F](
              method = Method.GET,
              uri = x.toNotebooksUri / "api" / "terminals" / terminalName.asString
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
  def isAllKernelsIdle(cloudContext: CloudContext, runtimeName: RuntimeName): F[Boolean]
  def isProxyAvailable(cloudContext: CloudContext, runtimeName: RuntimeName, tokenOpt: Option[String]): F[Boolean]
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
