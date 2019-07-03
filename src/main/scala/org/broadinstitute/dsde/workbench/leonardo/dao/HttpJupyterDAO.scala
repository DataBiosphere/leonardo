package org.broadinstitute.dsde.workbench.leonardo.dao

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.LazyLogging
import io.circe.Decoder
import io.circe.parser.parse
import org.broadinstitute.dsde.workbench.leonardo.dao.ExecutionState.{Idle, OtherState}
import org.broadinstitute.dsde.workbench.leonardo.dao.HttpJupyterDAO._
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache._
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.{ExecutionContext, Future}

class HttpJupyterDAO(val clusterDnsCache: ClusterDnsCache)(implicit system: ActorSystem, executionContext: ExecutionContext, materializer: ActorMaterializer) extends ToolDAO with LazyLogging {

  val http = Http(system)

  override def isProxyAvailable(googleProject: GoogleProject, clusterName: ClusterName): Future[Boolean] = {
    Proxy.getTargetHost(clusterDnsCache, googleProject, clusterName) flatMap {
      case HostReady(targetHost) =>
        val statusUri = Uri(s"https://${targetHost.toString}/notebooks/$googleProject/$clusterName/api/status")
        http.singleRequest(HttpRequest(uri = statusUri)) map { response =>
          response.status.isSuccess
        }
      case _ => Future.successful(false)
    }
  }

  override def isAllKernalsIdle(googleProject: GoogleProject, clusterName: ClusterName): Future[Boolean] = {
    for {
      hostStatus <- Proxy.getTargetHost(clusterDnsCache, googleProject, clusterName)
      resp <- hostStatus match {
        case HostReady(host) =>
          val sessionUri = Uri(s"https://${host.toString}/notebooks/$googleProject/$clusterName/api/sessions")

          for {
            resp <- http.singleRequest(HttpRequest(uri = sessionUri))
            respString <- Unmarshal(resp.entity).to[String]
            parsedResp = parse(respString).flatMap(json => json.as[List[Session]])
            res <- Future.fromTry(parsedResp.toTry)
          } yield res.forall(k => k.kernel.executionState == Idle)
        case _ => Future.successful(false)
      }
    } yield resp
  }
}

object HttpJupyterDAO {
  implicit val executionStateDecoder: Decoder[ExecutionState] = Decoder.decodeString.map(s => if(s == Idle.toString) Idle else OtherState(s))
  implicit val kernalDecoder: Decoder[Kernel] = Decoder.forProduct1("execution_state")(Kernel)
  implicit val sessionDecoder: Decoder[Session] = Decoder.forProduct1("kernel")(Session)
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
