package org.broadinstitute.dsde.workbench.leonardo.dao

import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache._
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.dns.{ClusterDnsCache, DnsCacheKey}
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import akka.http.scaladsl.unmarshalling.Unmarshal
import io.circe.Decoder
import io.circe.parser.parse
import org.broadinstitute.dsde.workbench.leonardo.dao.ExecutionState.{Idle, OtherState}
import HttpJupyterDAO._
import akka.stream.ActorMaterializer

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class HttpJupyterDAO(val clusterDnsCache: ClusterDnsCache)(implicit system: ActorSystem, executionContext: ExecutionContext, materializer: ActorMaterializer) extends ToolDAO with LazyLogging {

  val http = Http(system)

  override def isProxyAvailable(googleProject: GoogleProject, clusterName: ClusterName): Future[Boolean] = {
    getTargetHost(googleProject, clusterName) flatMap {
      case HostReady(targetHost) =>
        val statusUri = Uri(s"https://${targetHost.toString}/notebooks/$googleProject/$clusterName/api/status")
        http.singleRequest(HttpRequest(uri = statusUri)) map { response =>
          response.status.isSuccess
        }
      case _ => Future.successful(false)
    }
  }

  protected def getTargetHost(googleProject: GoogleProject, clusterName: ClusterName): Future[HostStatus] = {
    implicit val timeout: Timeout = Timeout(5 seconds)
    clusterDnsCache.getHostStatus(DnsCacheKey(googleProject, clusterName)).mapTo[HostStatus]
  }

  override def isAllKernalsIdle(googleProject: GoogleProject, clusterName: ClusterName): Future[Boolean] = {
    for {
      hostStatus <- getTargetHost(googleProject, clusterName)
      resp <- hostStatus match {
        case HostReady(host) =>
          val sessionUri = Uri(s"https://${host.toString}/notebooks/$googleProject/$clusterName/api/sessions")

          for {
            resp <- http.singleRequest(HttpRequest(uri = sessionUri))
            respString <- Unmarshal(resp.entity).to[String]
            parsedResp = parse(respString).flatMap(json => json.as[AllSessionsResponse])
            res <- Future.fromTry(parsedResp.toTry)
          } yield res.kernel.forall(k => k.executionState == Idle)
        case _ => Future.successful(false)
      }
    } yield resp
  }
}

object HttpJupyterDAO {
  implicit val executionStateDecoder: Decoder[ExecutionState] = Decoder.decodeString.map(s => if(s == Idle.toString) Idle else OtherState(s))
  implicit val kernalDecoder: Decoder[Kernel] = Decoder.forProduct1("execution_state")(Kernel)
  implicit val allSessionsResponseDecoder: Decoder[AllSessionsResponse] = Decoder.forProduct1("kernel")(AllSessionsResponse)
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

final case class Kernel(executionState: ExecutionState)
final case class AllSessionsResponse(kernel: List[Kernel])
