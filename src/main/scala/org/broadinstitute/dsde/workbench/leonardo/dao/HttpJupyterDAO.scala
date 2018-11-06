package org.broadinstitute.dsde.workbench.leonardo.dao

import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache._
import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.dns.{ClusterDnsCache, DnsCacheKey}
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class HttpJupyterDAO(val clusterDnsCache: ClusterDnsCache)(implicit system: ActorSystem, materializer: ActorMaterializer, executionContext: ExecutionContext) extends JupyterDAO with LazyLogging {

  val http = Http(system)

  override def getStatus(googleProject: GoogleProject, clusterName: ClusterName): Future[Boolean] = {
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
    clusterDnsCache.projectClusterToHostStatusCache.get(DnsCacheKey(googleProject, clusterName)).mapTo[HostStatus]
  }
}
