package org.broadinstitute.dsde.workbench.leonardo.dao

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache._
import org.broadinstitute.dsde.workbench.leonardo.dns.{ClusterDnsCache, DnsCacheKey}
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class HttpRStudioDAO(val clusterDnsCache: ClusterDnsCache)(implicit system: ActorSystem, executionContext: ExecutionContext) extends ToolDAO with LazyLogging {

  val http = Http(system)

  override def isProxyAvailable(googleProject: GoogleProject, clusterName: ClusterName): Future[Boolean] = {
    getTargetHost(googleProject, clusterName) flatMap {
      case HostReady(targetHost) =>
        val statusUri = Uri(s"https://${targetHost.toString}/proxy/$googleProject/$clusterName/rstudio")
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
}
