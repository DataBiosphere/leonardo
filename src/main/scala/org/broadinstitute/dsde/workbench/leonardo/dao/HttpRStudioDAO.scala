package org.broadinstitute.dsde.workbench.leonardo.dao

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache.HostReady
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.{ExecutionContext, Future}

class HttpRStudioDAO(val clusterDnsCache: ClusterDnsCache)(implicit system: ActorSystem, executionContext: ExecutionContext) extends RStudioDAO with LazyLogging {

  val http = Http(system)

  def isProxyAvailable(googleProject: GoogleProject, clusterName: ClusterName): Future[Boolean] = {
    Proxy.getTargetHost(clusterDnsCache, googleProject, clusterName) flatMap {
      case HostReady(targetHost) =>
        val statusUri = Uri(s"https://${targetHost.toString}/proxy/$googleProject/$clusterName/rstudio")
        http.singleRequest(HttpRequest(uri = statusUri)) map { response =>
          response.status.isSuccess
        }
      case _ => Future.successful(false)
    }
  }
}

trait RStudioDAO {
  def isProxyAvailable(googleProject: GoogleProject, clusterName: ClusterName): Future[Boolean]
}