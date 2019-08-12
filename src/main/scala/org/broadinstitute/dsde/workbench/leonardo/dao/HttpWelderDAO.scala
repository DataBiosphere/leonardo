package org.broadinstitute.dsde.workbench.leonardo
package dao

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache._
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.{ExecutionContext, Future}

class HttpWelderDAO(val clusterDnsCache: ClusterDnsCache)
                   (implicit system: ActorSystem, executionContext: ExecutionContext) extends WelderDAO with LazyLogging {

  val http = Http(system)

  def flushCache(googleProject: GoogleProject, clusterName: ClusterName): Future[Unit] = {
    Proxy.getTargetHost(clusterDnsCache, googleProject, clusterName) flatMap {
      case HostReady(targetHost) =>
        val statusUri = Uri(s"https://${targetHost.toString}/proxy/$googleProject/$clusterName/welder/cache/flush")
        http.singleRequest(HttpRequest(uri = statusUri, method = HttpMethods.POST)) flatMap { response =>
          if(response.status.isSuccess)
            Metrics.newRelic.incrementCounterFuture("flushWelderCacheSuccess")
          else
            Metrics.newRelic.incrementCounterFuture("flushWelderCacheFailure")
        }
      case _ =>
        logger.error(s"fail to get target host name for welder for ${googleProject}/${clusterName}")
        Future.unit
    }
  }

  def isProxyAvailable(googleProject: GoogleProject, clusterName: ClusterName): Future[Boolean] = {
    Proxy.getTargetHost(clusterDnsCache, googleProject, clusterName) flatMap {
      case HostReady(targetHost) =>
        val statusUri = Uri(s"https://${targetHost.toString}/proxy/$googleProject/$clusterName/welder/status")
        http.singleRequest(HttpRequest(uri = statusUri)) map { response =>
          response.status.isSuccess
        }
      case _ => Future.successful(false)
    }
  }
}

trait WelderDAO extends ToolDAO {
  def flushCache(googleProject: GoogleProject, clusterName: ClusterName): Future[Unit]
  def isProxyAvailable(googleProject: GoogleProject, clusterName: ClusterName): Future[Boolean]
}
