package org.broadinstitute.dsde.workbench.leonardo.dao

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache._
import org.broadinstitute.dsde.workbench.leonardo.dns.{ClusterDnsCache, DnsCacheKey}
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class HttpWelderDAO(val clusterDnsCache: ClusterDnsCache)(implicit system: ActorSystem, executionContext: ExecutionContext, materializer: ActorMaterializer) extends WelderDAO with LazyLogging {

  val http = Http(system)

  override def flushCache(googleProject: GoogleProject, clusterName: ClusterName): Future[Unit] = {
    getTargetHost(googleProject, clusterName) flatMap {
      case HostReady(targetHost) =>
        val statusUri = Uri(s"https://${targetHost.toString}/$googleProject/$clusterName/welder/cache/flush")
        http.singleRequest(HttpRequest(uri = statusUri)) map { response =>
          if(response.status.isSuccess)
        }
      case _ =>
        logger.error("fail to get target host name for welder")
        Future.unit
    }
  }

  protected def getTargetHost(googleProject: GoogleProject, clusterName: ClusterName): Future[HostStatus] = {
    implicit val timeout: Timeout = Timeout(5 seconds)
    clusterDnsCache.getHostStatus(DnsCacheKey(googleProject, clusterName)).mapTo[HostStatus]
  }
}
