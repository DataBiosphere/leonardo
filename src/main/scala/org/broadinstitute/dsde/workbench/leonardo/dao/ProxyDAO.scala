package org.broadinstitute.dsde.workbench.leonardo.dao

import akka.util.Timeout
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache.HostStatus
import org.broadinstitute.dsde.workbench.leonardo.dns.{ClusterDnsCache, DnsCacheKey}
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.Future
import scala.concurrent.duration._

//trait ProxyDAO {
//  implicit val executionContext: ExecutionContext
//
//  def http: HttpExt
//
//  def clusterDnsCache: ClusterDnsCache
//
//  def statusPath(host: Uri.Host, googleProject: GoogleProject, clusterName: ClusterName): Uri
//
//  def isProxyAvailable(googleProject: GoogleProject, clusterName: ClusterName): Future[Boolean] = {
//    getTargetHost(googleProject, clusterName) flatMap {
//      case HostReady(targetHost) =>
//        val statusUri = statusPath(targetHost, googleProject, clusterName)
//        http.singleRequest(HttpRequest(uri = statusUri)) map { response =>
//          response.status.isSuccess
//        }
//      case _ => Future.successful(false)
//    }
//  }
//
//  def getTargetHost(googleProject: GoogleProject, clusterName: ClusterName): Future[HostStatus] = {
//    implicit val timeout: Timeout = Timeout(5 seconds)
//    clusterDnsCache.getHostStatus(DnsCacheKey(googleProject, clusterName)).mapTo[HostStatus]
//  }
//}

object Proxy {
  def getTargetHost(clusterDnsCache: ClusterDnsCache, googleProject: GoogleProject, clusterName: ClusterName): Future[HostStatus] = {
    implicit val timeout: Timeout = Timeout(5 seconds)
    clusterDnsCache.getHostStatus(DnsCacheKey(googleProject, clusterName)).mapTo[HostStatus]
  }
}