package org.broadinstitute.dsde.workbench.leonardo.dao

import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache._
import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.leonardo.service.{ClusterNotFoundException, ClusterNotReadyException, ClusterPausedException, ProxyException}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class HttpJupyterProxyDAO(val baseJupyterProxyURL: String, val clusterDnsCache: ActorRef)(implicit system: ActorSystem, materializer: ActorMaterializer, executionContext: ExecutionContext) extends JupyterProxyDAO with LazyLogging {

  val http = Http(system)

  override def getStatus(googleProject: GoogleProject, clusterName: ClusterName): Future[Boolean] = {

    getTargetHost(googleProject, clusterName) flatMap {
      case ClusterReady(targetHost) =>
        executeJupyterProxyRequest(HttpRequest(uri = s"http://${targetHost.toString}/$googleProject/$clusterName/api/status"))
      case ClusterNotReady =>
        throw ClusterNotReadyException(googleProject, clusterName)
      case ClusterPaused =>
        throw ClusterPausedException(googleProject, clusterName)
      case ClusterNotFound =>
        throw ClusterNotFoundException(googleProject, clusterName)
      }
    }

  protected def getTargetHost(googleProject: GoogleProject, clusterName: ClusterName): Future[GetClusterResponse] = {
    implicit val timeout: Timeout = Timeout(5 seconds)
    (clusterDnsCache ? GetByProjectAndName(googleProject, clusterName)).mapTo[GetClusterResponse]
  }

  private def executeJupyterProxyRequest(httpRequest: HttpRequest): Future[Boolean] = {
    logger.info(httpRequest.toString)
    http.singleRequest(httpRequest) flatMap { response =>
      logger.info(response.toString)
      if (response.status.isSuccess)
        Future.successful(true)
       else
        Future.successful(false)
    }
  }
}