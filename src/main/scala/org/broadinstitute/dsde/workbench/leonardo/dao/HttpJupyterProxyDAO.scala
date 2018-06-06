package org.broadinstitute.dsde.workbench.leonardo.dao

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods.GET
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.model.google.GoogleProject


import scala.concurrent.{ExecutionContext, Future}

class HttpJupyterProxyDAO(val baseJupyterProxyURL: String)(implicit system: ActorSystem, materializer: ActorMaterializer, executionContext: ExecutionContext) extends JupyterProxyDAO with LazyLogging {
  private val jupyterProxyURL = baseJupyterProxyURL

  val http = Http(system)

  override def getStatus(googleProject: GoogleProject, clusterName: ClusterName): Future[Boolean] = {
    val uri = Uri(jupyterProxyURL + s"${googleProject.value}/${clusterName.value}/api/status")
    executeJupyterProxyRequest(HttpRequest(GET, uri))
  }

  private def executeJupyterProxyRequest(httpRequest: HttpRequest): Future[Boolean] = {
    logger.info(httpRequest.toString)
    http.singleRequest(httpRequest) recover { case t: Throwable =>
      throw CallToSamFailedException(httpRequest.uri, StatusCodes.InternalServerError, Some(t.getMessage))
    } flatMap { response =>
      logger.info(response.toString)
      if (response.status.intValue == 200)
        Future.successful(true)
       else
        Future.successful(false)
    }
  }
}