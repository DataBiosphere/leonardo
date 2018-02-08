package org.broadinstitute.dsde.workbench.leonardo.dao.google

import akka.http.scaladsl.model.StatusCodes
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, LeoException}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.{ExecutionContext, Future}

/**
  * Adds support for recovering from Google API errors and re-throwing LeoExceptions.
  */
object GoogleExceptionSupport extends LazyLogging {

  case class CallToGoogleApiFailedException(googleProject: GoogleProject, context: Option[String], exceptionStatusCode: Int, errorMessage: String)
    extends LeoException(s"Call to Google API failed for ${googleProject.value} ${context.map(c => s"/ $c").getOrElse("")}. Message: $errorMessage", exceptionStatusCode)

  implicit class GoogleFuture[A](future: Future[A]) {

    def handleGoogleException(cluster: Cluster)(implicit executionContext: ExecutionContext): Future[A] = {
      handleGoogleException(cluster.googleProject, Some(cluster.clusterName.value))
    }

    def handleGoogleException(googleProject: GoogleProject, context: Option[String] = None)(implicit executionContext: ExecutionContext): Future[A] = {
      future.recover {
        case e: GoogleJsonResponseException =>
          logger.error(s"Error occurred executing Google request for ${googleProject.value} / $context", e)
          throw CallToGoogleApiFailedException(googleProject, context, e.getStatusCode, e.getDetails.getMessage)
        case illegalArgumentException: IllegalArgumentException =>
          logger.error(s"Illegal argument passed to Google request for ${googleProject.value} / $context", illegalArgumentException)
          throw CallToGoogleApiFailedException(googleProject, context, StatusCodes.BadRequest.intValue, illegalArgumentException.getMessage)
      }
    }
  }

}
