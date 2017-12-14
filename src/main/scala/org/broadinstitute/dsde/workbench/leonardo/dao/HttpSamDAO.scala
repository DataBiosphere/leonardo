package org.broadinstitute.dsde.workbench.leonardo.dao

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.{Unmarshal, Unmarshaller}
import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.model.LeoException
import org.broadinstitute.dsde.workbench.model.ErrorReportJsonSupport._
import org.broadinstitute.dsde.workbench.model.WorkbenchIdentityJsonSupport._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{ErrorReport, UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.util.health.StatusJsonSupport._
import org.broadinstitute.dsde.workbench.util.health.StatusCheckResponse

import scala.concurrent.{ExecutionContext, Future}

case class CallToSamFailedException(uri: Uri, status: StatusCode, msg: Option[String])
  extends LeoException(s"Call to Sam endpoint [${uri.path}] failed with status $status.${msg.map(m => s" Message: $m").getOrElse("")}", status)

/**
  * Created by rtitle on 10/16/17.
  */
class HttpSamDAO(val baseSamServiceURL: String)(implicit system: ActorSystem, materializer: ActorMaterializer, executionContext: ExecutionContext) extends SamDAO with LazyLogging {
  private val samServiceURL = baseSamServiceURL

  val http = Http(system)

  override def getStatus(): Future[StatusCheckResponse] = {
    val uri = Uri(samServiceURL + "/status")
    executeSamRequest[StatusCheckResponse](HttpRequest(GET, uri), ignoreError = true) map { statusCheckResponse =>
      // If it's an OK status, strip out any subsystem information from the Sam response.
      // Otherwise, return the StatusCheckResponse as-is, with error information.
      if (statusCheckResponse.ok) statusCheckResponse.copy(systems = Map.empty)
      else statusCheckResponse
    }
  }

  override def getPetServiceAccountForProject(userInfo: UserInfo, googleProject: GoogleProject): Future[WorkbenchEmail] = {
    val uri = Uri(samServiceURL + s"/api/google/user/petServiceAccount/${googleProject.value}")
    executeSamRequestAsUser[WorkbenchEmail](HttpRequest(GET, uri), userInfo)
  }

  private def authHeader(userInfo: UserInfo): HttpHeader = Authorization(userInfo.accessToken)

  // If ignoreError is true, this method will try to unmarshal the response entity to a T, regardless of status code.
  // If ignoreError is false (the default), it will try to unmarshal an ErrorReport for a non-successful status code.
  private def executeSamRequest[T](httpRequest: HttpRequest, ignoreError: Boolean = false)(implicit um: Unmarshaller[ResponseEntity, T]): Future[T] = {
    http.singleRequest(httpRequest) recover { case t: Throwable =>
      throw CallToSamFailedException(httpRequest.uri, StatusCodes.InternalServerError, Some(t.getMessage))
    } flatMap { response =>
      if (ignoreError || response.status.isSuccess) {
        Unmarshal(response.entity).to[T]
      } else {
        // Try to unmarshal the response entity as an ErrorReport
        Unmarshal(response.entity).to[ErrorReport] map { report =>
          // Log the full error report, but only return the message to the user
          logger.error(s"Sam call to ${httpRequest.uri} failed with error report: ${ErrorReport.loggableString(report)}")
          Some(report.message)
        } recoverWith { case _: Throwable =>
          // Couldn't unmarshal as an ErrorReport, unmarshal as a String instead
          Unmarshal(response.entity).to[String] map { entityAsString =>
            // Log the Sam entity response, but return a generic message to the user
            logger.error(s"Sam call to ${httpRequest.uri} failed with entity: $entityAsString")
            None
          }
        } flatMap { messageOpt =>
          Future.failed(CallToSamFailedException(httpRequest.uri, response.status, messageOpt))
        }
      }
    }
  }

  private def executeSamRequestAsUser[T](httpRequest: HttpRequest, userInfo: UserInfo, ignoreError: Boolean = false)(implicit um: Unmarshaller[ResponseEntity, T]): Future[T] = {
    executeSamRequest[T](httpRequest.copy(headers = httpRequest.headers :+ authHeader(userInfo)), ignoreError)
  }
}