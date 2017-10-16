package org.broadinstitute.dsde.workbench.leonardo.dao

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoException, UserInfo}
import org.broadinstitute.dsde.workbench.model.WorkbenchIdentityJsonSupport._
import org.broadinstitute.dsde.workbench.model.WorkbenchUserServiceAccountEmail
import org.broadinstitute.dsde.workbench.util.health.StatusJsonSupport._
import org.broadinstitute.dsde.workbench.util.health.SubsystemStatus
import scala.concurrent.{ExecutionContext, Future}

case class CallToSamFailedException(uri: Uri, status: StatusCode, msg: String)
  extends LeoException(s"Call to Sam endpoint [${uri.path}] failed with status $status. Message: $msg", status)

/**
  * Created by rtitle on 10/16/17.
  */
class HttpSamDAO(val baseSamServiceURL: String)(implicit system: ActorSystem, materializer: ActorMaterializer, executionContext: ExecutionContext) extends SamDAO {
  private val samServiceURL = baseSamServiceURL

  val http = Http(system)

  override def getStatus(): Future[SubsystemStatus] = {
    val uri = Uri(samServiceURL + "/status")
    executeSamRequest(HttpRequest(GET, uri)).flatMap {
      case HttpResponse(OK, _, entity, _) =>
        Unmarshal(entity).to[SubsystemStatus]
      case HttpResponse(status, _, entity, _) =>
        val entityAsString = Unmarshal(entity).to[String]
        Future.successful(SubsystemStatus(false, Some(List(s"Sam status check failed with code $status: $entityAsString"))))
    }
  }

  override def getPetServiceAccount(userInfo: UserInfo): Future[WorkbenchUserServiceAccountEmail] = {
    val uri = Uri(samServiceURL + "/api/user/petServiceAccount")
    executeSamRequestAsUser(HttpRequest(GET, uri), userInfo).flatMap {
      case HttpResponse(OK, _, entity, _) =>
        Unmarshal(entity).to[WorkbenchUserServiceAccountEmail]
      case HttpResponse(status, _, entity, _) =>
        Unmarshal(entity).to[String].flatMap { entityAsString =>
          Future.failed(CallToSamFailedException(uri, status, entityAsString))
        }
    }
  }

  private def authHeader(userInfo: UserInfo): HttpHeader = Authorization(userInfo.accessToken)

  private def executeSamRequest(httpRequest: HttpRequest): Future[HttpResponse] = {
    http.singleRequest(httpRequest) recover { case t: Throwable =>
      throw CallToSamFailedException(httpRequest.uri, StatusCodes.InternalServerError, t.getMessage)
    }
  }

  private def executeSamRequestAsUser(httpRequest: HttpRequest, userInfo: UserInfo): Future[HttpResponse] = {
    executeSamRequest(httpRequest.copy(headers = httpRequest.headers :+ authHeader(userInfo)))
  }
}
