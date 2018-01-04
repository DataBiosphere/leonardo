package org.broadinstitute.dsde.workbench.api

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import akka.util.ByteString
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.broadinstitute.dsde.workbench.config.AuthToken

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

trait WorkbenchClient {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  implicit protected class JsonStringUtil(s: String) {
    def fromJsonMapAs[A](key: String): Option[A] = parseJsonAsMap.get(key)
    def parseJsonAsMap[A]: Map[String, A] = mapper.readValue(s, classOf[Map[String, A]])
  }

  private def makeAuthHeader(token: AuthToken): Authorization = {
    headers.Authorization(OAuth2BearerToken(token.value))
  }

  private def sendRequest(httpRequest: HttpRequest): HttpResponse = {
    Await.result(Http().singleRequest(httpRequest), 5.minutes)
  }

  private def handleResponse(response: HttpResponse): String = {
    response.status.isSuccess() match {
      case true =>
        val byteStringSink: Sink[ByteString, Future[ByteString]] = Sink.fold(ByteString("")) { (z, i) => z.concat(i) }
        val entityFuture = response.entity.dataBytes.runWith(byteStringSink)
        Await.result(entityFuture, 100.millis).decodeString("UTF-8")
      case _ =>
        val byteStringSink: Sink[ByteString, Future[ByteString]] = Sink.fold(ByteString("")) { (z, i) => z.concat(i) }
        val entityFuture = response.entity.dataBytes.runWith(byteStringSink)
        throw APIException(Await.result(entityFuture, 100.millis).decodeString("UTF-8"))
    }
  }

  def parseResponse(response: HttpResponse): String = {
    response.status.isSuccess() match {
      case true =>
        val byteStringSink: Sink[ByteString, Future[ByteString]] = Sink.fold(ByteString("")) { (z, i) => z.concat(i) }
        val entityFuture = response.entity.dataBytes.runWith(byteStringSink)
        Await.result(entityFuture, 100.millis).decodeString("UTF-8")
      case _ =>
        val byteStringSink: Sink[ByteString, Future[ByteString]] = Sink.fold(ByteString("")) { (z, i) => z.concat(i) }
        val entityFuture = response.entity.dataBytes.runWith(byteStringSink)
        throw APIException(Await.result(entityFuture, 100.millis).decodeString("UTF-8"))
    }
  }

  import scala.reflect.{ClassTag, classTag}
  def parseResponseAs[T: ClassTag](response: HttpResponse): T = {
    // https://stackoverflow.com/questions/6200253/scala-classof-for-type-parameter
    val classT: Class[T] = classTag[T].runtimeClass.asInstanceOf[Class[T]]
    mapper.readValue(parseResponse(response), classT)
  }

  // return Some(T) on success, None on failure
  def parseResponseOption[T: ClassTag](response: HttpResponse): Option[T] = {
    if (response.status.isSuccess())
      Option(parseResponseAs[T](response))
    else
      None
  }

  private def requestWithJsonContent(method: HttpMethod, uri: String, content: Any, httpHeaders: List[HttpHeader] = List())(implicit token: AuthToken): String = {
    val req = HttpRequest(method, uri, List(makeAuthHeader(token)) ++ httpHeaders, HttpEntity(ContentTypes.`application/json`, mapper.writeValueAsString(content)))
    handleResponse(sendRequest(req))
  }

  def postRequestWithMultipart(uri:String, name: String, content: String)(implicit token: AuthToken): String = {
    val part = Multipart.FormData.BodyPart(name, HttpEntity(ByteString(content)))
    val formData = Multipart.FormData(Source.single(part))
    val req = HttpRequest(POST, uri, List(makeAuthHeader(token)), formData.toEntity())
    handleResponse(sendRequest(req))
  }

  private def requestBasic(method: HttpMethod, uri: String, httpHeaders: List[HttpHeader] = List())(implicit token: AuthToken): HttpResponse = {
    val req = HttpRequest(method, uri, List(makeAuthHeader(token)) ++ httpHeaders)
    sendRequest(req)
  }

  def patchRequest(uri: String, content: Any, httpHeaders: List[HttpHeader] = List())(implicit token: AuthToken): String = {
    requestWithJsonContent(PATCH, uri, content, httpHeaders)
  }

  def postRequest(uri: String, content: Any = None, httpHeaders: List[HttpHeader] = List())(implicit token: AuthToken): String = {
    requestWithJsonContent(POST, uri, content, httpHeaders)
  }

  def putRequest(uri: String, content: Any = None, httpHeaders: List[HttpHeader] = List())(implicit token: AuthToken): String = {
    requestWithJsonContent(PUT, uri, content, httpHeaders)
  }

  def deleteRequest(uri: String, httpHeaders: List[HttpHeader] = List())(implicit token: AuthToken): String = {
    handleResponse(requestBasic(DELETE, uri, httpHeaders))
  }

  def getRequest(uri: String, httpHeaders: List[HttpHeader] = List())(implicit token: AuthToken): HttpResponse = {
    requestBasic(GET, uri, httpHeaders)
  }
}
