package org.broadinstitute.dede.workbench.leonardo.consumer

import au.com.dius.pact.consumer.dsl.{DslPart, PactDslResponse, PactDslWithProvider}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.http4s.{AuthScheme, Credentials}
import org.http4s.Credentials.Token

case object InvalidCredentials extends Exception

case object UserAlreadyExists extends Exception

case object UnknownError extends Exception

object AuthHelper {
  def mockBearerHeader(workbenchEmail: WorkbenchEmail) = s"Bearer TokenFor$workbenchEmail"
  def mockAuthToken(workbenchEmail: WorkbenchEmail): Token =
    Credentials.Token(AuthScheme.Bearer, s"TokenFor$workbenchEmail")
}

object PactHelper {
  def buildInteraction(builder: PactDslResponse,
                       state: String,
                       uponReceiving: String,
                       method: String,
                       path: String,
                       requestHeaders: Seq[(String, String)],
                       status: Int,
                       responseHeaders: Seq[(String, String)],
                       body: DslPart
  ): PactDslResponse =
    builder
      .`given`(state)
      .uponReceiving(uponReceiving)
      .method(method)
      .path(path)
      .headers(scala.jdk.CollectionConverters.MapHasAsJava(requestHeaders.toMap).asJava)
      .willRespondWith()
      .status(status)
      .headers(scala.jdk.CollectionConverters.MapHasAsJava(responseHeaders.toMap).asJava)
      .body(body)

  def buildInteraction(builder: PactDslWithProvider,
                       state: String,
                       uponReceiving: String,
                       method: String,
                       path: String,
                       requestHeaders: Seq[(String, String)],
                       status: Int,
                       responseHeaders: Seq[(String, String)],
                       body: DslPart
  ): PactDslResponse =
    builder
      .`given`(state)
      .uponReceiving(uponReceiving)
      .method(method)
      .path(path)
      .headers(scala.jdk.CollectionConverters.MapHasAsJava(requestHeaders.toMap).asJava)
      .willRespondWith()
      .status(status)
      .headers(scala.jdk.CollectionConverters.MapHasAsJava(responseHeaders.toMap).asJava)
      .body(body)
}
