package org.broadinstitute.dsde.workbench.leonardo.dao.sam

import akka.http.scaladsl.model.StatusCodes
import com.fasterxml.jackson.databind.ObjectMapper
import org.broadinstitute.dsde.workbench.client.sam.ApiException
import org.broadinstitute.dsde.workbench.client.sam.model.ErrorReport
import org.broadinstitute.dsde.workbench.leonardo.LeonardoTestSuite
import org.broadinstitute.dsde.workbench.model.TraceId
import org.scalatest.flatspec.AnyFlatSpec

import scala.jdk.CollectionConverters._

class SamExceptionSpec extends AnyFlatSpec with LeonardoTestSuite {
  val objectMapper = new ObjectMapper

  "SamException" should "create from an ApiException" in {
    val cause = new RuntimeException("cause")
    val errorReport = new ErrorReport().statusCode(400).message("error report message")
    val apiException = new ApiException(
      "test message",
      cause,
      400,
      Map("responseHeader1" -> List("responseHeader1Value").asJava).asJava,
      objectMapper.writeValueAsString(errorReport)
    )
    val samException = SamException.create("messagePrefix", apiException, TraceId("traceId"))

    samException.message shouldBe "messagePrefix: test message"
    samException.statusCode shouldBe StatusCodes.BadRequest
    samException.cause shouldBe cause
  }

  it should "extract the message from the ErrorReport if the top-level message is blank" in {
    val cause = new RuntimeException("cause")
    val errorReport = new ErrorReport().statusCode(400).message("error report message")
    val apiException = new ApiException(
      "",
      cause,
      400,
      Map("responseHeader1" -> List("responseHeader1Value").asJava).asJava,
      objectMapper.writeValueAsString(errorReport)
    )
    val samException = SamException.create("messagePrefix", apiException, TraceId("traceId"))

    samException.message shouldBe "messagePrefix: error report message"
    samException.statusCode shouldBe StatusCodes.BadRequest
    samException.cause shouldBe cause
  }
}
