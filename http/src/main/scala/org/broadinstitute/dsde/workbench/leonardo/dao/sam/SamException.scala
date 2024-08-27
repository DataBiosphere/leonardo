package org.broadinstitute.dsde.workbench.leonardo.dao.sam

import com.fasterxml.jackson.databind.ObjectMapper
import org.broadinstitute.dsde.workbench.client.sam.ApiException
import org.broadinstitute.dsde.workbench.client.sam.model.ErrorReport
import org.broadinstitute.dsde.workbench.leonardo.model.LeoException
import org.broadinstitute.dsde.workbench.model.TraceId

import scala.util.Try

/**
 * Represents an exception interacting with Sam.
 */
class SamException private (message: String, code: Int, cause: Throwable, traceId: TraceId)
    extends LeoException(
      message,
      code,
      cause,
      traceId = Some(traceId)
    )

object SamException {
  val objectMapper = new ObjectMapper

  def create(messagePrefix: String, apiException: ApiException, traceId: TraceId): SamException =
    new SamException(
      // TODO: investigate whether we need to extract the ApiException message like in
      //  TCL SamExceptionFactory
      extractMessage(messagePrefix, apiException),
      apiException.getCode,
      apiException.getCause,
      traceId
    )

  /**
   * Extracts a useful message from a Sam client ApiException.
   *
   * Logic borrowed from terra-common-lib SamExceptionFactory class. We can't use the class directly
   * because it has Spring dependencies, which Leo excludes.
   */
  private def extractMessage(messagePrefix: String, apiException: ApiException): String = {
    // The generated ApiException class unfortunately formats getMessage(), and includes
    // the entire response body. We want to extract the actual message from that.
    val messagePattern = "^Message: ([\\S\\s]*)\\nHTTP response code:".r
    val extractedMessage = messagePattern.findFirstMatchIn(apiException.getMessage).map(_.group(1)).filterNot(_.isBlank)

    // If we could not extract the top-level message, try to deserialize the error report
    // buried one level down and extract the message from that.
    val finalMessage = extractedMessage.orElse {
      val errorReport = Try(objectMapper.readValue(apiException.getResponseBody, classOf[ErrorReport]))
      errorReport.map(_.getMessage).toOption
    }

    // Prepend the message prefix to the extracted message, if we have one.
    s"$messagePrefix: ${finalMessage.getOrElse("")}"
  }
}
