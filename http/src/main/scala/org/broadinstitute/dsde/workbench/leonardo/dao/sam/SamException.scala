package org.broadinstitute.dsde.workbench.leonardo.dao.sam

import org.broadinstitute.dsde.workbench.client.sam.ApiException
import org.broadinstitute.dsde.workbench.leonardo.model.LeoException
import org.broadinstitute.dsde.workbench.model.TraceId

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
  def create(messagePrefix: String, apiException: ApiException, traceId: TraceId): SamException =
    new SamException(
      // TODO: investigate whether we need to extract the ApiException message like in
      //  TCL SamExceptionFactory
      s"$messagePrefix: ${apiException}",
      apiException.getCode,
      apiException.getCause,
      traceId
    )
}
