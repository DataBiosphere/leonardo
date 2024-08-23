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
    // TODO: I tried to use TCL SamExceptionFactory to process the ApiException.
    //  However it's not usable from Leo becauseof missing Spring dependencies.
    //  Investigate whether the logic in SamExceptionFactory should be ported to Leo.
    new SamException(
      s"$messagePrefix: ${apiException.getMessage}",
      apiException.getCode,
      apiException.getCause,
      traceId
    )
}
