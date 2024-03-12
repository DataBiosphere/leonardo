package org.broadinstitute.dsde.workbench.leonardo.util

import io.opencensus.common.Scope
import io.opencensus.trace.{Span, Tracing}
import jakarta.ws.rs.client.{ClientRequestContext, ClientRequestFilter, ClientResponseContext, ClientResponseFilter}
import jakarta.ws.rs.ext.Provider

/**
 * Deferred execution in Scala (Futures, IOs, DBIOs, etc) result in a thread switchery. OpenCensus tracing
 * often uses thread local variables to maintain trace information which does not work in this case.
 * This filter holds the span and sets it appropriately during request processing when we are pretty sure
 * there is no more thread switchery.
 *
 * Inspired from Rawls: https://github.com/broadinstitute/rawls/blob/develop/core/src/main/scala/org/broadinstitute/dsde/rawls/util/WithSpanFilter.scala
 */
@Provider
class WithSpanFilter(span: Span) extends ClientRequestFilter with ClientResponseFilter {
  private var scope: Scope = _

  override def filter(requestContext: ClientRequestContext): Unit =
    scope = Tracing.getTracer.withSpan(span)

  override def filter(requestContext: ClientRequestContext, responseContext: ClientResponseContext): Unit =
    scope.close()
}
