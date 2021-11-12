package org.broadinstitute.dsde.workbench.leonardo.http.api

import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{HttpHeader, HttpRequest}
import io.opencensus.scala.Tracing
import io.opencensus.scala.akka.http.TracingDirective
import io.opencensus.scala.http.propagation.{B3FormatPropagation, Propagation}
import io.opencensus.trace.samplers.Samplers
import io.opencensus.trace.{EndSpanOptions, Span, SpanBuilder, SpanContext, Status}

import scala.concurrent.{ExecutionContext, Future}

// This whole file is just workaround for the fact
object CustomTracingDirectives extends TracingDirective {
  private val tracingInternal = new Tracing {
    private val tracer = io.opencensus.trace.Tracing.getTracer

    override def startSpan(name: String): Span = buildSpan(tracer.spanBuilder(name))

    override def startSpanWithParent(name: String, parent: Span): Span =
      buildSpan(tracer.spanBuilderWithExplicitParent(name, parent))

    override def startSpanWithRemoteParent(name: String, parentContext: SpanContext): Span =
      buildSpan(tracer.spanBuilderWithRemoteParent(name, parentContext))

    override def setStatus(span: Span, status: Status): Unit = span.setStatus(status)

    override def endSpan(span: Span): Unit = span.end()

    override def endSpan(span: Span, status: Status): Unit =
      span.end(EndSpanOptions.builder().setStatus(status).build())

    override def trace[T](name: String, failureStatus: Throwable => Status)(f: Span => Future[T])(
      implicit ec: ExecutionContext
    ): Future[T] = Tracing.trace(name, failureStatus)(f)

    override def traceWithParent[T](name: String, parentSpan: Span, failureStatus: Throwable => Status)(
      f: Span => Future[T]
    )(implicit ec: ExecutionContext): Future[T] = Tracing.traceWithParent(name, parentSpan, failureStatus)(f)
  }

  private def buildSpan(builder: SpanBuilder): Span =
    builder
      .setSampler(Samplers.probabilitySampler(0.2))
      .startSpan()

  override protected def tracing: Tracing = tracingInternal

  override protected def propagation: Propagation[HttpHeader, HttpRequest] = CustomAkkaB3FormatPropagation
}

object CustomAkkaB3FormatPropagation extends B3FormatPropagation[HttpHeader, HttpRequest] {

  override def headerValue(req: HttpRequest, key: String): Option[String] =
    req.headers
      .find(_.lowercaseName() == key.toLowerCase)
      .map(_.value())

  override def createHeader(key: String, value: String): HttpHeader =
    RawHeader(key, value)
}
