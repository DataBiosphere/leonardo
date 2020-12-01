package org.broadinstitute.dsde.workbench

import java.time.Instant
import java.util.concurrent.TimeUnit

import cats.implicits._
import cats.{Applicative, Functor}
import cats.effect.Timer
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.model.TraceId
import scala.concurrent.duration._

package object leonardo {
  type LabelMap = Map[String, String]
  val autoPauseOffValue = 0 minutes
  val traceIdHeaderString = "X-Cloud-Trace-Context"

  // convenience to get now as a F[Instant] using a Timer
  def nowInstant[F[_]: Timer: Functor]: F[Instant] =
    Timer[F].clock.realTime(TimeUnit.MILLISECONDS).map(Instant.ofEpochMilli)

  // converts an Ask[F, RuntimeServiceContext] to an  Ask[F, TraceId]
  // (you'd think Ask would have a `map` function)
  implicit def ctxConversion[F[_]: Applicative](
    implicit as: Ask[F, AppContext]
  ): Ask[F, TraceId] =
    new Ask[F, TraceId] {
      override def applicative: Applicative[F] = as.applicative
      override def ask[E2 >: TraceId]: F[E2] = as.ask.map(_.traceId)
    }

  private val leoNameReg = "([a-z|0-9|-])*".r

  def validateName(nameString: String): Either[String, String] =
    nameString match {
      case leoNameReg(_) => Right(nameString)
      case _ =>
        Left(
          s"Invalid name ${nameString}. Only lowercase alphanumeric characters, numbers and dashes are allowed in leo names"
        )
    }
}
