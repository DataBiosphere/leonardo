package org.broadinstitute.dsde.workbench

import java.time.Instant
import java.util.concurrent.TimeUnit

import cats.implicits._
import cats.{Applicative, Functor}
import cats.effect.Timer
import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.model.TraceId

package object leonardo {
  type LabelMap = Map[String, String]
  //this value is the default for autopause, if none is specified. An autopauseThreshold of 0 indicates no autopause
  val autoPauseOffValue = 0
  val traceIdHeaderString = "X-Cloud-Trace-Context"

  // convenience to get now as a F[Instant] using a Timer
  def nowInstant[F[_]: Timer: Functor]: F[Instant] =
    Timer[F].clock.realTime(TimeUnit.MILLISECONDS).map(Instant.ofEpochMilli)

  // converts an ApplicativeAsk[F, RuntimeServiceContext] to an  ApplicativeAsk[F, TraceId]
  // (you'd think ApplicativeAsk would have a `map` function)
  implicit def ctxConversion[F[_]: Applicative](
    implicit as: ApplicativeAsk[F, AppContext]
  ): ApplicativeAsk[F, TraceId] =
    new ApplicativeAsk[F, TraceId] {
      override val applicative: Applicative[F] = Applicative[F]
      override def ask: F[TraceId] = as.ask.map(_.traceId)
      override def reader[A](f: TraceId => A): F[A] = ask.map(f)
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
