package org.broadinstitute.dsde.workbench

import cats.Applicative
import cats.effect.Sync
import cats.mtl.Ask
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.model.TraceId
import org.typelevel.ci._

import java.time.Instant

package object leonardo {
  type LabelMap = Map[String, String]
  //this value is the default for autopause, if none is specified. An autopauseThreshold of 0 indicates no autopause
  val autoPauseOffValue = 0
  val traceIdHeaderString = ci"X-Cloud-Trace-Context"

  // convenience to get now as a F[Instant] using a Timer
  def nowInstant[F[_]: Sync]: F[Instant] =
    Sync[F].realTimeInstant

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
