package org.broadinstitute.dsde.workbench

import cats.effect.Sync
import org.broadinstitute.dsde.workbench.google2.RegionName
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

  private val leoNameReg = "([a-z|0-9|-])*".r

  def validateName(nameString: String): Either[String, String] =
    nameString match {
      case leoNameReg(_) => Right(nameString)
      case _ =>
        Left(
          s"Invalid name ${nameString}. Only lowercase alphanumeric characters, numbers and dashes are allowed in leo names"
        )
    }

  val allSupportedRegions = List(
    RegionName("us-central1"),
    RegionName("northamerica-northeast1"),
    RegionName("southamerica-east1"),
    RegionName("us-east1"),
    RegionName("us-east4"),
    RegionName("us-west1"),
    RegionName("us-west2"),
    RegionName("us-west3"),
    RegionName("us-west4"),
    RegionName("europe-central2"),
    RegionName("europe-north1"),
    RegionName("europe-west1"),
    RegionName("europe-west2"),
    RegionName("europe-west3"),
    RegionName("europe-west4"),
    RegionName("europe-west6"),
    RegionName("asia-east1"),
    RegionName("asia-east2"),
    RegionName("asia-northeast1"),
    RegionName("asia-northeast2"),
    RegionName("asia-northeast3"),
    RegionName("asia-south1"),
    RegionName("asia-southeast1"),
    RegionName("asia-southeast2"),
    RegionName("australia-southeast1"),
    RegionName("northamerica-northeast2")
  )
}
