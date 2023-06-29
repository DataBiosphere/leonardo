package org.broadinstitute.dsde.workbench

import cats.effect.Sync
import org.broadinstitute.dsde.workbench.google2.RegionName
import org.broadinstitute.dsde.workbench.google2.util.RetryPredicates
import org.http4s.{Entity, EntityEncoder}
import org.typelevel.ci._

import java.time.Instant

package object leonardo {
  type LabelMap = Map[String, String]
  // this value is the default for autopause, if none is specified. An autopauseThreshold of 0 indicates no autopause
  val autoPauseOffValue = 0
  val traceIdHeaderString = ci"X-Cloud-Trace-Context"

  val SECURITY_GROUP = "security-group"
  val SECURITY_GROUP_HIGH = "high"

  val AOU_UI_LABEL = "all-of-us"

  implicit def http4sBody[F[_], A](body: A)(implicit encoder: EntityEncoder[F, A]): Entity[F] =
    encoder.toEntity(body)

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

  val googleDataprocRetryPolicy = {
    // Google permission update is getting slow. This is to address the issue when creating dataproc
    // we're getting the following error
    // com.google.api.gax.rpc.PermissionDeniedException: io.grpc.StatusRuntimeException: PERMISSION_DENIED: Required 'compute.images.get' permission for 'projects/broad-dsp-gcr-public/global/images/custom-leo-image-dataproc-2-0-51-debian10-a46b242'. This usually happens when Dataproc Control Plane Identity service account does not have permission to validate resources in another project. For additional details see https://cloud.google.com/dataproc/docs/concepts/iam/dataproc-principals#service_agent_control_plane_identity
    val retryOnPermissionDenied: Throwable => Boolean = {
      case e: com.google.api.gax.rpc.PermissionDeniedException =>
        if ((e.getCause.getMessage contains "compute.images.get") || (e.getMessage contains "compute.images.get"))
          true
        else false
      case _ => false
    }

    RetryPredicates.retryConfigWithPredicates(
      RetryPredicates.standardGoogleRetryPredicate,
      RetryPredicates.whenStatusCode(400),
      retryOnPermissionDenied
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
