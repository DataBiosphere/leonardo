package org.broadinstitute.dsde.workbench.leonardo

import java.time.Instant

import cats.implicits._
import io.circe.{Decoder, Encoder}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsObjectName, GoogleProject}

object JsonCodec {
  implicit val googleProjectDecoder: Decoder[GoogleProject] = Decoder.decodeString.map(GoogleProject)
  implicit val machineConfigDecoder: Decoder[MachineConfig] = Decoder.forProduct7(
    "numberOfWorkers",
    "masterMachineType",
    "masterDiskSize",
    "workerMachineType",
    "workerDiskSize",
    "numberOfWorkerLocalSSDs",
    "numberOfPreemptibleWorkers"
  )(MachineConfig.apply)
  implicit val workbenchEmailDecoder: Decoder[WorkbenchEmail] = Decoder.decodeString.map(WorkbenchEmail)
  implicit val serviceAccountInfoDecoder: Decoder[ServiceAccountInfo] = Decoder.forProduct2(
    "clusterServiceAccount",
    "notebookServiceAccount"
  )(ServiceAccountInfo.apply)
  implicit val gcsBucketNameDecoder: Decoder[GcsBucketName] = Decoder.decodeString.map(GcsBucketName)
  implicit val gcsBucketNameEncoder: Encoder[GcsBucketName] = Encoder.encodeString.contramap(_.value)
  implicit val gcsObjectNameEncoder: Encoder[GcsObjectName] = Encoder.encodeString.contramap(_.value)
  implicit val instantDecoder: Decoder[Instant] =
    Decoder.decodeString.emap(s => Either.catchNonFatal(Instant.parse(s)).leftMap(_.getMessage))
  implicit val clusterErrorDecoder: Decoder[ClusterError] = Decoder.forProduct3(
    "errorMessage",
    "errorCode",
    "timestamp"
  )(ClusterError.apply)
}
