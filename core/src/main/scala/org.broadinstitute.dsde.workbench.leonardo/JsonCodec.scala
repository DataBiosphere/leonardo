package org.broadinstitute.dsde.workbench.leonardo

import java.time.Instant

import cats.implicits._
import io.circe.{Decoder, DecodingFailure, Encoder}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsObjectName, GcsPath, GoogleProject}
import org.broadinstitute.dsde.workbench.model.google.parseGcsPath

object JsonCodec {
  val negativeNumberDecodingFailure = DecodingFailure("Negative number is not allowed", List.empty)
  val oneWorkerSpecifiedDecodingFailure = DecodingFailure("Google Dataproc does not support clusters with 1 non-preemptible worker. Must be 0, 2 or more.", List.empty)
  val emptyMasterMachineType = DecodingFailure("masterMachineType can not be an empty string", List.empty)

  implicit val gcsBucketNameEncoder: Encoder[GcsBucketName] = Encoder.encodeString.contramap(_.value)
  implicit val gcsObjectNameEncoder: Encoder[GcsObjectName] = Encoder.encodeString.contramap(_.value)

  implicit val googleProjectDecoder: Decoder[GoogleProject] = Decoder.decodeString.map(GoogleProject)
  implicit val machineConfigDecoder: Decoder[MachineConfig] = Decoder.instance {
    c =>
      val minimumDiskSize = 10
      for {
          numberOfWorkersInput <- c.downField("numberOfWorkers").as[Int]
          masterMachineType <- c.downField("masterMachineType").as[String]
          _ <- if(masterMachineType.isEmpty) Left(emptyMasterMachineType) else Right(())
          masterDiskSizeInput <- c.downField("masterDiskSize").as[Int].flatMap(x => if(x < 0) Left(negativeNumberDecodingFailure) else Right(x))
          masterDiskSize = math.max(minimumDiskSize, masterDiskSizeInput)
          res <- numberOfWorkersInput match {
            case x if x < 0 => Left(negativeNumberDecodingFailure)
            case 0 => Right(MachineConfig(0, masterMachineType, masterDiskSize))
            case 1 => Left(oneWorkerSpecifiedDecodingFailure)
            case x => for {
              workerMachineType <- c.downField("workerMachineType").as[Option[String]]
              workerDiskSize <- c.downField("workerDiskSize").as[Option[Int]].flatMap(x => if(x.exists(_ < 0)) Left(negativeNumberDecodingFailure) else Right(x))
              numberOfWorkerLocalSSDs <- c.downField("numberOfWorkerLocalSSDs").as[Option[Int]].flatMap(x => if(x.exists(_ < 0)) Left(negativeNumberDecodingFailure) else Right(x))
              numberOfPreemptibleWorkers <- c.downField("numberOfPreemptibleWorkers").as[Option[Int]].flatMap(x => if(x.exists(_ < 0)) Left(negativeNumberDecodingFailure) else Right(x))
            } yield MachineConfig(x, masterMachineType, masterDiskSize, workerMachineType, workerDiskSize, numberOfWorkerLocalSSDs, numberOfPreemptibleWorkers)
          }
      } yield res
  }
  implicit val workbenchEmailDecoder: Decoder[WorkbenchEmail] = Decoder.decodeString.map(WorkbenchEmail)
  implicit val serviceAccountInfoDecoder: Decoder[ServiceAccountInfo] = Decoder.forProduct2(
    "clusterServiceAccount",
    "notebookServiceAccount"
  )(ServiceAccountInfo.apply)
  implicit val gcsBucketNameDecoder: Decoder[GcsBucketName] = Decoder.decodeString.map(GcsBucketName)

  implicit val instantDecoder: Decoder[Instant] =
    Decoder.decodeString.emap(s => Either.catchNonFatal(Instant.parse(s)).leftMap(_.getMessage))
  implicit val clusterErrorDecoder: Decoder[ClusterError] = Decoder.forProduct3(
    "errorMessage",
    "errorCode",
    "timestamp"
  )(ClusterError.apply)
  implicit val gcsPathDecoder: Decoder[GcsPath] = Decoder.decodeString.emap(s => parseGcsPath(s).leftMap(_.value))
  implicit val userScriptPathDecoder: Decoder[UserScriptPath] = Decoder.decodeString.emap{
    s => UserScriptPath.stringToUserScriptPath(s).leftMap(_.getMessage)
  }
  implicit val userJupyterExtensionConfigDecoder: Decoder[UserJupyterExtensionConfig] = Decoder.forProduct4(
    "nbExtensions",
    "serverExtensions",
    "combinedExtensions",
    "labExtensions"
  )(UserJupyterExtensionConfig.apply)
  implicit val containerImageDecoder: Decoder[ContainerImage] = Decoder.decodeString.emap(s => ContainerImage.stringToJupyterDockerImage(s).toRight(s"invalid container image ${s}"))
}