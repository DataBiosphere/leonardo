package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import cats.implicits._
import io.circe.{Decoder, DecodingFailure}
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.http.service.{CreateRuntimeRequest, RuntimeConfigRequest}
import org.broadinstitute.dsde.workbench.model.google.GcsPath

object LeoRoutesJsonCodec {
  val invalidPropertiesError = DecodingFailure("invalid properties", List.empty)

  implicit val dataprocConfigDecoder: Decoder[RuntimeConfigRequest.DataprocConfig] = Decoder.instance { c =>
    for {
      numberOfWorkersInput <- c.downField("numberOfWorkers").as[Option[Int]]
      masterMachineType <- c.downField("masterMachineType").as[Option[String]]
      _ <- if (masterMachineType.nonEmpty && masterMachineType.exists(_.isEmpty)) Left(emptyMasterMachineType)
      else Right(())
      masterDiskSize <- c
        .downField("masterDiskSize")
        .as[Option[Int]]
        .flatMap(x => if (x.exists(_ < 0)) Left(negativeNumberDecodingFailure) else Right(x))
      workerMachineType <- c.downField("workerMachineType").as[Option[String]]
      workerDiskSize <- c
        .downField("workerDiskSize")
        .as[Option[Int]]
        .flatMap(x => if (x.exists(_ < 0)) Left(negativeNumberDecodingFailure) else Right(x))
      numberOfWorkerLocalSSDs <- c
        .downField("numberOfWorkerLocalSSDs")
        .as[Option[Int]]
        .flatMap(x => if (x.exists(_ < 0)) Left(negativeNumberDecodingFailure) else Right(x))
      numberOfPreemptibleWorkers <- c
        .downField("numberOfPreemptibleWorkers")
        .as[Option[Int]]
        .flatMap(x => if (x.exists(_ < 0)) Left(negativeNumberDecodingFailure) else Right(x))
      res <- numberOfWorkersInput match {
        case Some(x) if x < 0 => Left(negativeNumberDecodingFailure)
        case Some(0)          => Right(RuntimeConfigRequest.DataprocConfig(Some(0), masterMachineType, masterDiskSize))
        case Some(1)          => Left(oneWorkerSpecifiedDecodingFailure)
        case Some(x) =>
          Right(
            RuntimeConfigRequest.DataprocConfig(Some(x),
                                                masterMachineType,
                                                masterDiskSize,
                                                workerMachineType,
                                                workerDiskSize,
                                                numberOfWorkerLocalSSDs,
                                                numberOfPreemptibleWorkers)
          )
        case None =>
          Right(
            RuntimeConfigRequest.DataprocConfig(None,
                                                masterMachineType,
                                                masterDiskSize,
                                                workerMachineType,
                                                workerDiskSize,
                                                numberOfWorkerLocalSSDs,
                                                numberOfPreemptibleWorkers)
          )
      }
    } yield res
  }

  implicit val gceConfigDecoder: Decoder[RuntimeConfigRequest.GceConfig] = Decoder.forProduct2(
    "machineType",
    "diskSize"
  )((mt, ds) => RuntimeConfigRequest.GceConfig(mt, ds))

  implicit val runtimeConfigDecoder: Decoder[RuntimeConfigRequest] = Decoder.instance { x =>
    //For newer version of requests, we use `cloudService` field to distinguish whether user is
    val newDecoder = for {
      cloudService <- x.downField("cloudService").as[CloudService]
      r <- cloudService match {
        case CloudService.Dataproc =>
          x.as[RuntimeConfigRequest.DataprocConfig]
        case CloudService.GCE =>
          x.as[RuntimeConfigRequest.GceConfig]
      }
    } yield r

    newDecoder // when the request has `cloudService` field specified
      .orElse(x.as[RuntimeConfigRequest.DataprocConfig]: Either[DecodingFailure, RuntimeConfigRequest]) //try decode as DataprocConfig
      .orElse(x.as[RuntimeConfigRequest.GceConfig]) //try decode as GceConfig
  }

  implicit val createRuntimeRequestDecoder: Decoder[CreateRuntimeRequest] = Decoder.instance { c =>
    for {
      labels <- c.downField("labels").as[Option[Map[String, String]]]
      jupyterExtensionUri <- c.downField("jupyterExtensionUri").as[Option[GcsPath]]
      jupyterUserScriptUri <- c.downField("jupyterUserScriptUri").as[Option[UserScriptPath]]
      jupyterStartUserScriptUri <- c.downField("jupyterStartUserScriptUri").as[Option[UserScriptPath]]
      // TODO: handle GCE here
      machineConfig <- c.downField("machineConfig").as[Option[RuntimeConfigRequest.DataprocConfig]]
      properties <- c.downField("properties").as[Option[Map[String, String]]].map(_.getOrElse(Map.empty))
      isValid = properties.keys.toList.forall { s =>
        val prefix = s.split(":")(0)
        PropertyFilePrefix.stringToObject.get(prefix).isDefined
      }
      _ <- if (isValid) Right(()) else Left(invalidPropertiesError)
      stopAfterCreation <- c.downField("stopAfterCreation").as[Option[Boolean]]
      userJupyterExtensionConfig <- c.downField("userJupyterExtensionConfig").as[Option[UserJupyterExtensionConfig]]
      autopause <- c.downField("autopause").as[Option[Boolean]]
      autopauseThreshold <- c.downField("autopauseThreshold").as[Option[Int]]
      defaultClientId <- c.downField("defaultClientId").as[Option[String]]
      jupyterDockerImage <- c.downField("jupyterDockerImage").as[Option[ContainerImage]]
      toolDockerImage <- c.downField("toolDockerImage").as[Option[ContainerImage]]
      welderDockerImage <- c.downField("welderDockerImage").as[Option[ContainerImage]]
      scopes <- c.downField("scopes").as[Option[Set[String]]]
      enableWelder <- c.downField("enableWelder").as[Option[Boolean]]
      allowStop <- c.downField("allowStop").as[Option[Boolean]]
      customClusterEnvironmentVariables <- c
        .downField("customClusterEnvironmentVariables")
        .as[Option[Map[String, String]]]
    } yield CreateRuntimeRequest(
      labels.getOrElse(Map.empty),
      jupyterExtensionUri,
      jupyterUserScriptUri,
      jupyterStartUserScriptUri,
      machineConfig,
      properties,
      stopAfterCreation,
      allowStop.getOrElse(false),
      userJupyterExtensionConfig,
      autopause,
      autopauseThreshold,
      defaultClientId,
      jupyterDockerImage,
      toolDockerImage,
      welderDockerImage,
      scopes.getOrElse(Set.empty),
      enableWelder,
      customClusterEnvironmentVariables.getOrElse(Map.empty)
    )
  }
}
