package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import io.circe.{Decoder, DecodingFailure}
import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.http.service.CreateRuntimeRequest
import org.broadinstitute.dsde.workbench.model.google.GcsPath

// Shared routes specific codecs live in this file. When Routes file get too big, we can potentially move codec to this file too
object LeoRoutesJsonCodec {
  val invalidPropertiesError =
    DecodingFailure("invalid properties. An example of property is `spark:spark.executor.cores`", List.empty)

  implicit val dataprocConfigDecoder: Decoder[RuntimeConfigRequest.DataprocConfig] = Decoder.instance { c =>
    for {
      numberOfWorkersInput <- c.downField("numberOfWorkers").as[Option[Int]]
      masterMachineType <- c.downField("masterMachineType").as[Option[MachineTypeName]]
      propertiesOpt <- c.downField("properties").as[Option[LabelMap]]
      properties = propertiesOpt.getOrElse(Map.empty)
      isValid = properties.keys.toList.forall { s =>
        val prefix = s.split(":")(0)
        PropertyFilePrefix.stringToObject.get(prefix).isDefined
      }
      _ <- if (isValid) Right(()) else Left(invalidPropertiesError)
      diskSizeBeforeValidation <- c
        .downField("masterDiskSize")
        .as[Option[DiskSize]]
      masterDiskSize <- if (diskSizeBeforeValidation.exists(x => x.gb < 50)) // Dataproc cluster doesn't have a separate boot disk, hence disk size needs to be larger than the VM image
        Left(DecodingFailure("Minimum required disk size is 50GB", List.empty))
      else Right(diskSizeBeforeValidation)
      workerMachineType <- c.downField("workerMachineType").as[Option[MachineTypeName]]
      workerDiskSize <- c
        .downField("workerDiskSize")
        .as[Option[DiskSize]]
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
        case Some(0) =>
          Right(
            RuntimeConfigRequest
              .DataprocConfig(Some(0), masterMachineType, masterDiskSize, None, None, None, None, properties)
          )
        case Some(1) => Left(oneWorkerSpecifiedDecodingFailure)
        case Some(x) =>
          Right(
            RuntimeConfigRequest.DataprocConfig(Some(x),
                                                masterMachineType,
                                                masterDiskSize,
                                                workerMachineType,
                                                workerDiskSize,
                                                numberOfWorkerLocalSSDs,
                                                numberOfPreemptibleWorkers,
                                                properties)
          )
        case None =>
          Right(
            RuntimeConfigRequest.DataprocConfig(None,
                                                masterMachineType,
                                                masterDiskSize,
                                                workerMachineType,
                                                workerDiskSize,
                                                numberOfWorkerLocalSSDs,
                                                numberOfPreemptibleWorkers,
                                                properties)
          )
      }
    } yield res
  }

  implicit val createRuntimeRequestDecoder: Decoder[CreateRuntimeRequest] = Decoder.instance { c =>
    for {
      labels <- c.downField("labels").as[Option[Map[String, String]]]
      jupyterExtensionUri <- c.downField("jupyterExtensionUri").as[Option[GcsPath]]
      jupyterUserScriptUri <- c.downField("jupyterUserScriptUri").as[Option[UserScriptPath]]
      jupyterStartUserScriptUri <- c.downField("jupyterStartUserScriptUri").as[Option[UserScriptPath]]
      // TODO: handle GCE here
      machineConfig <- c.downField("machineConfig").as[Option[RuntimeConfigRequest.DataprocConfig]]
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
    } yield {
      val userJupyterExt = jupyterExtensionUri match {
        case Some(ext) => Map("notebookExtension" -> ext.toUri)
        case None      => Map.empty[String, String]
      }

      // add the userJupyterExt to the nbExtensions
      val updatedUserJupyterExtensionConfig = userJupyterExtensionConfig match {
        case Some(config) => config.copy(nbExtensions = config.nbExtensions ++ userJupyterExt)
        case None         => UserJupyterExtensionConfig(userJupyterExt, Map.empty, Map.empty, Map.empty)
      }

      CreateRuntimeRequest(
        labels.getOrElse(Map.empty),
        jupyterUserScriptUri,
        jupyterStartUserScriptUri,
        machineConfig,
        stopAfterCreation,
        allowStop.getOrElse(false),
        if (updatedUserJupyterExtensionConfig.asLabels.isEmpty) None else Some(updatedUserJupyterExtensionConfig),
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
}
