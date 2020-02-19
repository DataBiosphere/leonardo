package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import cats.implicits._
import io.circe.{Decoder, DecodingFailure}
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.http.service.{
  CreateClusterAPIResponse,
  GetClusterResponse,
  ListClusterResponse,
  UpdateClusterResponse
}
import org.broadinstitute.dsde.workbench.leonardo.model.LeonardoJsonSupport._
import org.broadinstitute.dsde.workbench.leonardo.model.google.GoogleJsonSupport._
import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterRequest, PropertyFilePrefix, RuntimeConfigRequest}
import org.broadinstitute.dsde.workbench.model.WorkbenchIdentityJsonSupport._
import org.broadinstitute.dsde.workbench.model.google.GcsPath
import org.broadinstitute.dsde.workbench.model.google.GoogleModelJsonSupport.{GcsPathFormat => _, _}
import spray.json.{JsObject, _}

object LeoRoutesJsonCodec extends DefaultJsonProtocol {
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

  implicit val gceConfigDecoder: Decoder[model.RuntimeConfigRequest.GceConfig] = Decoder.forProduct2(
    "machineType",
    "diskSize"
  )((mt, ds) => RuntimeConfigRequest.GceConfig(mt, ds))

  implicit val runtimeConfigDecoder: Decoder[model.RuntimeConfigRequest] = Decoder.instance { x =>
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
      .orElse(x.as[model.RuntimeConfigRequest.DataprocConfig]: Either[DecodingFailure, RuntimeConfigRequest]) //try decode as DataprocConfig
      .orElse(x.as[model.RuntimeConfigRequest.GceConfig]) //try decode as GceConfig
  }

  implicit val clusterRequestDecoder: Decoder[ClusterRequest] = Decoder.instance { c =>
    for {
      labels <- c.downField("labels").as[Option[Map[String, String]]]
      jupyterExtensionUri <- c.downField("jupyterExtensionUri").as[Option[GcsPath]]
      jupyterUserScriptUri <- c.downField("jupyterUserScriptUri").as[Option[UserScriptPath]]
      jupyterStartUserScriptUri <- c.downField("jupyterStartUserScriptUri").as[Option[UserScriptPath]]
      machineConfig <- c.downField("machineConfig").as[Option[model.RuntimeConfigRequest.DataprocConfig]]
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
    } yield ClusterRequest(
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

  implicit val runtimeConfigWriter: RootJsonWriter[RuntimeConfig] = (obj: RuntimeConfig) => {
    val allFields = obj match {
      case x: RuntimeConfig.GceConfig =>
        Map(
          "machineType" -> x.machineType.value.toJson,
          "diskSize" -> x.diskSize.toJson,
          "cloudService" -> x.cloudService.asString.toJson
        )
      case x: RuntimeConfig.DataprocConfig =>
        Map(
          "numberOfWorkers" -> x.numberOfWorkers.toJson,
          "masterMachineType" -> x.masterMachineType.toJson,
          "masterDiskSize" -> x.masterDiskSize.toJson,
          "workerMachineType" -> x.workerMachineType.toJson,
          "workerDiskSize" -> x.workerDiskSize.map(_.toJson).getOrElse(JsNull),
          "cloudService" -> x.cloudService.asString.toJson,
          "numberOfWorkerLocalSSDs" -> x.numberOfWorkerLocalSSDs.map(_.toJson).getOrElse(JsNull),
          "numberOfPreemptibleWorkers" -> x.numberOfPreemptibleWorkers.map(_.toJson).getOrElse(JsNull)
        )
    }

    val presentFields = allFields.filter(_._2 != JsNull)

    JsObject(presentFields)
  }

  implicit val listClusterResponseWriter: RootJsonWriter[ListClusterResponse] = (obj: ListClusterResponse) => {
    val allFields = Map(
      "id" -> obj.id.toJson,
      "internalId" -> obj.internalId.asString.toJson,
      "clusterName" -> obj.clusterName.toJson,
      "googleId" -> obj.dataprocInfo.map(_.googleId.toJson).getOrElse(JsNull),
      "googleProject" -> obj.googleProject.toJson,
      "serviceAccountInfo" -> obj.serviceAccountInfo.toJson,
      "machineConfig" -> obj.machineConfig.toJson,
      "clusterUrl" -> obj.clusterUrl.toJson,
      "operationName" -> obj.dataprocInfo.map(_.operationName.toJson).getOrElse(JsNull),
      "status" -> obj.status.toJson,
      "hostIp" -> obj.dataprocInfo.map(_.hostIp.toJson).getOrElse(JsNull),
      "creator" -> obj.auditInfo.creator.toJson,
      "createdDate" -> obj.auditInfo.createdDate.toJson,
      "destroyedDate" -> obj.auditInfo.destroyedDate.toJson,
      "kernelFoundBusyDate" -> obj.auditInfo.kernelFoundBusyDate.toJson,
      "labels" -> obj.labels.toJson,
      "jupyterExtensionUri" -> obj.jupyterExtensionUri.toJson,
      "jupyterUserScriptUri" -> obj.jupyterUserScriptUri.toJson,
      "stagingBucket" -> obj.dataprocInfo.map(_.stagingBucket.toJson).getOrElse(JsNull),
      "instances" -> obj.instances.toJson,
      "dateAccessed" -> obj.auditInfo.dateAccessed.toJson,
      "autopauseThreshold" -> obj.autopauseThreshold.toJson,
      "defaultClientId" -> obj.defaultClientId.toJson,
      "stopAfterCreation" -> obj.stopAfterCreation.toJson,
      "welderEnabled" -> obj.welderEnabled.toJson,
      "scopes" -> List
        .empty[String]
        .toJson //TODO: stubbing this out temporarily until AOU move to new swagger generated client
    )

    val presentFields = allFields.filter(_._2 != JsNull)

    JsObject(presentFields)
  }

  implicit object GetClusterFormat extends RootJsonWriter[GetClusterResponse] {
    override def write(obj: GetClusterResponse): JsValue = {
      val allFields = List(
        "id" -> obj.id.toJson,
        "internalId" -> obj.internalId.asString.toJson,
        "clusterName" -> obj.clusterName.toJson,
        "googleId" -> obj.dataprocInfo.map(_.googleId).toJson,
        "googleProject" -> obj.googleProject.toJson,
        "serviceAccountInfo" -> obj.serviceAccountInfo.toJson,
        "machineConfig" -> obj.runtimeConfig.toJson,
        "clusterUrl" -> obj.clusterUrl.toJson,
        "operationName" -> obj.dataprocInfo.map(_.operationName).toJson,
        "status" -> obj.status.toJson,
        "hostIp" -> obj.dataprocInfo.map(_.hostIp).toJson,
        "creator" -> obj.auditInfo.creator.toJson,
        "createdDate" -> obj.auditInfo.createdDate.toJson,
        "destroyedDate" -> obj.auditInfo.destroyedDate.toJson,
        "kernelFoundBusyDate" -> obj.auditInfo.kernelFoundBusyDate.toJson,
        "labels" -> obj.labels.toJson,
        "jupyterExtensionUri" -> obj.jupyterExtensionUri.toJson,
        "jupyterUserScriptUri" -> obj.jupyterUserScriptUri.toJson,
        "jupyterStartUserScriptUri" -> obj.jupyterStartUserScriptUri.toJson,
        "stagingBucket" -> obj.dataprocInfo.map(_.stagingBucket).toJson,
        "errors" -> obj.errors.toJson,
        "instances" -> obj.instances.toJson,
        "userJupyterExtensionConfig" -> obj.userJupyterExtensionConfig.toJson,
        "dateAccessed" -> obj.auditInfo.dateAccessed.toJson,
        "autopauseThreshold" -> obj.autopauseThreshold.toJson,
        "defaultClientId" -> obj.defaultClientId.toJson,
        "stopAfterCreation" -> obj.stopAfterCreation.toJson,
        "clusterImages" -> obj.clusterImages.toJson,
        "scopes" -> obj.scopes.toJson,
        "welderEnabled" -> obj.welderEnabled.toJson
      )

      val presentFields = allFields.filter(_._2 != JsNull)

      JsObject(presentFields: _*)
    }
  }

  implicit object CreateClusterAPIResponseFormat extends RootJsonWriter[CreateClusterAPIResponse] {
    override def write(obj: CreateClusterAPIResponse): JsValue = {
      val allFields = List(
        "id" -> obj.id.toJson,
        "internalId" -> obj.internalId.asString.toJson,
        "clusterName" -> obj.clusterName.toJson,
        "googleId" -> obj.dataprocInfo.map(_.googleId).toJson,
        "googleProject" -> obj.googleProject.toJson,
        "serviceAccountInfo" -> obj.serviceAccountInfo.toJson,
        "machineConfig" -> obj.runtimeConfig.toJson,
        "clusterUrl" -> obj.clusterUrl.toJson,
        "operationName" -> obj.dataprocInfo.map(_.operationName).toJson,
        "status" -> obj.status.toJson,
        "hostIp" -> obj.dataprocInfo.map(_.hostIp).toJson,
        "creator" -> obj.auditInfo.creator.toJson,
        "createdDate" -> obj.auditInfo.createdDate.toJson,
        "destroyedDate" -> obj.auditInfo.destroyedDate.toJson,
        "kernelFoundBusyDate" -> obj.auditInfo.kernelFoundBusyDate.toJson,
        "labels" -> obj.labels.toJson,
        "jupyterExtensionUri" -> obj.jupyterExtensionUri.toJson,
        "jupyterUserScriptUri" -> obj.jupyterUserScriptUri.toJson,
        "jupyterStartUserScriptUri" -> obj.jupyterStartUserScriptUri.toJson,
        "stagingBucket" -> obj.dataprocInfo.map(_.stagingBucket).toJson,
        "errors" -> obj.errors.toJson,
        "instances" -> obj.instances.toJson,
        "userJupyterExtensionConfig" -> obj.userJupyterExtensionConfig.toJson,
        "dateAccessed" -> obj.auditInfo.dateAccessed.toJson,
        "autopauseThreshold" -> obj.autopauseThreshold.toJson,
        "defaultClientId" -> obj.defaultClientId.toJson,
        "stopAfterCreation" -> obj.stopAfterCreation.toJson,
        "clusterImages" -> obj.clusterImages.toJson,
        "scopes" -> obj.scopes.toJson,
        "welderEnabled" -> obj.welderEnabled.toJson
      )

      val presentFields = allFields.filter(_._2 != JsNull)

      JsObject(presentFields: _*)
    }
  }
  implicit object UpdateClusterResponseFormat extends RootJsonWriter[UpdateClusterResponse] {
    override def write(obj: UpdateClusterResponse): JsValue = {
      val allFields = List(
        "id" -> obj.id.toJson,
        "internalId" -> obj.internalId.asString.toJson,
        "clusterName" -> obj.clusterName.toJson,
        "googleId" -> obj.dataprocInfo.map(_.googleId).toJson,
        "googleProject" -> obj.googleProject.toJson,
        "serviceAccountInfo" -> obj.serviceAccountInfo.toJson,
        "machineConfig" -> obj.runtimeConfig.toJson, //Note, for this response, we're still encoding runtimeConfig as machineConfig
        "clusterUrl" -> obj.clusterUrl.toJson,
        "operationName" -> obj.dataprocInfo.map(_.operationName).toJson,
        "status" -> obj.status.toJson,
        "hostIp" -> obj.dataprocInfo.map(_.hostIp).toJson,
        "creator" -> obj.auditInfo.creator.toJson,
        "createdDate" -> obj.auditInfo.createdDate.toJson,
        "destroyedDate" -> obj.auditInfo.destroyedDate.toJson,
        "kernelFoundBusyDate" -> obj.auditInfo.kernelFoundBusyDate.toJson,
        "labels" -> obj.labels.toJson,
        "jupyterExtensionUri" -> obj.jupyterExtensionUri.toJson,
        "jupyterUserScriptUri" -> obj.jupyterUserScriptUri.toJson,
        "jupyterStartUserScriptUri" -> obj.jupyterStartUserScriptUri.toJson,
        "stagingBucket" -> obj.dataprocInfo.map(_.stagingBucket).toJson,
        "errors" -> obj.errors.toJson,
        "instances" -> obj.instances.toJson,
        "userJupyterExtensionConfig" -> obj.userJupyterExtensionConfig.toJson,
        "dateAccessed" -> obj.auditInfo.dateAccessed.toJson,
        "autopauseThreshold" -> obj.autopauseThreshold.toJson,
        "defaultClientId" -> obj.defaultClientId.toJson,
        "stopAfterCreation" -> obj.stopAfterCreation.toJson,
        "clusterImages" -> obj.clusterImages.toJson,
        "scopes" -> obj.scopes.toJson,
        "welderEnabled" -> obj.welderEnabled.toJson
      )

      val presentFields = allFields.filter(_._2 != JsNull)

      JsObject(presentFields: _*)
    }
  }
}
