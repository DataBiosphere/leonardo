package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import org.broadinstitute.dsde.workbench.leonardo.http.service.{
  CreateRuntimeResponse,
  GetRuntimeResponse,
  ListRuntimeResponse
}
import org.broadinstitute.dsde.workbench.model.WorkbenchIdentityJsonSupport._
import org.broadinstitute.dsde.workbench.model.google.GoogleModelJsonSupport._
import spray.json._

// TODO: remove this in favor of our circe codecs
object LeoRoutesSprayJsonCodec extends DefaultJsonProtocol {

  implicit val runtimeConfigWriter: RootJsonWriter[RuntimeConfig] = (obj: RuntimeConfig) => {
    val allFields = obj match {
      case x: RuntimeConfig.GceConfig =>
        Map(
          "machineType" -> x.machineType.value.toJson,
          "diskSize" -> x.diskSize.gb.toJson,
          "cloudService" -> x.cloudService.asString.toJson
        )
      case x: RuntimeConfig.DataprocConfig =>
        Map(
          "numberOfWorkers" -> x.numberOfWorkers.toJson,
          "masterMachineType" -> x.masterMachineType.value.toJson,
          "masterDiskSize" -> x.masterDiskSize.gb.toJson,
          "workerMachineType" -> x.workerMachineType.map(_.value).toJson,
          "workerDiskSize" -> x.workerDiskSize.map(_.gb.toJson).getOrElse(JsNull),
          "cloudService" -> x.cloudService.asString.toJson,
          "numberOfWorkerLocalSSDs" -> x.numberOfWorkerLocalSSDs.map(_.toJson).getOrElse(JsNull),
          "numberOfPreemptibleWorkers" -> x.numberOfPreemptibleWorkers.map(_.toJson).getOrElse(JsNull)
        )
      case x: RuntimeConfig.GceWithPdConfig =>
        Map(
          "machineType" -> x.machineType.value.toJson,
          "diskSize" -> x.diskSize.gb.toJson,
          "cloudService" -> x.cloudService.asString.toJson
        )
    }

    val presentFields = allFields.filter(_._2 != JsNull)

    JsObject(presentFields)
  }

  implicit val dataprocInstanceKeyWriter: RootJsonWriter[DataprocInstanceKey] = (obj: DataprocInstanceKey) => {
    val allFields = Map(
      "name" -> obj.name.value.toJson,
      "project" -> obj.project.value.toJson,
      "zone" -> obj.zone.value.toJson
    )

    val presentFields = allFields.filter(_._2 != JsNull)

    JsObject(presentFields)
  }

  implicit val dataprocInstanceWriter: RootJsonFormat[DataprocInstance] = new RootJsonFormat[DataprocInstance] {
    override def read(json: JsValue): DataprocInstance = ???

    override def write(obj: DataprocInstance): JsValue = {
      val allFields = Map(
        "createdDate" -> obj.createdDate.toJson,
        "dataprocRole" -> obj.dataprocRole.toString.toJson,
        "googleId" -> obj.googleId.toJson,
        "status" -> obj.status.toString.toJson,
        "key" -> obj.key.toJson
      )

      val presentFields = allFields.filter(_._2 != JsNull)

      JsObject(presentFields)
    }
  }

  implicit val runtimeStatusFormat: RootJsonWriter[RuntimeStatus] = (status: RuntimeStatus) => {
    val stringFormat = status match {
      case RuntimeStatus.PreCreating => RuntimeStatus.Creating.toString
      case RuntimeStatus.PreStarting => RuntimeStatus.Starting.toString
      case RuntimeStatus.PreStopping => RuntimeStatus.Stopping.toString
      case RuntimeStatus.PreDeleting => RuntimeStatus.Deleting.toString
      case _                         => status.toString
    }
    JsString(stringFormat)
  }

  implicit val listRuntimeResponseWriter: RootJsonWriter[ListRuntimeResponse] = (obj: ListRuntimeResponse) => {
    val allFields = Map(
      "id" -> obj.id.toJson,
      "internalId" -> obj.samResource.resourceId.toJson,
      "clusterName" -> obj.clusterName.asString.toJson,
      "googleId" -> obj.asyncRuntimeFields.map(_.googleId.value.toJson).getOrElse(JsNull),
      "googleProject" -> obj.googleProject.toJson,
      "googleServiceAccount" -> obj.serviceAccountInfo.toJson,
      "machineConfig" -> obj.machineConfig.toJson,
      "clusterUrl" -> obj.clusterUrl.toString.toJson,
      "operationName" -> obj.asyncRuntimeFields.map(_.operationName.value.toJson).getOrElse(JsNull),
      "status" -> obj.status.toJson,
      "hostIp" -> obj.asyncRuntimeFields.flatMap(_.hostIp.map(_.value.toJson)).getOrElse(JsNull),
      "creator" -> obj.auditInfo.creator.toJson,
      "createdDate" -> obj.auditInfo.createdDate.toJson,
      "destroyedDate" -> obj.auditInfo.destroyedDate.toJson,
      "kernelFoundBusyDate" -> obj.kernelFoundBusyDate.toJson,
      "labels" -> obj.labels.toJson,
      "jupyterUserScriptUri" -> obj.jupyterUserScriptUri.map(_.asString).toJson,
      "stagingBucket" -> obj.asyncRuntimeFields.map(_.stagingBucket.toJson).getOrElse(JsNull),
      "instances" -> obj.dataprocInstances.toJson,
      "dateAccessed" -> obj.auditInfo.dateAccessed.toJson,
      "autopauseThreshold" -> obj.autopauseThreshold.toJson,
      "defaultClientId" -> obj.defaultClientId.toJson,
      "stopAfterCreation" -> obj.stopAfterCreation.toJson,
      "welderEnabled" -> obj.welderEnabled.toJson,
      "patchInProgress" -> obj.patchInProgress.toJson,
      "scopes" -> List
        .empty[String]
        .toJson //TODO: stubbing this out temporarily until AOU move to new swagger generated client
    )

    val presentFields = allFields.filter(_._2 != JsNull)

    JsObject(presentFields)
  }

  implicit val RuntimeErrorFormat: RootJsonFormat[RuntimeError] = new RootJsonFormat[RuntimeError] {
    override def read(json: JsValue): RuntimeError =
      throw new NotImplementedError("decoding RuntimeError via spray-json is not implemented")

    override def write(obj: RuntimeError): JsValue = {
      val allFields = Map(
        "errorMessage" -> obj.errorMessage.toJson,
        "errorCode" -> obj.errorCode.toJson,
        "timestamp" -> obj.timestamp.toJson
      )

      val presentFields = allFields.filter(_._2 != JsNull)

      JsObject(presentFields)
    }
  }

  implicit val UserClusterExtensionConfigFormat = jsonFormat4(UserJupyterExtensionConfig.apply)

  implicit val RuntimeImageFormat: RootJsonFormat[RuntimeImage] = new RootJsonFormat[RuntimeImage] {
    override def read(json: JsValue): RuntimeImage =
      throw new NotImplementedError("decoding RuntimeError via spray-json is not implemented")

    override def write(obj: RuntimeImage): JsValue = {
      val allFields = Map(
        "imageType" -> obj.imageType.toString.toJson,
        "imageUrl" -> obj.imageUrl.toJson,
        "timestamp" -> obj.timestamp.toJson
      )

      val presentFields = allFields.filter(_._2 != JsNull)

      JsObject(presentFields)
    }
  }

  implicit val GetRuntimeFormat: RootJsonWriter[GetRuntimeResponse] = (obj: GetRuntimeResponse) => {
    val allFields = List(
      "id" -> obj.id.toJson,
      "internalId" -> obj.samResource.resourceId.toJson,
      "clusterName" -> obj.clusterName.asString.toJson,
      "googleId" -> obj.asyncRuntimeFields.map(_.googleId.value).toJson,
      "googleProject" -> obj.googleProject.toJson,
      "googleServiceAccount" -> obj.serviceAccountInfo.toJson,
      "machineConfig" -> obj.runtimeConfig.toJson,
      "clusterUrl" -> obj.clusterUrl.toString.toJson,
      "operationName" -> obj.asyncRuntimeFields.map(_.operationName.value).toJson,
      "status" -> obj.status.toJson,
      "hostIp" -> obj.asyncRuntimeFields.flatMap(_.hostIp.map(_.value.toJson)).getOrElse(JsNull),
      "creator" -> obj.auditInfo.creator.toJson,
      "createdDate" -> obj.auditInfo.createdDate.toJson,
      "destroyedDate" -> obj.auditInfo.destroyedDate.toJson,
      "kernelFoundBusyDate" -> obj.kernelFoundBusyDate.toJson,
      "labels" -> obj.labels.toJson,
      "jupyterExtensionUri" -> obj.userJupyterExtensionConfig
        .flatMap(_.nbExtensions.values.headOption)
        .getOrElse("")
        .toJson,
      "jupyterUserScriptUri" -> obj.jupyterUserScriptUri.map(_.asString).toJson,
      "jupyterStartUserScriptUri" -> obj.jupyterStartUserScriptUri.map(_.asString).toJson,
      "stagingBucket" -> obj.asyncRuntimeFields.map(_.stagingBucket).toJson,
      "errors" -> obj.errors.toJson,
      "instances" -> obj.dataprocInstances.toJson,
      "userJupyterExtensionConfig" -> obj.userJupyterExtensionConfig.toJson,
      "dateAccessed" -> obj.auditInfo.dateAccessed.toJson,
      "autopauseThreshold" -> obj.autopauseThreshold.toJson,
      "defaultClientId" -> obj.defaultClientId.toJson,
      "stopAfterCreation" -> obj.stopAfterCreation.toJson,
      "clusterImages" -> obj.clusterImages.toJson,
      "scopes" -> obj.scopes.toJson,
      "welderEnabled" -> obj.welderEnabled.toJson,
      "patchInProgress" -> obj.patchInProgress.toJson
    )

    val presentFields = allFields.filter(_._2 != JsNull)

    JsObject(presentFields: _*)
  }

  implicit val CreateRuntimeResponseFormat: RootJsonWriter[CreateRuntimeResponse] =
    (obj: CreateRuntimeResponse) => {
      val allFields = List(
        "id" -> obj.id.toJson,
        "internalId" -> obj.samResource.resourceId.toJson,
        "clusterName" -> obj.clusterName.asString.toJson,
        "googleId" -> obj.asyncRuntimeFields.map(_.googleId.value).toJson,
        "googleProject" -> obj.googleProject.toJson,
        "googleServiceAccount" -> obj.serviceAccountInfo.toJson,
        "machineConfig" -> obj.runtimeConfig.toJson,
        "clusterUrl" -> obj.clusterUrl.toString.toJson,
        "operationName" -> obj.asyncRuntimeFields.map(_.operationName.value).toJson,
        "status" -> obj.status.toJson,
        "hostIp" -> obj.asyncRuntimeFields.flatMap(_.hostIp.map(_.value)).toJson,
        "creator" -> obj.auditInfo.creator.toJson,
        "createdDate" -> obj.auditInfo.createdDate.toJson,
        "destroyedDate" -> obj.auditInfo.destroyedDate.toJson,
        "kernelFoundBusyDate" -> obj.kernelFoundBusyDate.toJson,
        "labels" -> obj.labels.toJson,
        "jupyterExtensionUri" -> obj.jupyterExtensionUri.toJson,
        "jupyterUserScriptUri" -> obj.jupyterUserScriptUri.map(_.asString).toJson,
        "jupyterStartUserScriptUri" -> obj.jupyterStartUserScriptUri.map(_.asString).toJson,
        "stagingBucket" -> obj.asyncRuntimeFields.map(_.stagingBucket).toJson,
        "errors" -> obj.errors.toJson,
        "instances" -> obj.dataprocInstances.toJson,
        "userJupyterExtensionConfig" -> obj.userJupyterExtensionConfig.toJson,
        "dateAccessed" -> obj.auditInfo.dateAccessed.toJson,
        "autopauseThreshold" -> obj.autopauseThreshold.toJson,
        "defaultClientId" -> obj.defaultClientId.toJson,
        "stopAfterCreation" -> obj.stopAfterCreation.toJson,
        "clusterImages" -> obj.clusterImages.toJson,
        "scopes" -> obj.scopes.toJson,
        "welderEnabled" -> obj.welderEnabled.toJson,
        "patchInProgress" -> obj.patchInProgress.toJson
      )

      val presentFields = allFields.filter(_._2 != JsNull)

      JsObject(presentFields: _*)

    }
}
