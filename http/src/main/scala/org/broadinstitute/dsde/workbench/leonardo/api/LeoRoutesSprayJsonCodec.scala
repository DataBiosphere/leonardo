package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import org.broadinstitute.dsde.workbench.leonardo.http.service.{
  CreateRuntimeAPIResponse,
  GetRuntimeResponse,
  ListRuntimeResponse,
  UpdateRuntimeResponse
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
          "diskSize" -> x.diskSize.toJson,
          "cloudService" -> x.cloudService.asString.toJson
        )
      case x: RuntimeConfig.DataprocConfig =>
        Map(
          "numberOfWorkers" -> x.numberOfWorkers.toJson,
          "masterMachineType" -> x.masterMachineType.value.toJson,
          "masterDiskSize" -> x.masterDiskSize.toJson,
          "workerMachineType" -> x.workerMachineType.map(_.value).toJson,
          "workerDiskSize" -> x.workerDiskSize.map(_.toJson).getOrElse(JsNull),
          "cloudService" -> x.cloudService.asString.toJson,
          "numberOfWorkerLocalSSDs" -> x.numberOfWorkerLocalSSDs.map(_.toJson).getOrElse(JsNull),
          "numberOfPreemptibleWorkers" -> x.numberOfPreemptibleWorkers.map(_.toJson).getOrElse(JsNull)
        )
    }

    val presentFields = allFields.filter(_._2 != JsNull)

    JsObject(presentFields)
  }

  implicit val serviceAccountInfoWriter: RootJsonWriter[ServiceAccountInfo] = (obj: ServiceAccountInfo) => {
    val allFields = Map(
      "clusterServiceAccount" -> obj.clusterServiceAccount.toJson,
      "notebookServiceAccount" -> obj.notebookServiceAccount.toJson
    )

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

  implicit val listRuntimeResponseWriter: RootJsonWriter[ListRuntimeResponse] = (obj: ListRuntimeResponse) => {
    val allFields = Map(
      "id" -> obj.id.toJson,
      "internalId" -> obj.internalId.asString.toJson,
      "clusterName" -> obj.clusterName.asString.toJson,
      "googleId" -> obj.asyncRuntimeFields.map(_.googleId.toString.toJson).getOrElse(JsNull),
      "googleProject" -> obj.googleProject.toJson,
      "serviceAccountInfo" -> obj.serviceAccountInfo.toJson,
      "machineConfig" -> obj.machineConfig.toJson,
      "clusterUrl" -> obj.clusterUrl.toString.toJson,
      "operationName" -> obj.asyncRuntimeFields.map(_.operationName.value.toJson).getOrElse(JsNull),
      "status" -> obj.status.toString.toJson,
      "hostIp" -> obj.asyncRuntimeFields.flatMap(_.hostIp.map(_.value.toJson)).getOrElse(JsNull),
      "creator" -> obj.auditInfo.creator.toJson,
      "createdDate" -> obj.auditInfo.createdDate.toJson,
      "destroyedDate" -> obj.auditInfo.destroyedDate.toJson,
      "kernelFoundBusyDate" -> obj.auditInfo.kernelFoundBusyDate.toJson,
      "labels" -> obj.labels.toJson,
      "jupyterExtensionUri" -> obj.jupyterExtensionUri.toJson,
      "jupyterUserScriptUri" -> obj.jupyterUserScriptUri.map(_.asString).toJson,
      "stagingBucket" -> obj.asyncRuntimeFields.map(_.stagingBucket.toJson).getOrElse(JsNull),
      "instances" -> obj.dataprocInstances.toJson,
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

  implicit val RuntimeErrorFormat: RootJsonFormat[RuntimeError] = new RootJsonFormat[RuntimeError] {
    override def read(json: JsValue): RuntimeError =
      throw new NotImplementedError("decode RuntimeError not implemented")

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

  // TODO
  implicit val UserJupyterExtensionConfigFormat: RootJsonFormat[UserJupyterExtensionConfig] =
    new RootJsonFormat[UserJupyterExtensionConfig] {
      override def read(json: JsValue): UserJupyterExtensionConfig = ???

      override def write(obj: UserJupyterExtensionConfig): JsValue = ???
    }

  // TODO
  implicit val RuntimeImageFormat: RootJsonFormat[RuntimeImage] = new RootJsonFormat[ClusterImage] {
    override def read(json: JsValue): ClusterImage = ???

    override def write(obj: ClusterImage): JsValue = ???
  }

  implicit val GetRuntimeFormat: RootJsonWriter[GetRuntimeResponse] = (obj: GetRuntimeResponse) => {
    val allFields = List(
      "id" -> obj.id.toJson,
      "internalId" -> obj.internalId.asString.toJson,
      "clusterName" -> obj.clusterName.asString.toJson,
      "googleId" -> obj.asyncRuntimeFields.map(_.googleId.toString).toJson,
      "googleProject" -> obj.googleProject.toJson,
      "serviceAccountInfo" -> obj.serviceAccountInfo.toJson,
      "machineConfig" -> obj.runtimeConfig.toJson,
      "clusterUrl" -> obj.clusterUrl.toString.toJson,
      "operationName" -> obj.asyncRuntimeFields.map(_.operationName.value).toJson,
      "status" -> obj.status.toString.toJson,
      "hostIp" -> obj.asyncRuntimeFields.flatMap(_.hostIp.map(_.value.toJson)).getOrElse(JsNull),
      "creator" -> obj.auditInfo.creator.toJson,
      "createdDate" -> obj.auditInfo.createdDate.toJson,
      "destroyedDate" -> obj.auditInfo.destroyedDate.toJson,
      "kernelFoundBusyDate" -> obj.auditInfo.kernelFoundBusyDate.toJson,
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
      "welderEnabled" -> obj.welderEnabled.toJson
    )

    val presentFields = allFields.filter(_._2 != JsNull)

    JsObject(presentFields: _*)
  }

  implicit val CreateRuntimeAPIResponseFormat: RootJsonWriter[CreateRuntimeAPIResponse] =
    (obj: CreateRuntimeAPIResponse) => {
      val allFields = List(
        "id" -> obj.id.toJson,
        "internalId" -> obj.internalId.asString.toJson,
        "clusterName" -> obj.clusterName.asString.toJson,
        "googleId" -> obj.asyncRuntimeFields.map(_.googleId.toString).toJson,
        "googleProject" -> obj.googleProject.toJson,
        "serviceAccountInfo" -> obj.serviceAccountInfo.toJson,
        "machineConfig" -> obj.runtimeConfig.toJson,
        "clusterUrl" -> obj.clusterUrl.toString.toJson,
        "operationName" -> obj.asyncRuntimeFields.map(_.operationName.value).toJson,
        "status" -> obj.status.toString.toJson,
        "hostIp" -> obj.asyncRuntimeFields.flatMap(_.hostIp.map(_.value)).toJson,
        "creator" -> obj.auditInfo.creator.toJson,
        "createdDate" -> obj.auditInfo.createdDate.toJson,
        "destroyedDate" -> obj.auditInfo.destroyedDate.toJson,
        "kernelFoundBusyDate" -> obj.auditInfo.kernelFoundBusyDate.toJson,
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
        "welderEnabled" -> obj.welderEnabled.toJson
      )

      val presentFields = allFields.filter(_._2 != JsNull)

      JsObject(presentFields: _*)

    }

  implicit val UpdateRuntimeResponseFormat: RootJsonWriter[UpdateRuntimeResponse] = (obj: UpdateRuntimeResponse) => {
    val allFields = List(
      "id" -> obj.id.toJson,
      "internalId" -> obj.internalId.asString.toJson,
      "clusterName" -> obj.clusterName.asString.toJson,
      "googleId" -> obj.asyncRuntimeFields.map(_.googleId.toString).toJson,
      "googleProject" -> obj.googleProject.toJson,
      "serviceAccountInfo" -> obj.serviceAccountInfo.toJson,
      "machineConfig" -> obj.runtimeConfig.toJson, //Note, for this response, we're still encoding runtimeConfig as machineConfig
      "clusterUrl" -> obj.clusterUrl.toString.toJson,
      "operationName" -> obj.asyncRuntimeFields.map(_.operationName.value).toJson,
      "status" -> obj.status.toString.toJson,
      "hostIp" -> obj.asyncRuntimeFields.flatMap(_.hostIp.map(_.value)).toJson,
      "creator" -> obj.auditInfo.creator.toJson,
      "createdDate" -> obj.auditInfo.createdDate.toJson,
      "destroyedDate" -> obj.auditInfo.destroyedDate.toJson,
      "kernelFoundBusyDate" -> obj.auditInfo.kernelFoundBusyDate.toJson,
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
      "welderEnabled" -> obj.welderEnabled.toJson
    )

    val presentFields = allFields.filter(_._2 != JsNull)

    JsObject(presentFields: _*)
  }
}
