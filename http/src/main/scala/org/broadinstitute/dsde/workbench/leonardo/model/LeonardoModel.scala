package org.broadinstitute.dsde.workbench.leonardo
package model

import java.net.URL
import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import ca.mrvisser.sealerate
import cats.Semigroup
import cats.implicits._
import enumeratum.{Enum, EnumEntry}
import org.broadinstitute.dsde.workbench.leonardo.ClusterImageType._
import org.broadinstitute.dsde.workbench.leonardo.config._
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterContainerServiceType.JupyterService
import org.broadinstitute.dsde.workbench.leonardo.model.google.DataprocRole.SecondaryWorker
import org.broadinstitute.dsde.workbench.leonardo.model.google.GoogleJsonSupport._
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.leonardo.http.service.ListClusterResponse
import org.broadinstitute.dsde.workbench.model.WorkbenchIdentityJsonSupport._
import org.broadinstitute.dsde.workbench.model._
import org.broadinstitute.dsde.workbench.model.google.GoogleModelJsonSupport.{GcsPathFormat => _, _}
import org.broadinstitute.dsde.workbench.model.google._
import spray.json._

// Create cluster API request
final case class ClusterRequest(labels: LabelMap = Map.empty,
                                jupyterExtensionUri: Option[GcsPath] = None,
                                jupyterUserScriptUri: Option[UserScriptPath] = None,
                                jupyterStartUserScriptUri: Option[UserScriptPath] = None,
                                machineConfig: Option[MachineConfig] = None,
                                properties: Map[String, String] = Map.empty,
                                stopAfterCreation: Option[Boolean] = None,
                                userJupyterExtensionConfig: Option[UserJupyterExtensionConfig] = None,
                                autopause: Option[Boolean] = None,
                                autopauseThreshold: Option[Int] = None,
                                defaultClientId: Option[String] = None,
                                jupyterDockerImage: Option[ContainerImage] = None,
                                toolDockerImage: Option[ContainerImage] = None,
                                welderDockerImage: Option[ContainerImage] = None,
                                scopes: Set[String] = Set.empty,
                                enableWelder: Option[Boolean] = None,
                                customClusterEnvironmentVariables: Map[String, String] = Map.empty)

// A resource that is required by a cluster
case class ClusterResource(value: String) extends ValueObject

case class DataprocInfo(googleId: UUID, operationName: OperationName, stagingBucket: GcsBucketName, hostIp: Option[IP])

case class AuditInfo(creator: WorkbenchEmail,
                     createdDate: Instant,
                     destroyedDate: Option[Instant],
                     dateAccessed: Instant,
                     kernelFoundBusyDate: Option[Instant])

sealed trait ClusterContainerServiceType extends EnumEntry with Serializable with Product {
  def imageType: ClusterImageType
  def proxySegment: String
}
object ClusterContainerServiceType extends Enum[ClusterContainerServiceType] {
  val values = findValues
  val imageTypeToClusterContainerServiceType: Map[ClusterImageType, ClusterContainerServiceType] =
    values.toList.map(v => v.imageType -> v).toMap
  case object JupyterService extends ClusterContainerServiceType {
    override def imageType: ClusterImageType = Jupyter
    override def proxySegment: String = "jupyter"
  }
  case object RStudioService extends ClusterContainerServiceType {
    override def imageType: ClusterImageType = RStudio
    override def proxySegment: String = "rstudio"
  }
  case object WelderService extends ClusterContainerServiceType {
    override def imageType: ClusterImageType = Welder
    override def proxySegment: String = "welder"
  }
}

case class ClusterImage(imageType: ClusterImageType, imageUrl: String, timestamp: Instant)

case class ClusterInternalId(value: String) extends ValueObject

final case class RunningCluster(googleProject: GoogleProject,
                                clusterName: ClusterName,
                                containers: List[ClusterContainerServiceType])

// The cluster itself
final case class Cluster(id: Long = 0, // DB AutoInc
                         internalId: ClusterInternalId,
                         clusterName: ClusterName,
                         googleProject: GoogleProject,
                         serviceAccountInfo: ServiceAccountInfo,
                         dataprocInfo: Option[DataprocInfo],
                         auditInfo: AuditInfo,
                         machineConfig: MachineConfig,
                         properties: Map[String, String],
                         clusterUrl: URL,
                         status: ClusterStatus,
                         labels: LabelMap,
                         jupyterExtensionUri: Option[GcsPath],
                         jupyterUserScriptUri: Option[UserScriptPath],
                         jupyterStartUserScriptUri: Option[UserScriptPath],
                         errors: List[ClusterError],
                         instances: Set[Instance],
                         userJupyterExtensionConfig: Option[UserJupyterExtensionConfig],
                         autopauseThreshold: Int,
                         defaultClientId: Option[String],
                         stopAfterCreation: Boolean,
                         clusterImages: Set[ClusterImage],
                         scopes: Set[String],
                         welderEnabled: Boolean,
                         customClusterEnvironmentVariables: Map[String, String]) {
  def projectNameString: String = s"${googleProject.value}/${clusterName.value}"
  def nonPreemptibleInstances: Set[Instance] = instances.filterNot(_.dataprocRole.contains(SecondaryWorker))
  def toListClusterResp: ListClusterResponse =
    ListClusterResponse(
      id,
      internalId,
      clusterName,
      googleProject,
      serviceAccountInfo,
      dataprocInfo,
      auditInfo,
      machineConfig,
      clusterUrl,
      status,
      labels,
      jupyterExtensionUri,
      jupyterUserScriptUri,
      instances,
      autopauseThreshold,
      defaultClientId,
      stopAfterCreation,
      welderEnabled
    )
}

object Cluster {
  def create(clusterRequest: ClusterRequest,
             internalId: ClusterInternalId,
             userEmail: WorkbenchEmail,
             clusterName: ClusterName,
             googleProject: GoogleProject,
             serviceAccountInfo: ServiceAccountInfo,
             machineConfig: MachineConfig,
             clusterUrlBase: String,
             autopauseThreshold: Int,
             clusterScopes: Set[String],
             clusterImages: Set[ClusterImage]): Cluster =
    Cluster(
      internalId = internalId,
      clusterName = clusterName,
      googleProject = googleProject,
      serviceAccountInfo = serviceAccountInfo,
      dataprocInfo = None,
      auditInfo = AuditInfo(userEmail, Instant.now(), None, Instant.now(), None),
      machineConfig = machineConfig,
      properties = clusterRequest.properties,
      clusterUrl = getClusterUrl(googleProject, clusterName, clusterImages, clusterRequest.labels),
      status = ClusterStatus.Creating,
      labels = clusterRequest.labels,
      jupyterExtensionUri = clusterRequest.jupyterExtensionUri,
      jupyterUserScriptUri = clusterRequest.jupyterUserScriptUri,
      jupyterStartUserScriptUri = clusterRequest.jupyterStartUserScriptUri,
      errors = List.empty,
      instances = Set.empty,
      userJupyterExtensionConfig = clusterRequest.userJupyterExtensionConfig,
      autopauseThreshold = autopauseThreshold,
      defaultClientId = clusterRequest.defaultClientId,
      stopAfterCreation = clusterRequest.stopAfterCreation.getOrElse(false),
      clusterImages = clusterImages,
      scopes = clusterScopes,
      welderEnabled = clusterRequest.enableWelder.getOrElse(false),
      customClusterEnvironmentVariables = clusterRequest.customClusterEnvironmentVariables
    )

  def addDataprocFields(cluster: Cluster, operation: Operation, stagingBucket: GcsBucketName): Cluster =
    cluster.copy(
      dataprocInfo = Some(DataprocInfo(operation.uuid, operation.name, stagingBucket, None))
    )

  def getClusterUrl(googleProject: GoogleProject,
                    clusterName: ClusterName,
                    clusterImages: Set[ClusterImage],
                    labels: Map[String, String]): URL = {
    val tool = clusterImages
      .map(_.imageType)
      .filterNot(Set(Welder, CustomDataProc).contains)
      .headOption
      .orElse(labels.get("tool").flatMap(ClusterImageType.withNameInsensitiveOption))
      .flatMap(ClusterContainerServiceType.imageTypeToClusterContainerServiceType.get)
      .headOption
      .getOrElse(JupyterService)

    new URL(
      Config.dataprocConfig.clusterUrlBase + googleProject.value + "/" + clusterName.value + "/" + tool.proxySegment
    )
  }
}

// Default cluster labels
case class DefaultLabels(clusterName: ClusterName,
                         googleProject: GoogleProject,
                         creator: WorkbenchEmail,
                         clusterServiceAccount: Option[WorkbenchEmail],
                         notebookServiceAccount: Option[WorkbenchEmail],
                         notebookUserScript: Option[UserScriptPath],
                         notebookStartUserScript: Option[UserScriptPath],
                         tool: Option[ClusterImageType])

// Provides ways of combining MachineConfigs with Leo defaults
object MachineConfigOps {
  case object NegativeIntegerArgumentInClusterRequestException
      extends LeoException(
        s"Your cluster request should not have negative integer values. Please revise your request and submit again.",
        StatusCodes.BadRequest
      )

  implicit private val machineConfigSemigroup = new Semigroup[MachineConfig] {
    def combine(defined: MachineConfig, default: MachineConfig): MachineConfig = {
      val minimumDiskSize = 10
      val masterDiskSize = math.max(minimumDiskSize, defined.masterDiskSize)
      defined.numberOfWorkers match {
        case 0 =>
          defined.copy(
            masterDiskSize = masterDiskSize
          )
        case numWorkers =>
          val workerDiskSize = defined.workerDiskSize.orElse(default.workerDiskSize)
          MachineConfig(
            numWorkers,
            defined.masterMachineType,
            masterDiskSize,
            defined.workerMachineType.orElse(default.workerMachineType),
            workerDiskSize.map(s => math.max(minimumDiskSize, s)),
            defined.numberOfWorkerLocalSSDs.orElse(default.numberOfWorkerLocalSSDs),
            defined.numberOfPreemptibleWorkers.orElse(default.numberOfPreemptibleWorkers)
          )
      }
    }
  }

  def create(definedMachineConfig: Option[MachineConfig], defaultMachineConfig: ClusterDefaultsConfig): MachineConfig = {
    val defeaultMachineConfig = MachineConfigOps.createFromDefaults(defaultMachineConfig)
    definedMachineConfig.getOrElse(defeaultMachineConfig) |+| defeaultMachineConfig
  }

  def createFromDefaults(clusterDefaultsConfig: ClusterDefaultsConfig): MachineConfig = MachineConfig(
    clusterDefaultsConfig.numberOfWorkers,
    clusterDefaultsConfig.masterMachineType,
    clusterDefaultsConfig.masterDiskSize,
    Some(clusterDefaultsConfig.workerMachineType),
    Some(clusterDefaultsConfig.workerDiskSize),
    Some(clusterDefaultsConfig.numberOfWorkerLocalSSDs),
    Some(clusterDefaultsConfig.numberOfPreemptibleWorkers)
  )
}

// Fields that must be templated into cluster resources (e.g. the init script).
// see https://broadinstitute.atlassian.net/browse/GAWB-2619 for why these are Strings rather than value classes
final case class ClusterTemplateValues private (googleProject: String,
                                                clusterName: String,
                                                stagingBucketName: String,
                                                jupyterDockerImage: String,
                                                rstudioDockerImage: String,
                                                proxyDockerImage: String,
                                                welderDockerImage: String,
                                                jupyterServerCrt: String,
                                                jupyterServerKey: String,
                                                rootCaPem: String,
                                                jupyterDockerCompose: String,
                                                rstudioDockerCompose: String,
                                                proxyDockerCompose: String,
                                                welderDockerCompose: String,
                                                proxySiteConf: String,
                                                jupyterServerName: String,
                                                rstudioServerName: String,
                                                welderServerName: String,
                                                proxyServerName: String,
                                                jupyterUserScriptUri: String,
                                                jupyterUserScriptOutputUri: String,
                                                jupyterStartUserScriptUri: String,
                                                jupyterStartUserScriptOutputBaseUri: String,
                                                jupyterServiceAccountCredentials: String,
                                                loginHint: String,
                                                jupyterServerExtensions: String,
                                                jupyterNbExtensions: String,
                                                jupyterCombinedExtensions: String,
                                                jupyterLabExtensions: String,
                                                jupyterNotebookConfigUri: String,
                                                jupyterNotebookFrontendConfigUri: String,
                                                googleClientId: String,
                                                welderEnabled: String,
                                                notebooksDir: String,
                                                customEnvVarsConfigUri: String,
                                                memLimit: String) {

  def toMap: Map[String, String] =
    this.getClass.getDeclaredFields.map(_.getName).zip(this.productIterator.to).toMap.mapValues(_.toString)
}

object ClusterTemplateValues {
  val serviceAccountCredentialsFilename = "service-account-credentials.json"
  val customEnvVarFilename = "custom_env_vars.env"

  def apply(cluster: Cluster,
            initBucketName: Option[GcsBucketName],
            stagingBucketName: Option[GcsBucketName],
            serviceAccountKey: Option[ServiceAccountKey],
            dataprocConfig: DataprocConfig,
            welderConfig: WelderConfig,
            proxyConfig: ProxyConfig,
            clusterFilesConfig: ClusterFilesConfig,
            clusterResourcesConfig: ClusterResourcesConfig,
            clusterResourceConstraints: Option[ClusterResourceConstraints]): ClusterTemplateValues =
    ClusterTemplateValues(
      cluster.googleProject.value,
      cluster.clusterName.value,
      stagingBucketName.map(_.value).getOrElse(""),
      cluster.clusterImages.find(_.imageType == Jupyter).map(_.imageUrl).getOrElse(""),
      cluster.clusterImages.find(_.imageType == RStudio).map(_.imageUrl).getOrElse(""),
      proxyConfig.jupyterProxyDockerImage,
      cluster.clusterImages.find(_.imageType == Welder).map(_.imageUrl).getOrElse(""),
      initBucketName
        .map(n => GcsPath(n, GcsObjectName(clusterFilesConfig.jupyterServerCrt.getName)).toUri)
        .getOrElse(""),
      initBucketName
        .map(n => GcsPath(n, GcsObjectName(clusterFilesConfig.jupyterServerKey.getName)).toUri)
        .getOrElse(""),
      initBucketName
        .map(n => GcsPath(n, GcsObjectName(clusterFilesConfig.jupyterRootCaPem.getName)).toUri)
        .getOrElse(""),
      initBucketName
        .map(n => GcsPath(n, GcsObjectName(clusterResourcesConfig.jupyterDockerCompose.value)).toUri)
        .getOrElse(""),
      initBucketName
        .map(n => GcsPath(n, GcsObjectName(clusterResourcesConfig.rstudioDockerCompose.value)).toUri)
        .getOrElse(""),
      initBucketName
        .map(n => GcsPath(n, GcsObjectName(clusterResourcesConfig.proxyDockerCompose.value)).toUri)
        .getOrElse(""),
      initBucketName
        .map(n => GcsPath(n, GcsObjectName(clusterResourcesConfig.welderDockerCompose.value)).toUri)
        .getOrElse(""),
      initBucketName
        .map(n => GcsPath(n, GcsObjectName(clusterResourcesConfig.proxySiteConf.value)).toUri)
        .getOrElse(""),
      dataprocConfig.jupyterServerName,
      dataprocConfig.rstudioServerName,
      dataprocConfig.welderServerName,
      proxyConfig.proxyServerName,
      cluster.jupyterUserScriptUri.map(_.asString).getOrElse(""),
      stagingBucketName.map(n => GcsPath(n, GcsObjectName("userscript_output.txt")).toUri).getOrElse(""),
      cluster.jupyterStartUserScriptUri.map(_.asString).getOrElse(""),
      stagingBucketName.map(n => GcsPath(n, GcsObjectName("startscript_output.txt")).toUri).getOrElse(""),
      (for {
        _ <- serviceAccountKey
        n <- initBucketName
      } yield GcsPath(n, GcsObjectName(serviceAccountCredentialsFilename)).toUri).getOrElse(""),
      cluster.auditInfo.creator.value,
      cluster.userJupyterExtensionConfig.map(x => x.serverExtensions.values.mkString(" ")).getOrElse(""),
      cluster.userJupyterExtensionConfig.map(x => x.nbExtensions.values.mkString(" ")).getOrElse(""),
      cluster.userJupyterExtensionConfig.map(x => x.combinedExtensions.values.mkString(" ")).getOrElse(""),
      cluster.userJupyterExtensionConfig.map(x => x.labExtensions.values.mkString(" ")).getOrElse(""),
      initBucketName
        .map(n => GcsPath(n, GcsObjectName(clusterResourcesConfig.jupyterNotebookConfigUri.value)).toUri)
        .getOrElse(""),
      initBucketName
        .map(n => GcsPath(n, GcsObjectName(clusterResourcesConfig.jupyterNotebookFrontendConfigUri.value)).toUri)
        .getOrElse(""),
      cluster.defaultClientId.getOrElse(""),
      cluster.welderEnabled.toString, // TODO: remove this and conditional below when welder is rolled out to all clusters
      if (cluster.welderEnabled) welderConfig.welderEnabledNotebooksDir.toString
      else welderConfig.welderDisabledNotebooksDir.toString,
      initBucketName
        .map(n => GcsPath(n, GcsObjectName(clusterResourcesConfig.customEnvVarsConfigUri.value)).toUri)
        .getOrElse(""),
      clusterResourceConstraints.map(_.memoryLimit.toString).getOrElse("")
    )
}

sealed abstract class PropertyFilePrefix
object PropertyFilePrefix {
  case object CapacityScheduler extends PropertyFilePrefix {
    override def toString: String = "capacity-scheduler"
  }
  case object Core extends PropertyFilePrefix {
    override def toString: String = "core"
  }
  case object Distcp extends PropertyFilePrefix {
    override def toString: String = "distcp"
  }
  case object HadoopEnv extends PropertyFilePrefix {
    override def toString: String = "hadoop-env"
  }
  case object Hdfs extends PropertyFilePrefix {
    override def toString: String = "hdfs"
  }
  case object Hive extends PropertyFilePrefix {
    override def toString: String = "hive"
  }
  case object Mapred extends PropertyFilePrefix {
    override def toString: String = "mapred"
  }
  case object MapredEnv extends PropertyFilePrefix {
    override def toString: String = "mapred-env"
  }
  case object Pig extends PropertyFilePrefix {
    override def toString: String = "pig"
  }
  case object Presto extends PropertyFilePrefix {
    override def toString: String = "presto"
  }
  case object PrestoJvm extends PropertyFilePrefix {
    override def toString: String = "presto-jvm"
  }
  case object Spark extends PropertyFilePrefix {
    override def toString: String = "spark"
  }
  case object SparkEnv extends PropertyFilePrefix {
    override def toString: String = "spark-env"
  }
  case object Yarn extends PropertyFilePrefix {
    override def toString: String = "yarn"
  }
  case object YarnEnv extends PropertyFilePrefix {
    override def toString: String = "yarn-env"
  }
  case object Zeppelin extends PropertyFilePrefix {
    override def toString: String = "zeppelin"
  }
  case object ZeppelinEnv extends PropertyFilePrefix {
    override def toString: String = "zeppelin-env"
  }
  case object Zookeeper extends PropertyFilePrefix {
    override def toString: String = "zookeeper"
  }
  case object Dataproc extends PropertyFilePrefix {
    override def toString: String = "dataproc"
  }

  def values: Set[PropertyFilePrefix] = sealerate.values[PropertyFilePrefix]

  def stringToObject: Map[String, PropertyFilePrefix] = values.map(v => v.toString -> v).toMap
}

sealed trait ExtensionType extends EnumEntry
object ExtensionType extends Enum[ExtensionType] {
  val values = findValues

  case object NBExtension extends ExtensionType
  case object ServerExtension extends ExtensionType
  case object CombinedExtension extends ExtensionType
  case object LabExtension extends ExtensionType
}

sealed trait WelderAction extends EnumEntry
object WelderAction extends Enum[WelderAction] {
  val values = findValues

  case object DeployWelder extends WelderAction
  case object UpdateWelder extends WelderAction
  case object NoAction extends WelderAction
  case object ClusterOutOfDate extends WelderAction
  case object DisableDelocalization extends WelderAction
}

final case class MemorySize(bytes: Long) extends AnyVal {
  override def toString: String = bytes.toString + "b"
}
object MemorySize {
  def fromKb(kb: Double): MemorySize = MemorySize((kb * 1024).toLong)
  def fromMb(mb: Double): MemorySize = MemorySize((mb * 1048576).toLong)
  def fromGb(gb: Double): MemorySize = MemorySize((gb * 1073741824).toLong)
}

// See https://docs.docker.com/compose/compose-file/compose-file-v2/#cpu-and-other-resources
// for other types of resources we may want to add here.
final case class ClusterResourceConstraints(memoryLimit: MemorySize)

object LeonardoJsonSupport extends DefaultJsonProtocol {
  implicit object URLFormat extends JsonFormat[URL] {
    def write(obj: URL) = JsString(obj.toString)

    def read(json: JsValue): URL = json match {
      case JsString(url) => new URL(url)
      case other         => throw DeserializationException("Expected URL, got: " + other)
    }
  }

  // the one from workbench-libs is ignored from import because we need a different encoding/decoding
  implicit val gcsPathFormat: JsonFormat[GcsPath] = new JsonFormat[GcsPath] {
    def write(obj: GcsPath) = JsString(obj.toUri)

    def read(json: JsValue): GcsPath = json match {
      case JsString(uri) =>
        parseGcsPath(uri).getOrElse(throw DeserializationException(s"Could not parse bucket URI from: $uri"))
      case other => throw DeserializationException(s"Expected bucket URI, got: $other")
    }
  }

  implicit object JupyterDockerImageJsonFormat extends JsonFormat[ContainerImage] {
    def read(json: JsValue): ContainerImage = json match {
      case JsString(imageUrl) =>
        ContainerImage
          .stringToJupyterDockerImage(imageUrl)
          .getOrElse(
            throw DeserializationException(
              s"Invalid docker registry. Only ${ContainerRegistry.allRegistries.mkString(", ")} are supported"
            )
          )
      case other => throw DeserializationException(s"Expected custom docker image URL, got: $other")
    }
    override def write(obj: ContainerImage): JsValue = JsString(obj.imageUrl)
  }

  implicit object UserScriptPathJsonFormat extends JsonFormat[UserScriptPath] {
    def read(json: JsValue): UserScriptPath = json match {
      case JsString(path) =>
        UserScriptPath
          .stringToUserScriptPath(path)
          .fold(e => throw DeserializationException(s"Invalid userscript path: ${e}"), identity)
      case other => throw DeserializationException(s"Expected userscript path, got: $other")
    }
    def write(obj: UserScriptPath): JsValue = JsString(obj.asString)
  }

  implicit val UserClusterExtensionConfigFormat = jsonFormat4(UserJupyterExtensionConfig.apply)

  implicit val ClusterResourceFormat = ValueObjectFormat(ClusterResource)

  implicit val ServiceAccountInfoFormat = jsonFormat2(ServiceAccountInfo)

  implicit val ClusterErrorFormat = jsonFormat3(ClusterError.apply)

  implicit val ClusterToolFormat = EnumEntryFormat(ClusterImageType.withName)

  implicit val DefaultLabelsFormat = jsonFormat8(DefaultLabels.apply)

  implicit val ClusterImageFormat = jsonFormat3(ClusterImage.apply)

  implicit val ClusterInternalIdFormat = ValueObjectFormat(ClusterInternalId)

  implicit object ClusterFormat extends RootJsonWriter[Cluster] {
    override def write(obj: Cluster): JsValue = {
      val allFields = List(
        "id" -> obj.id.toJson,
        "internalId" -> obj.internalId.toJson,
        "clusterName" -> obj.clusterName.toJson,
        "googleId" -> obj.dataprocInfo.map(_.googleId).toJson,
        "googleProject" -> obj.googleProject.toJson,
        "serviceAccountInfo" -> obj.serviceAccountInfo.toJson,
        "machineConfig" -> obj.machineConfig.toJson,
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
