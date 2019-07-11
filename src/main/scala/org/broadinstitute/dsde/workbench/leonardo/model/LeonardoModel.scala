package org.broadinstitute.dsde.workbench.leonardo.model

import java.net.URL
import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes
import cats.Semigroup
import cats.implicits._
import com.typesafe.config.ConfigFactory
import enumeratum.{Enum, EnumEntry}
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.config.{ClusterDefaultsConfig, ClusterFilesConfig, ClusterResourcesConfig, DataprocConfig, ProxyConfig}
import org.broadinstitute.dsde.workbench.leonardo.model.Cluster._
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterTool.{Jupyter, RStudio, Welder}
import org.broadinstitute.dsde.workbench.leonardo.model.google.DataprocRole.SecondaryWorker
import org.broadinstitute.dsde.workbench.leonardo.model.google.GoogleJsonSupport._
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.model.WorkbenchIdentityJsonSupport._
import org.broadinstitute.dsde.workbench.model.google.GoogleModelJsonSupport._
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.model._
import spray.json.{RootJsonFormat, RootJsonReader, _}
import ca.mrvisser.sealerate

// Create cluster API request
final case class ClusterRequest(labels: LabelMap,
                          jupyterExtensionUri: Option[GcsPath] = None,
                          jupyterUserScriptUri: Option[GcsPath] = None,
                          machineConfig: Option[MachineConfig] = None,
                          properties: Map[String, String],
                          stopAfterCreation: Option[Boolean] = None,
                          userJupyterExtensionConfig: Option[UserJupyterExtensionConfig] = None,
                          autopause: Option[Boolean] = None,
                          autopauseThreshold: Option[Int] = None,
                          defaultClientId: Option[String] = None,
                          jupyterDockerImage: Option[String] = None,
                          rstudioDockerImage: Option[String] = None,
                          welderDockerImage: Option[String] = None,
                          scopes: Set[String] = Set.empty,
                          enableWelder: Option[Boolean] = None)


case class UserJupyterExtensionConfig(nbExtensions: Map[String, String] = Map(),
                                      serverExtensions: Map[String, String] = Map(),
                                      combinedExtensions: Map[String, String] = Map(),
                                      labExtensions: Map[String, String] = Map()) {

  def asLabels: LabelMap = {
    nbExtensions ++ serverExtensions ++ combinedExtensions ++ labExtensions
  }
}


// A resource that is required by a cluster
case class ClusterResource(value: String) extends ValueObject

// Information about service accounts used by the cluster
case class ServiceAccountInfo(clusterServiceAccount: Option[WorkbenchEmail],
                              notebookServiceAccount: Option[WorkbenchEmail])

case class ClusterError(errorMessage: String,
                        errorCode: Int,
                        timestamp: Instant)

case class DataprocInfo(googleId: Option[UUID],
                        operationName: Option[OperationName],
                        stagingBucket: Option[GcsBucketName],
                        hostIp: Option[IP])

case class AuditInfo(creator: WorkbenchEmail,
                     createdDate: Instant,
                     destroyedDate: Option[Instant],
                     dateAccessed: Instant,
                     kernelFoundBusyDate: Option[Instant])

sealed trait ClusterTool extends EnumEntry with Serializable with Product
object ClusterTool extends Enum[ClusterTool] {
  val values = findValues
  case object Jupyter extends ClusterTool
  case object RStudio extends ClusterTool
  case object Welder extends ClusterTool
}
case class ClusterImage(tool: ClusterTool,
                        dockerImage: String,
                        timestamp: Instant)

// The cluster itself
// Also the API response for "list clusters" and "get active cluster"
final case class Cluster(id: Long = 0, // DB AutoInc
                   clusterName: ClusterName,
                   googleProject: GoogleProject,
                   serviceAccountInfo: ServiceAccountInfo,
                   dataprocInfo: DataprocInfo,
                   auditInfo: AuditInfo,
                   machineConfig: MachineConfig,
                   properties: Map[String, String],
                   clusterUrl: URL,
                   status: ClusterStatus,
                   labels: LabelMap,
                   jupyterExtensionUri: Option[GcsPath],
                   jupyterUserScriptUri: Option[GcsPath],
                   errors: List[ClusterError],
                   instances: Set[Instance],
                   userJupyterExtensionConfig: Option[UserJupyterExtensionConfig],
                   autopauseThreshold: Int,
                   defaultClientId: Option[String],
                   stopAfterCreation: Boolean,
                   clusterImages: Set[ClusterImage],
                   scopes: Set[String],
                   welderEnabled: Boolean) {
  def projectNameString: String = s"${googleProject.value}/${clusterName.value}"
  def nonPreemptibleInstances: Set[Instance] = instances.filterNot(_.dataprocRole.contains(SecondaryWorker))
}
object Cluster {
  type LabelMap = Map[String, String]

  def create(clusterRequest: ClusterRequest,
             userEmail: WorkbenchEmail,
             clusterName: ClusterName,
             googleProject: GoogleProject,
             serviceAccountInfo: ServiceAccountInfo,
             machineConfig: MachineConfig,
             clusterUrlBase: String,
             autopauseThreshold: Int,
             clusterScopes: Set[String],
             operation: Option[Operation] = None,
             stagingBucket: Option[GcsBucketName] = None,
             clusterImages: Set[ClusterImage] = Set.empty): Cluster = {
    Cluster(
      clusterName = clusterName,
      googleProject = googleProject,
      serviceAccountInfo = serviceAccountInfo,
      dataprocInfo = DataprocInfo(operation.map(_.uuid), operation.map(_.name), stagingBucket, None),
      auditInfo = AuditInfo(userEmail, Instant.now(), None, Instant.now(), None),
      machineConfig = machineConfig,
      properties = clusterRequest.properties,
      clusterUrl = getClusterUrl(googleProject, clusterName, clusterUrlBase),
      status = ClusterStatus.Creating,
      labels = clusterRequest.labels,
      jupyterExtensionUri = clusterRequest.jupyterExtensionUri,
      jupyterUserScriptUri = clusterRequest.jupyterUserScriptUri,
      errors = List.empty,
      instances = Set.empty,
      userJupyterExtensionConfig = clusterRequest.userJupyterExtensionConfig,
      autopauseThreshold = autopauseThreshold,
      defaultClientId = clusterRequest.defaultClientId,
      stopAfterCreation = clusterRequest.stopAfterCreation.getOrElse(false),
      clusterImages = clusterImages,
      scopes = clusterScopes,
      welderEnabled = clusterRequest.enableWelder.getOrElse(false))
  }

  // TODO it's hacky to re-parse the Leo config in the model object.
  // It would be better to pass the clusterUrlBase config value to the getClusterUrl method as a parameter.
  // The reason we can't always do that is getClusterUrl is called by ClusterComponent, which is not aware of leonardo.conf.
  // A possible future solution might be to separate Cluster into an internal representation (backed by the database)
  // and an API-response representation (which may contain additional metadata/fields).
  private lazy val cachedClusterUrlBase: String = {
    val config = ConfigFactory.parseResources("leonardo.conf").withFallback(ConfigFactory.load())
    val dataprocConfig = config.as[DataprocConfig]("dataproc")
    dataprocConfig.clusterUrlBase
  }

  def getClusterUrl(googleProject: GoogleProject, clusterName: ClusterName, clusterUrlBase: String = cachedClusterUrlBase): URL = {
    new URL(clusterUrlBase + googleProject.value + "/" + clusterName.value)
  }
}

// Default cluster labels
case class DefaultLabels(clusterName: ClusterName,
                         googleProject: GoogleProject,
                         creator: WorkbenchEmail,
                         clusterServiceAccount: Option[WorkbenchEmail],
                         notebookServiceAccount: Option[WorkbenchEmail],
                         notebookUserScript: Option[GcsPath])

// Provides ways of combining MachineConfigs with Leo defaults
object MachineConfigOps {
  case class NegativeIntegerArgumentInClusterRequestException()
    extends LeoException(s"Your cluster request should not have negative integer values. Please revise your request and submit again.", StatusCodes.BadRequest)

  case class OneWorkerSpecifiedInClusterRequestException()
    extends LeoException("Google Dataproc does not support clusters with 1 non-preemptible worker. Must be 0, 2 or more.")

  private implicit val machineConfigSemigroup = new Semigroup[MachineConfig] {
    def combine(defined: MachineConfig, default: MachineConfig): MachineConfig = {
      val minimumDiskSize = 10
      defined.numberOfWorkers match {
        case None | Some(0) => MachineConfig(
          Some(0),
          defined.masterMachineType.orElse(default.masterMachineType),
          checkNegativeValue(defined.masterDiskSize.orElse(default.masterDiskSize)).map(s => math.max(minimumDiskSize, s)),
        )
        case Some(numWorkers) if numWorkers == 1 => throw OneWorkerSpecifiedInClusterRequestException()
        case numWorkers => MachineConfig(checkNegativeValue(numWorkers),
          defined.masterMachineType.orElse(default.masterMachineType),
          checkNegativeValue(defined.masterDiskSize.orElse(default.masterDiskSize)).map(s => math.max(minimumDiskSize, s)),
          defined.workerMachineType.orElse(default.workerMachineType),
          checkNegativeValue(defined.workerDiskSize.orElse(default.workerDiskSize)).map(s => math.max(minimumDiskSize, s)),
          checkNegativeValue(defined.numberOfWorkerLocalSSDs.orElse(default.numberOfWorkerLocalSSDs)),
          checkNegativeValue(defined.numberOfPreemptibleWorkers.orElse(default.numberOfPreemptibleWorkers)))
      }
    }
  }

  private def checkNegativeValue(value: Option[Int]): Option[Int] = {
    value.map(v => if (v < 0) throw NegativeIntegerArgumentInClusterRequestException() else v)
  }

  def create(definedMachineConfig: Option[MachineConfig], defaultMachineConfig: ClusterDefaultsConfig): MachineConfig = {
    definedMachineConfig.getOrElse(MachineConfig()) |+| MachineConfigOps.createFromDefaults(defaultMachineConfig)
  }

  def createFromDefaults(clusterDefaultsConfig: ClusterDefaultsConfig): MachineConfig = MachineConfig(
    Some(clusterDefaultsConfig.numberOfWorkers),
    Some(clusterDefaultsConfig.masterMachineType),
    Some(clusterDefaultsConfig.masterDiskSize),
    Some(clusterDefaultsConfig.workerMachineType),
    Some(clusterDefaultsConfig.workerDiskSize),
    Some(clusterDefaultsConfig.numberOfWorkerLocalSSDs),
    Some(clusterDefaultsConfig.numberOfPreemptibleWorkers)
  )
}

// Fields that must be templated into cluster resources (e.g. the init script).
// see https://broadinstitute.atlassian.net/browse/GAWB-2619 for why these are Strings rather than value classes
case class ClusterInitValues(googleProject: String,
                             clusterName: String,
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
                             jupyterExtensionUri: String,
                             jupyterUserScriptUri: String,
                             jupyterUserScriptOutputUri: String,
                             jupyterServiceAccountCredentials: String,
                             googleSignInJsUri: String,
                             extensionEntryUri: String,
                             jupyterLabGooglePluginUri: String,
                             safeModeJsUri: String,
                             editModeJsUri: String,
                             userEmailLoginHint: String,
                             contentSecurityPolicy: String,
                             jupyterServerExtensions: String,
                             jupyterNbExtensions: String,
                             jupyterCombinedExtensions: String,
                             jupyterNotebookConfigUri: String,
                             jupyterLabExtensions: String,
                             defaultClientId: String,
                             welderEnabled: String,
                             notebooksDir: String
                            ){
  def toMap: Map[String, String] = this.getClass.getDeclaredFields.map(_.getName).zip(this.productIterator.to).toMap.mapValues(_.toString)}

object ClusterInitValues {
  val serviceAccountCredentialsFilename = "service-account-credentials.json"

  def apply(googleProject: GoogleProject, clusterName: ClusterName, initBucketName: GcsBucketName, clusterRequest: ClusterRequest, dataprocConfig: DataprocConfig,
            clusterFilesConfig: ClusterFilesConfig, clusterResourcesConfig: ClusterResourcesConfig, proxyConfig: ProxyConfig,
            serviceAccountKey: Option[ServiceAccountKey], userEmailLoginHint: WorkbenchEmail, contentSecurityPolicy: String,
            clusterImages: Set[ClusterImage], stagingBucket: GcsBucketName): ClusterInitValues =
    ClusterInitValues(
      googleProject.value,
      clusterName.value,
      clusterImages.find(_.tool == Jupyter).map(_.dockerImage).getOrElse(""),
      clusterImages.find(_.tool == RStudio).map(_.dockerImage).getOrElse(""),
      proxyConfig.jupyterProxyDockerImage,
      clusterImages.find(_.tool == Welder).map(_.dockerImage).getOrElse(""),
      GcsPath(initBucketName, GcsObjectName(clusterFilesConfig.jupyterServerCrt.getName)).toUri,
      GcsPath(initBucketName, GcsObjectName(clusterFilesConfig.jupyterServerKey.getName)).toUri,
      GcsPath(initBucketName, GcsObjectName(clusterFilesConfig.jupyterRootCaPem.getName)).toUri,
      GcsPath(initBucketName, GcsObjectName(clusterResourcesConfig.jupyterDockerCompose.value)).toUri,
      GcsPath(initBucketName, GcsObjectName(clusterResourcesConfig.rstudioDockerCompose.value)).toUri,
      GcsPath(initBucketName, GcsObjectName(clusterResourcesConfig.proxyDockerCompose.value)).toUri,
      GcsPath(initBucketName, GcsObjectName(clusterResourcesConfig.welderDockerCompose.value)).toUri,
      GcsPath(initBucketName, GcsObjectName(clusterResourcesConfig.proxySiteConf.value)).toUri,
      dataprocConfig.jupyterServerName,
      dataprocConfig.rstudioServerName,
      dataprocConfig.welderServerName,
      proxyConfig.proxyServerName,
      clusterRequest.jupyterExtensionUri.map(_.toUri).getOrElse(""),
      clusterRequest.jupyterUserScriptUri.map(_.toUri).getOrElse(""),
      GcsPath(stagingBucket, GcsObjectName("userscript_output.txt")).toUri,
      serviceAccountKey.map(_ => GcsPath(initBucketName, GcsObjectName(serviceAccountCredentialsFilename)).toUri).getOrElse(""),
      GcsPath(initBucketName, GcsObjectName(clusterResourcesConfig.googleSignInJs.value)).toUri,
      GcsPath(initBucketName, GcsObjectName(clusterResourcesConfig.extensionEntry.value)).toUri,
      GcsPath(initBucketName, GcsObjectName(clusterResourcesConfig.jupyterLabGooglePlugin.value)).toUri,
      GcsPath(initBucketName, GcsObjectName(clusterResourcesConfig.safeModeJs.value)).toUri,
      GcsPath(initBucketName, GcsObjectName(clusterResourcesConfig.editModeJs.value)).toUri,
      userEmailLoginHint.value,
      contentSecurityPolicy,
      clusterRequest.userJupyterExtensionConfig.map(x => x.serverExtensions.values.mkString(" ")).getOrElse(""),
      clusterRequest.userJupyterExtensionConfig.map(x => x.nbExtensions.values.mkString(" ")).getOrElse(""),
      clusterRequest.userJupyterExtensionConfig.map(x => x.combinedExtensions.values.mkString(" ")).getOrElse(""),
      GcsPath(initBucketName, GcsObjectName(clusterResourcesConfig.jupyterNotebookConfigUri.value)).toUri,
      clusterRequest.userJupyterExtensionConfig.map(x => x.labExtensions.values.mkString(" ")).getOrElse(""),
      clusterRequest.defaultClientId.getOrElse(""),
      clusterRequest.enableWelder.getOrElse(false).toString,  // TODO: remove this when welder is rolled out to all clusters
      dataprocConfig.notebooksDir
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

  def stringToObject: Map[String, PropertyFilePrefix] = values.map(v =>  v.toString -> v).toMap
}

sealed trait ExtensionType extends EnumEntry
object ExtensionType extends Enum[ExtensionType] {
  val values = findValues

  case object NBExtension extends ExtensionType
  case object ServerExtension extends ExtensionType
  case object CombinedExtension extends ExtensionType
  case object LabExtension extends ExtensionType
}

object LeonardoJsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  implicit object URLFormat extends JsonFormat[URL] {
    def write(obj: URL) = JsString(obj.toString)

    def read(json: JsValue): URL = json match {
      case JsString(url) => new URL(url)
      case other => throw DeserializationException("Expected URL, got: " + other)
    }
  }

  // Overrides the one from workbench-libs to serialize/deserialize as a URI
  implicit object GcsPathFormat extends JsonFormat[GcsPath] {
    def write(obj: GcsPath) = JsString(obj.toUri)

    def read(json: JsValue): GcsPath = json match {
      case JsString(uri) => parseGcsPath(uri).getOrElse(throw DeserializationException(s"Could not parse bucket URI from: $uri"))
      case other => throw DeserializationException(s"Expected bucket URI, got: $other")
    }
  }

  implicit val UserClusterExtensionConfigFormat = jsonFormat4(UserJupyterExtensionConfig.apply)

  implicit val ClusterRequestFormat: RootJsonReader[ClusterRequest] = (json: JsValue) => {
    val fields = json.asJsObject.fields
    val properties = for {
      props <- fields.get("properties").map(_.convertTo[Map[String, String]])
    } yield {
      // validating user's properties input has valid prefix
      props.keys.toList.map{
        s =>
          val prefix = s.split(":")(0)
          PropertyFilePrefix.stringToObject.get(prefix).getOrElse(throw new RuntimeException(s"invalid properties $s"))
      }
      props
    }

    val fieldsWithoutNull = fields.filterNot(x => x._2 == null || x._2 == JsNull)

    ClusterRequest(
      fieldsWithoutNull.get("labels").map(_.convertTo[LabelMap]).getOrElse(Map.empty),
      fieldsWithoutNull.get("jupyterExtensionUri").map(_.convertTo[GcsPath]),
      fieldsWithoutNull.get("jupyterUserScriptUri").map(_.convertTo[GcsPath]),
      fieldsWithoutNull.get("machineConfig").map(_.convertTo[MachineConfig]),
      properties.getOrElse(Map.empty),
      fieldsWithoutNull.get("stopAfterCreation").map(_.convertTo[Boolean]),
      fieldsWithoutNull.get("userJupyterExtensionConfig").map(_.convertTo[UserJupyterExtensionConfig]),
      fieldsWithoutNull.get("autopause").map(_.convertTo[Boolean]),
      fieldsWithoutNull.get("autopauseThreshold").map(_.convertTo[Int]),
      fieldsWithoutNull.get("defaultClientId").map(_.convertTo[String]),
      fieldsWithoutNull.get("jupyterDockerImage").map(_.convertTo[String]),
      fieldsWithoutNull.get("rstudioDockerImage").map(_.convertTo[String]),
      fieldsWithoutNull.get("welderDockerImage").map(_.convertTo[String]),
      fieldsWithoutNull.get("scopes").map(_.convertTo[Set[String]]).getOrElse(Set.empty),
      fieldsWithoutNull.get("enableWelder").map(_.convertTo[Boolean])
    )
  }

  implicit val ClusterResourceFormat = ValueObjectFormat(ClusterResource)

  implicit val ServiceAccountInfoFormat = jsonFormat2(ServiceAccountInfo)

  implicit val ClusterErrorFormat = jsonFormat3(ClusterError.apply)

  implicit val DefaultLabelsFormat = jsonFormat6(DefaultLabels.apply)

  implicit val ClusterToolFormat = EnumEntryFormat(ClusterTool.withName)

  implicit val ClusterImageFormat = jsonFormat3(ClusterImage.apply)


  implicit object ClusterFormat extends RootJsonFormat[Cluster] {
    override def read(json: JsValue): Cluster = {
      json match {
        case JsObject(fields: Map[String, JsValue]) =>
          Cluster(
            fields.getOrElse("id", JsNull).convertTo[Long],
            fields.getOrElse("clusterName", JsNull).convertTo[ClusterName],
            fields.getOrElse("googleProject", JsNull).convertTo[GoogleProject],
            fields.getOrElse("serviceAccountInfo", JsNull).convertTo[ServiceAccountInfo],
            DataprocInfo(fields.getOrElse("googleId", JsNull).convertTo[Option[UUID]],
                         fields.getOrElse("operationName", JsNull).convertTo[Option[OperationName]],
                         fields.getOrElse("stagingBucket", JsNull).convertTo[Option[GcsBucketName]],
                         fields.getOrElse("hostIp", JsNull).convertTo[Option[IP]]),
            AuditInfo(fields.getOrElse("creator", JsNull).convertTo[WorkbenchEmail],
                      fields.getOrElse("createdDate", JsNull).convertTo[Instant],
                      fields.getOrElse("destroyedDate", JsNull).convertTo[Option[Instant]],
                      fields.getOrElse("dateAccessed", JsNull).convertTo[Instant],
                      fields.getOrElse("kernelFoundBusyDate", JsNull).convertTo[Option[Instant]]),
            fields.getOrElse("machineConfig", JsNull).convertTo[MachineConfig],
            fields.getOrElse("properties", JsNull).convertTo[Option[Map[String, String]]].getOrElse(Map.empty),
            fields.getOrElse("clusterUrl", JsNull).convertTo[URL],
            fields.getOrElse("status", JsNull).convertTo[ClusterStatus],
            fields.getOrElse("labels", JsNull).convertTo[LabelMap],
            fields.getOrElse("jupyterExtensionUri", JsNull).convertTo[Option[GcsPath]],
            fields.getOrElse("jupyterUserScriptUri", JsNull).convertTo[Option[GcsPath]],
            fields.getOrElse("errors", JsNull).convertTo[List[ClusterError]],
            fields.getOrElse("instances", JsNull).convertTo[Set[Instance]],
            fields.getOrElse("userJupyterExtensionConfig", JsNull).convertTo[Option[UserJupyterExtensionConfig]],
            fields.getOrElse("autopauseThreshold", JsNull).convertTo[Int],
            fields.getOrElse("defaultClientId", JsNull).convertTo[Option[String]],
            fields.getOrElse("stopAfterCreation", JsNull).convertTo[Boolean],
            fields.getOrElse("clusterImages", JsNull).convertTo[Set[ClusterImage]],
            fields.getOrElse("scopes", JsNull).convertTo[Set[String]],
            fields.getOrElse("welderEnabled", JsNull).convertTo[Boolean])
        case _ => deserializationError("Cluster expected as a JsObject")
      }
    }

    override def write(obj: Cluster): JsValue = {
      val allFields = List(
        "id" -> obj.id.toJson,
        "clusterName" -> obj.clusterName.toJson,
        "googleId" -> obj.dataprocInfo.googleId.toJson,
        "googleProject" -> obj.googleProject.toJson,
        "serviceAccountInfo" -> obj.serviceAccountInfo.toJson,
        "machineConfig" -> obj.machineConfig.toJson,
        "clusterUrl" -> obj.clusterUrl.toJson,
        "operationName" -> obj.dataprocInfo.operationName.toJson,
        "status" -> obj.status.toJson,
        "hostIp" -> obj.dataprocInfo.hostIp.toJson,
        "creator" -> obj.auditInfo.creator.toJson,
        "createdDate" -> obj.auditInfo.createdDate.toJson,
        "destroyedDate" -> obj.auditInfo.destroyedDate.toJson,
        "kernelFoundBusyDate" -> obj.auditInfo.kernelFoundBusyDate.toJson,
        "labels" -> obj.labels.toJson,
        "jupyterExtensionUri" -> obj.jupyterExtensionUri.toJson,
        "jupyterUserScriptUri" -> obj.jupyterUserScriptUri.toJson,
        "stagingBucket" -> obj.dataprocInfo.stagingBucket.toJson,
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

      JsObject(presentFields:_*)
    }
  }
}
