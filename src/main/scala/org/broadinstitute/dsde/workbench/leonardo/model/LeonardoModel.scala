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
import org.broadinstitute.dsde.workbench.leonardo.model.google.DataprocRole.SecondaryWorker
import org.broadinstitute.dsde.workbench.leonardo.model.google.GoogleJsonSupport._
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.model.WorkbenchIdentityJsonSupport._
import org.broadinstitute.dsde.workbench.model.google.GoogleModelJsonSupport._
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.model._
import spray.json._

// Create cluster API request
case class ClusterRequest(labels: LabelMap = Map(),
                          jupyterExtensionUri: Option[GcsPath] = None,
                          jupyterUserScriptUri: Option[GcsPath] = None,
                          machineConfig: Option[MachineConfig] = None,
                          stopAfterCreation: Option[Boolean] = None,
                          userJupyterExtensionConfig: Option[UserJupyterExtensionConfig] = None,
                          autopause: Option[Boolean] = None,
                          autopauseThreshold: Option[Int] = None,
                          defaultClientId: Option[String] = None,
                          scopes: Option[Set[String]] = None)


case class UserJupyterExtensionConfig(nbExtensions: Map[String, String] = Map(),
                                      serverExtensions: Map[String, String] = Map(),
                                      combinedExtensions: Map[String, String] = Map())


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
                     dateAccessed: Instant)

// The cluster itself
// Also the API response for "list clusters" and "get active cluster"
case class Cluster(id: Long = 0, // DB AutoInc
                   clusterName: ClusterName,
                   googleProject: GoogleProject,
                   serviceAccountInfo: ServiceAccountInfo,
                   dataprocInfo: DataprocInfo,
                   auditInfo: AuditInfo,
                   machineConfig: MachineConfig,
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
                   scopes: Set[String]) {
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
             stagingBucket: Option[GcsBucketName] = None): Cluster = {
    Cluster(
      clusterName = clusterName,
      googleProject = googleProject,
      serviceAccountInfo = serviceAccountInfo,
      dataprocInfo = DataprocInfo(operation.map(_.uuid), operation.map(_.name), stagingBucket, None),
      auditInfo = AuditInfo(userEmail, Instant.now(), None, Instant.now()),
      machineConfig = machineConfig,
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
      scopes = clusterScopes)
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
        case None | Some(0) => MachineConfig(Some(0), defined.masterMachineType.orElse(default.masterMachineType),
          checkNegativeValue(defined.masterDiskSize.orElse(default.masterDiskSize)).map(s => math.max(minimumDiskSize, s)))
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
                             proxyDockerImage: String,
                             jupyterServerCrt: String,
                             jupyterServerKey: String,
                             rootCaPem: String,
                             jupyterDockerCompose: String,
                             jupyterProxySiteConf: String,
                             jupyterServerName: String,
                             proxyServerName: String,
                             jupyterExtensionUri: String,
                             jupyterUserScriptUri: String,
                             jupyterServiceAccountCredentials: String,
                             jupyterCustomJsUri: String,
                             jupyterGoogleSignInJsUri: String,
                             userEmailLoginHint: String,
                             contentSecurityPolicy: String,
                             jupyterServerExtensions: String,
                             jupyterNbExtensions: String,
                             jupyterCombinedExtensions: String,
                             jupyterNotebookConfigUri: String,
                             defaultClientId: String
                            ){
  def toMap: Map[String, String] = this.getClass.getDeclaredFields.map(_.getName).zip(this.productIterator.to).toMap.mapValues(_.toString)}

object ClusterInitValues {
  val serviceAccountCredentialsFilename = "service-account-credentials.json"

  def apply(googleProject: GoogleProject, clusterName: ClusterName, initBucketName: GcsBucketName, clusterRequest: ClusterRequest, dataprocConfig: DataprocConfig,
            clusterFilesConfig: ClusterFilesConfig, clusterResourcesConfig: ClusterResourcesConfig, proxyConfig: ProxyConfig,
            serviceAccountKey: Option[ServiceAccountKey], userEmailLoginHint: WorkbenchEmail, contentSecurityPolicy: String): ClusterInitValues =
    ClusterInitValues(
      googleProject.value,
      clusterName.value,
      dataprocConfig.dataprocDockerImage,
      proxyConfig.jupyterProxyDockerImage,
      GcsPath(initBucketName, GcsObjectName(clusterFilesConfig.jupyterServerCrt.getName)).toUri,
      GcsPath(initBucketName, GcsObjectName(clusterFilesConfig.jupyterServerKey.getName)).toUri,
      GcsPath(initBucketName, GcsObjectName(clusterFilesConfig.jupyterRootCaPem.getName)).toUri,
      GcsPath(initBucketName, GcsObjectName(clusterResourcesConfig.clusterDockerCompose.value)).toUri,
      GcsPath(initBucketName, GcsObjectName(clusterResourcesConfig.jupyterProxySiteConf.value)).toUri,
      dataprocConfig.jupyterServerName,
      proxyConfig.proxyServerName,
      clusterRequest.jupyterExtensionUri.map(_.toUri).getOrElse(""),
      clusterRequest.jupyterUserScriptUri.map(_.toUri).getOrElse(""),
      serviceAccountKey.map(_ => GcsPath(initBucketName, GcsObjectName(serviceAccountCredentialsFilename)).toUri).getOrElse(""),
      GcsPath(initBucketName, GcsObjectName(clusterResourcesConfig.jupyterCustomJs.value)).toUri,
      GcsPath(initBucketName, GcsObjectName(clusterResourcesConfig.jupyterGoogleSignInJs.value)).toUri,
      userEmailLoginHint.value,
      contentSecurityPolicy,
      clusterRequest.userJupyterExtensionConfig.map(x => x.serverExtensions.values.mkString(" ")).getOrElse(""),
      clusterRequest.userJupyterExtensionConfig.map(x => x.nbExtensions.values.mkString(" ")).getOrElse(""),
      clusterRequest.userJupyterExtensionConfig.map(x => x.combinedExtensions.values.mkString(" ")).getOrElse(""),
      GcsPath(initBucketName, GcsObjectName(clusterResourcesConfig.jupyterNotebookConfigUri.value)).toUri,
      clusterRequest.defaultClientId.getOrElse("")
    )
}

sealed trait ExtensionType extends EnumEntry
object ExtensionType extends Enum[ExtensionType] {
  val values = findValues

  case object NBExtension extends ExtensionType
  case object ServerExtension extends ExtensionType
  case object CombinedExtension extends ExtensionType
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

  implicit val UserClusterExtensionConfigFormat = jsonFormat3(UserJupyterExtensionConfig.apply)

  implicit val ClusterRequestFormat = jsonFormat10(ClusterRequest)

  implicit val ClusterResourceFormat = ValueObjectFormat(ClusterResource)

  implicit val ServiceAccountInfoFormat = jsonFormat2(ServiceAccountInfo)

  implicit val ClusterErrorFormat = jsonFormat3(ClusterError.apply)

  implicit val DefaultLabelsFormat = jsonFormat6(DefaultLabels.apply)


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
                      fields.getOrElse("dateAccessed", JsNull).convertTo[Instant]),
            fields.getOrElse("machineConfig", JsNull).convertTo[MachineConfig],
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
            fields.getOrElse("scopes", JsNull).convertTo[Set[String]])
//            fields.getOrElse("scopes", JsNull).convertTo[Option[Set[String]]] match {
//              case Some(scopes) => scopes
//              case None => Set("https://www.googleapis.com/auth/userinfo.email", "https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/source.read_only")
//          })
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
        "stopAfterCreation" -> Option(obj.stopAfterCreation).toJson,
        "scopes" -> obj.scopes.toJson
      )

      val presentFields = allFields.filter(_._2 != JsNull)

      JsObject(presentFields:_*)
    }
  }
}
