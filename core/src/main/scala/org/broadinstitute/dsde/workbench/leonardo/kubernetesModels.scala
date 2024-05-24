package org.broadinstitute.dsde.workbench.leonardo

import ca.mrvisser.sealerate
import org.broadinstitute.dsde.workbench.google2.GKEModels.{KubernetesClusterId, KubernetesClusterName, NodepoolName}
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{
  NamespaceName,
  ServiceAccountName,
  ServiceName
}
import org.broadinstitute.dsde.workbench.google2.{
  KubernetesName,
  Location,
  MachineTypeName,
  NetworkName,
  RegionName,
  SubnetworkName
}
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId._
import org.broadinstitute.dsde.workbench.model.{IP, TraceId, WorkbenchEmail}
import org.broadinstitute.dsp.{ChartName, ChartVersion, Release}
import org.http4s.Uri

import java.net.URL
import java.time.Instant
import java.util.UUID

case class KubernetesCluster(id: KubernetesClusterLeoId,
                             cloudContext: CloudContext,
                             clusterName: KubernetesClusterName,
                             // the GKE API supports a location (e.g. us-central1 (a 'region') or us-central1-a (a 'zone))
                             // If a zone is specified, it will be a single-zone cluster, otherwise it will span multiple zones in the region
                             // Leo currently specifies a zone, e.g. "us-central1-a" and makes all clusters single-zone
                             // Location is exposed here in case we ever want to leverage the flexibility GKE provides
                             location: Location,
                             region: RegionName,
                             status: KubernetesClusterStatus,
                             ingressChart: Chart,
                             auditInfo: AuditInfo,
                             asyncFields: Option[KubernetesClusterAsyncFields],
                             nodepools: List[Nodepool]
) {

  // TODO consider renaming this method and the KubernetesClusterId class
  // to disambiguate a bit with KubernetesClusterLeoId which is a Leo-specific ID
  def getClusterId: KubernetesClusterId = cloudContext match {
    case CloudContext.Gcp(value) => KubernetesClusterId(value, location, clusterName)
    case CloudContext.Azure(_)   => throw new IllegalStateException("Can't get GCP ID for an Azure cluster")
  }
}

final case class KubernetesClusterAsyncFields(loadBalancerIp: IP, apiServerIp: IP, networkInfo: NetworkFields)

final case class NetworkFields(networkName: NetworkName, subNetworkName: SubnetworkName, subNetworkIpRange: IpRange)

//should be in the format 0.0.0.0/0
final case class CidrIP(value: String) extends AnyVal

final case class KubernetesClusterVersion(value: String) extends AnyVal

/** Google Container Cluster statuses
 *  see: https://cloud.google.com/kubernetes-engine/docs/reference/rest/v1/projects.locations.clusters#Cluster.Status
 */
sealed abstract class KubernetesClusterStatus
object KubernetesClusterStatus {
  final case object Unspecified extends KubernetesClusterStatus {
    override def toString: String = "STATUS_UNSPECIFIED"
  }

  final case object Provisioning extends KubernetesClusterStatus {
    override def toString: String = "PROVISIONING"
  }

  final case object Running extends KubernetesClusterStatus {
    override def toString: String = "RUNNING"
  }

  final case object Reconciling extends KubernetesClusterStatus {
    override def toString: String = "RECONCILING"
  }

  // in kubernetes statuses, they label 'deleting' resources as 'stopping'
  final case object Deleting extends KubernetesClusterStatus {
    override def toString: String = "STOPPING"
  }

  final case object Error extends KubernetesClusterStatus {
    override def toString: String = "ERROR"
  }

  final case object Degraded extends KubernetesClusterStatus {
    override def toString: String = "DEGRADED"
  }

  // this is a custom status, and will not be returned from google
  final case object Deleted extends KubernetesClusterStatus {
    override def toString: String = "DELETED"
  }

  final case object Precreating extends KubernetesClusterStatus {
    override def toString: String = "PRECREATING"
  }

  val deleted: KubernetesClusterStatus = Deleted
  def values: Set[KubernetesClusterStatus] = sealerate.values[KubernetesClusterStatus]
  def stringToObject: Map[String, KubernetesClusterStatus] = values.map(v => v.toString -> v).toMap

  val deletableStatuses: Set[KubernetesClusterStatus] =
    Set(Unspecified, Running, Reconciling, Error, Degraded)

  val creatingStatuses: Set[KubernetesClusterStatus] =
    Set(Precreating, Provisioning)
}

/** Google Container Nodepool statuses
 * See https://googleapis.github.io/googleapis/java/all/latest/apidocs/com/google/container/v1/NodePool.Status.html
 */
sealed abstract class NodepoolStatus
object NodepoolStatus {
  case object Unspecified extends NodepoolStatus {
    override def toString: String = "STATUS_UNSPECIFIED"
  }

  case object Provisioning extends NodepoolStatus {
    override def toString: String = "PROVISIONING"
  }

  case object Running extends NodepoolStatus {
    override def toString: String = "RUNNING"
  }

  case object Reconciling extends NodepoolStatus {
    override def toString: String = "RECONCILING"
  }

  // in kubernetes statuses, they label "deleting' resources as 'stopping'
  case object Deleting extends NodepoolStatus {
    override def toString: String = "STOPPING"
  }

  case object Error extends NodepoolStatus {
    override def toString: String = "ERROR"
  }

  // this is a custom status, and will not be returned from google
  case object Deleted extends NodepoolStatus {
    override def toString: String = "DELETED"
  }

  case object RunningWithError extends NodepoolStatus {
    override def toString: String = "RUNNING_WITH_ERROR"
  }

  final case object Precreating extends NodepoolStatus {
    override def toString: String = "PRECREATING"
  }

  def values: Set[NodepoolStatus] = sealerate.values[NodepoolStatus]
  def stringToObject: Map[String, NodepoolStatus] = values.map(v => v.toString -> v).toMap

  val deletableStatuses: Set[NodepoolStatus] =
    Set(Unspecified, Running, Reconciling, Error, RunningWithError)

  val nonParellizableStatuses: Set[NodepoolStatus] =
    Set(Deleting, Provisioning)
}

final case class KubernetesClusterLeoId(id: Long) extends AnyVal

final case class Nodepool(id: NodepoolLeoId,
                          clusterId: KubernetesClusterLeoId,
                          nodepoolName: NodepoolName,
                          status: NodepoolStatus,
                          auditInfo: AuditInfo,
                          machineType: MachineTypeName,
                          numNodes: NumNodes,
                          autoscalingEnabled: Boolean,
                          autoscalingConfig: Option[AutoscalingConfig],
                          apps: List[App],
                          isDefault: Boolean
)
object KubernetesNameUtils {
  // UUID almost works for this use case, but kubernetes names must start with a-z
  def getUniqueName[A](apply: String => A): Either[Throwable, A] =
    KubernetesName.withValidation(s"k${UUID.randomUUID().toString.toLowerCase.substring(1)}", apply)
}

case class DefaultNodepool(id: NodepoolLeoId,
                           clusterId: KubernetesClusterLeoId,
                           nodepoolName: NodepoolName,
                           status: NodepoolStatus,
                           auditInfo: AuditInfo,
                           machineType: MachineTypeName,
                           numNodes: NumNodes,
                           autoscalingEnabled: Boolean,
                           autoscalingConfig: Option[AutoscalingConfig]
) {
  def toNodepool(): Nodepool =
    Nodepool(id,
             clusterId,
             nodepoolName,
             status,
             auditInfo,
             machineType,
             numNodes,
             autoscalingEnabled,
             autoscalingConfig,
             List.empty,
             true
    )
}
object DefaultNodepool {
  def fromNodepool(n: Nodepool) =
    DefaultNodepool(n.id,
                    n.clusterId,
                    n.nodepoolName,
                    n.status,
                    n.auditInfo,
                    n.machineType,
                    n.numNodes,
                    n.autoscalingEnabled,
                    n.autoscalingConfig
    )
}

final case class AutoscalingConfig(autoscalingMin: AutoscalingMin, autoscalingMax: AutoscalingMax)

final case class NodepoolLeoId(id: Long) extends AnyVal
final case class NumNodes(amount: Int) extends AnyVal
final case class AutoscalingMin(amount: Int) extends AnyVal
final case class AutoscalingMax(amount: Int) extends AnyVal

final case class DefaultKubernetesLabels(cloudContext: CloudContext,
                                         appName: AppName,
                                         creator: WorkbenchEmail,
                                         serviceAccount: WorkbenchEmail
) {
  val cloudContextList = cloudContext match {
    case CloudContext.Gcp(value)   => List("googleProject" -> value.value)
    case CloudContext.Azure(value) => List("cloudContext" -> value.asString)
  }
  val toMap: LabelMap =
    Map(
      "appName" -> appName.value,
      "creator" -> creator.value,
      "clusterServiceAccount" -> serviceAccount.value
    ) ++ cloudContextList
}

sealed abstract class ErrorAction
object ErrorAction {
  case object CreateApp extends ErrorAction {
    override def toString: String = "createApp"
  }

  case object DeleteApp extends ErrorAction {
    override def toString: String = "deleteApp"
  }

  case object StopApp extends ErrorAction {
    override def toString: String = "stopApp"
  }

  case object StartApp extends ErrorAction {
    override def toString: String = "startApp"
  }

  case object UpdateApp extends ErrorAction {
    override def toString: String = "updateApp"
  }

  case object DeleteNodepool extends ErrorAction {
    override def toString: String = "deleteNodepool"
  }

  def values: Set[ErrorAction] = sealerate.values[ErrorAction]
  def stringToObject: Map[String, ErrorAction] = values.map(v => v.toString -> v).toMap
}
final case class AppError(errorMessage: String,
                          timestamp: Instant,
                          action: ErrorAction,
                          source: ErrorSource,
                          googleErrorCode: Option[Int],
                          traceId: Option[TraceId] = None
)

final case class KubernetesErrorId(value: Long) extends AnyVal

sealed abstract class ErrorSource
object ErrorSource {
  case object Cluster extends ErrorSource {
    override def toString: String = "cluster"
  }

  case object Nodepool extends ErrorSource {
    override def toString: String = "nodepool"
  }

  case object App extends ErrorSource {
    override def toString: String = "app"
  }

  case object Disk extends ErrorSource {
    override def toString: String = "disk"
  }
  case object PostgresDisk extends ErrorSource {
    override def toString: String = "postgres-disk"
  }

  def values: Set[ErrorSource] = sealerate.values[ErrorSource]
  def stringToObject: Map[String, ErrorSource] = values.map(v => v.toString -> v).toMap
}

sealed abstract class AllowedChartName extends Product with Serializable {
  def asString: String
}
object AllowedChartName {
  final case object RStudio extends AllowedChartName {
    def asString: String = "rstudio"
  }
  final case object Sas extends AllowedChartName {
    def asString: String = "sas"
  }

  // We used to have different chart names for RStudio and SAS. This is to handle the old names for backwards-compatibility
  private val deprecatedName: Map[String, AllowedChartName] = Map(
    "aou-rstudio-chart" -> RStudio,
    "aou-sas-chart" -> Sas
  )

  def stringToObject: Map[String, AllowedChartName] =
    sealerate.values[AllowedChartName].map(v => v.asString -> v).toMap ++ deprecatedName

  // Chartname from DB has the following format: /leonardo/cromwell-0.2.291
  def fromChartName(chartName: ChartName): Option[AllowedChartName] = {
    val splittedString = chartName.asString.split("/")
    if (splittedString.size == 3) {
      stringToObject.get(splittedString(2))
    } else None
  }
}

sealed abstract class AppType
object AppType {
  case object Galaxy extends AppType {
    override def toString: String = "GALAXY"
  }
  case object Cromwell extends AppType {
    override def toString: String = "CROMWELL"
  }
  case object WorkflowsApp extends AppType {
    override def toString: String = "WORKFLOWS_APP"
  }
  case object CromwellRunnerApp extends AppType {
    override def toString: String = "CROMWELL_RUNNER_APP"
  }

  case object Wds extends AppType {
    override def toString: String = "WDS"
  }

  case object HailBatch extends AppType {
    override def toString: String = "HAIL_BATCH"
  }

  // See more context in https://docs.google.com/document/d/1RaQRMqAx7ymoygP6f7QVdBbZC-iD9oY_XLNMe_oz_cs/edit
  case object Allowed extends AppType {
    override def toString: String = "ALLOWED"
  }

  case object Custom extends AppType {
    override def toString: String = "CUSTOM"
  }

  def values: Set[AppType] = sealerate.values[AppType]
  def stringToObject: Map[String, AppType] = values.map(v => v.toString -> v).toMap

  /**
   * Disk formatting for an App. Currently, only Galaxy, RStudio and Custom app types
   * support disk management. So we default all other app types to Cromwell,
   * but the field is unused.
   */
  def appTypeToFormattedByType(appType: AppType): FormattedBy =
    appType match {
      case Galaxy                                                        => FormattedBy.Galaxy
      case Custom                                                        => FormattedBy.Custom
      case Allowed                                                       => FormattedBy.Allowed
      case Cromwell | Wds | HailBatch | WorkflowsApp | CromwellRunnerApp => FormattedBy.Cromwell
    }
}

final case class AppId(id: Long) extends AnyVal
final case class AppName(value: String) extends AnyVal
//These are async from the perspective of Front Leo saving the app record, but both must exist before the helm command is executed
final case class AppResources(namespace: NamespaceName,
                              disk: Option[PersistentDisk],
                              services: List[KubernetesService],
                              kubernetesServiceAccountName: Option[ServiceAccountName]
)

final case class Chart(name: ChartName, version: ChartVersion) {
  override def toString: String = s"${name.asString}${Chart.nameVersionSeparator}${version.asString}"
}
object Chart {
  val nameVersionSeparator: Char = '-'

  def fromString(s: String): Option[Chart] = {
    val separatorIndex = s.lastIndexOf(nameVersionSeparator)
    if (separatorIndex > 0) {
      val name = s.substring(0, separatorIndex)
      val version = s.substring(separatorIndex + 1)
      if (name.nonEmpty && version.nonEmpty) {
        Some(Chart(ChartName(name), ChartVersion(version)))
      } else None
    } else None
  }
}

final case class AutodeleteThreshold(value: Int) extends AnyVal

final case class App(id: AppId,
                     nodepoolId: NodepoolLeoId,
                     appType: AppType,
                     appName: AppName,
                     appAccessScope: Option[AppAccessScope],
                     workspaceId: Option[WorkspaceId],
                     status: AppStatus,
                     chart: Chart,
                     release: Release,
                     samResourceId: AppSamResourceId,
                     googleServiceAccount: WorkbenchEmail,
                     auditInfo: AuditInfo,
                     labels: LabelMap,
                     // this is populated async to app creation
                     appResources: AppResources,
                     errors: List[AppError],
                     customEnvironmentVariables: Map[String, String],
                     descriptorPath: Option[Uri],
                     extraArgs: List[String],
                     sourceWorkspaceId: Option[WorkspaceId],
                     numOfReplicas: Option[Int],
                     autodeleteEnabled: Boolean,
                     autodeleteThreshold: Option[AutodeleteThreshold]
) {

  def getProxyUrls(cluster: KubernetesCluster, proxyUrlBase: String): Map[ServiceName, URL] =
    appResources.services.flatMap { service =>
      // A service can optionally define a path; otherwise, use the name.
      val leafPath = service.config.path.map(_.value).getOrElse(s"/${service.config.name.value}")
      // GCP uses a Leo proxy endpoint: e.g. https://notebooks.firecloud.org/google/v1/apps/{project}/{app}/{service}
      // Azure uses Azure relay: e.g. https://{namespace}.servicebus.windows.net/{app}-{workspaceId}/{service}
      val proxyPathOpt = cluster.cloudContext match {
        case CloudContext.Gcp(project) =>
          Some(s"${proxyUrlBase}google/v1/apps/${project.value}/${appName.value}${leafPath}")
        case CloudContext.Azure(_) =>
          // for backwards compatibility, name used to be just the appName
          cluster.asyncFields
            .map(_.loadBalancerIp.asString)
            .map(base =>
              s"${base}${customEnvironmentVariables.getOrElse("RELAY_HYBRID_CONNECTION_NAME", appName.value)}${leafPath}"
            )
        case _ =>
          None
      }
      proxyPathOpt.fold(Map.empty[ServiceName, URL])(p => Map(service.config.name -> new URL(p)))
    }.toMap
}

sealed abstract class AppStatus
object AppStatus {
  case object Unspecified extends AppStatus {
    override def toString: String = "STATUS_UNSPECIFIED"
  }

  case object Running extends AppStatus {
    override def toString: String = "RUNNING"
  }

  case object Error extends AppStatus {
    override def toString: String = "ERROR"
  }

  case object Deleting extends AppStatus {
    override def toString: String = "DELETING"
  }

  case object Deleted extends AppStatus {
    override def toString: String = "DELETED"
  }

  final case object Precreating extends AppStatus {
    override def toString: String = "PRECREATING"
  }

  final case object Predeleting extends AppStatus {
    override def toString: String = "PREDELETING"
  }

  final case object Provisioning extends AppStatus {
    override def toString: String = "PROVISIONING"
  }

  final case object PreStopping extends AppStatus {
    override def toString: String = "PRESTOPPING"
  }

  final case object Stopping extends AppStatus {
    override def toString: String = "STOPPING"
  }

  final case object Stopped extends AppStatus {
    override def toString: String = "STOPPED"
  }

  final case object PreStarting extends AppStatus {
    override def toString: String = "PRESTARTING"
  }

  final case object Starting extends AppStatus {
    override def toString: String = "STARTING"
  }

  final case object Updating extends AppStatus {
    override def toString: String = "UPDATING"
  }

  def values: Set[AppStatus] = sealerate.values[AppStatus]
  def stringToObject: Map[String, AppStatus] = values.map(v => v.toString -> v).toMap

  val deletableStatuses: Set[AppStatus] =
    Set(Unspecified, Running, Error)

  val stoppableStatuses: Set[AppStatus] =
    Set(Running, Starting)

  val startableStatuses: Set[AppStatus] =
    Set(Stopped, Stopping)

  val monitoredStatuses: Set[AppStatus] =
    Set(Deleting, Provisioning)

  implicit class EnrichedDiskStatus(status: AppStatus) {
    def isDeletable: Boolean = deletableStatuses contains status

    def isStoppable: Boolean = stoppableStatuses contains status

    def isStartable: Boolean = startableStatuses contains status

  }
}

final case class KubernetesService(id: ServiceId, config: ServiceConfig)
final case class ServiceId(id: Long) extends AnyVal
final case class ServiceConfig(name: ServiceName, kind: KubernetesServiceKindName, path: Option[ServicePath] = None)
final case class KubernetesServiceKindName(value: String) extends AnyVal
final case class ServicePath(value: String) extends AnyVal

final case class KubernetesRuntimeConfig(numNodes: NumNodes, machineType: MachineTypeName, autoscalingEnabled: Boolean)
final case class NumNodepools(value: Int) extends AnyVal

final case class NodepoolNotFoundException(nodepoolLeoId: NodepoolLeoId) extends Exception {
  override def getMessage: String = s"nodepool with id ${nodepoolLeoId} not found"
}

final case class DefaultNodepoolNotFoundException(clusterId: KubernetesClusterLeoId) extends Exception {
  override def getMessage: String = s"Unable to find default nodepool for cluster with id ${clusterId}"
}

final case class DbPassword(value: String) extends AnyVal
final case class ReleaseNameSuffix(value: String) extends AnyVal
final case class NamespaceNameSuffix(value: String) extends AnyVal

final case class GalaxyOrchUrl(value: String) extends AnyVal
final case class GalaxyDrsUrl(value: String) extends AnyVal
final case class AppMachineType(memorySizeInGb: Int, numOfCpus: Int)
final case class KsaName(value: String) extends AnyVal
