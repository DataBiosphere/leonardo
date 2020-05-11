package org.broadinstitute.dsde.workbench.leonardo

import ca.mrvisser.sealerate
import org.broadinstitute.dsde.workbench.google2.GKEModels.{KubernetesClusterId, KubernetesClusterName, NodepoolName}
import org.broadinstitute.dsde.workbench.google2.KubernetesModels.KubernetesApiServerIp
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.KubernetesNamespaceName
import org.broadinstitute.dsde.workbench.google2.{Location, MachineTypeName, NetworkName, SubnetworkName}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

case class KubernetesCluster(id: KubernetesClusterLeoId,
                             googleProject: GoogleProject,
                             clusterName: KubernetesClusterName,
                             // the GKE API supports a location (e.g. us-central1 (a 'region') or us-central1-a (a 'zone))
                             // If a zone is specified, it will be a single-zone cluster, otherwise it will span multiple zones in the region
                             // Leo currently specifies a zone, e.g. "us-central1-a" and makes all clusters single-zone
                             // Location is exposed here in case we ever want to leverage the flexibility GKE provides
                             location: Location,
                             status: KubernetesClusterStatus,
                             serviceAccount: WorkbenchEmail,
                             samResourceId: KubernetesClusterSamResourceId,
                             auditInfo: AuditInfo,
                             asyncFields: Option[KubernetesClusterAsyncFields],
                             namespaces: Set[KubernetesNamespaceName],
                             labels: LabelMap,
                             nodepools: Set[Nodepool],
                             //TODO: populate this
                             errors: List[RuntimeError]) {

  def getGkeClusterId: KubernetesClusterId = KubernetesClusterId(googleProject, location, clusterName)
}

final case class KubernetesClusterSamResourceId(resourceId: String) extends AnyVal

final case class KubernetesClusterAsyncFields(apiServerIp: KubernetesApiServerIp, networkInfo: NetworkFields)

final case class NetworkFields(networkName: NetworkName, subNetworkName: SubnetworkName, subNetworkIpRange: IpRange)

/** Google Container Cluster statuses
 *  see: https://cloud.google.com/kubernetes-engine/docs/reference/rest/v1/projects.locations.clusters#Cluster.Status
 */
sealed abstract class KubernetesClusterStatus
object KubernetesClusterStatus {
  final case object StatusUnspecified extends KubernetesClusterStatus {
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

  //in kubernetes statuses, they label "deleting' resources as 'stopping'
  final case object Deleting extends KubernetesClusterStatus {
    override def toString: String = "STOPPING"
  }

  final case object Error extends KubernetesClusterStatus {
    override def toString: String = "ERROR"
  }

  final case object Degraded extends KubernetesClusterStatus {
    override def toString: String = "DEGRADED"
  }

  //this is a custom status, and will not be returned from google
  final case object Deleted extends KubernetesClusterStatus {
    override def toString: String = "DELETED"
  }

  val deleted: KubernetesClusterStatus = Deleted
  def values: Set[KubernetesClusterStatus] = sealerate.values[KubernetesClusterStatus]
  def stringToObject: Map[String, KubernetesClusterStatus] = values.map(v => v.toString -> v).toMap
}

/** Google Container Nodepool statuses
 * See https://googleapis.github.io/googleapis/java/all/latest/apidocs/com/google/container/v1/NodePool.Status.html
 */
sealed abstract class NodepoolStatus
object NodepoolStatus {
  case object StatusUnspecified extends NodepoolStatus {
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

  //in kubernetes statuses, they label "deleting' resources as 'stopping'
  case object Deleting extends NodepoolStatus {
    override def toString: String = "STOPPING"
  }

  case object Error extends NodepoolStatus {
    override def toString: String = "ERROR"
  }

  //this is a custom status, and will not be returned from google
  case object Deleted extends NodepoolStatus {
    override def toString: String = "DELETED"
  }

  case object RunningWithError extends NodepoolStatus {
    override def toString: String = "RUNNING_WITH_ERROR"
  }

  def values: Set[NodepoolStatus] = sealerate.values[NodepoolStatus]
  def stringToObject: Map[String, NodepoolStatus] = values.map(v => v.toString -> v).toMap
}

final case class KubernetesClusterLeoId(id: Long) extends AnyVal
final case class KubernetesNamespaceId(id: Long) extends AnyVal

final case class Nodepool(id: NodepoolLeoId,
                          clusterId: KubernetesClusterLeoId,
                          nodepoolName: NodepoolName,
                          status: NodepoolStatus,
                          auditInfo: AuditInfo,
                          machineType: MachineTypeName,
                          numNodes: NumNodes,
                          autoScalingEnabled: Boolean,
                          autoscalingConfig: Option[NodepoolAutoscaling],
                          errors: List[RuntimeError])

final case class NodepoolAutoscaling(autoScalingMin: AutoScalingMin, autoScalingMax: AutoScalingMax)

final case class NodepoolLeoId(id: Long) extends AnyVal
final case class NumNodes(amount: Int) extends AnyVal
final case class AutoScalingMin(amount: Int) extends AnyVal
final case class AutoScalingMax(amount: Int) extends AnyVal
