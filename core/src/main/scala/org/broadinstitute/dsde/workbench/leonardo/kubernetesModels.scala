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
                             location: Location,
                             status: KubernetesClusterStatus,
                             serviceAccountInfo: WorkbenchEmail,
                             samResourceId: KubernetesClusterSamResource,
                             auditInfo: AuditInfo,
                             asyncFields: Option[KubernetesClusterAsyncFields],
                             namespaces: Set[KubernetesNamespaceName],
                             labels: LabelMap,
                             nodepools: Set[Nodepool]
                            )

object KubernetesCluster {
  implicit class EnrichedKubernetesCluster(cluster: KubernetesCluster) {
    def getGkeClusterId: KubernetesClusterId = KubernetesClusterId(cluster.googleProject, cluster.location, cluster.clusterName)
  }
}

final case class KubernetesClusterSamResource(resourceId: String)

final case class KubernetesClusterAsyncFields(apiServerIp: KubernetesApiServerIp,
                                        networkInfo: NetworkFields
                                       )

final case class NetworkFields(networkName: NetworkName,
                         subNetworkName: SubnetworkName,
                         subNetworkIpRange: IpRange)


/** Google Container Cluster statuses
 *  see: https://cloud.google.com/kubernetes-engine/docs/reference/rest/v1/projects.locations.clusters#Cluster.Status
 */

sealed abstract class KubernetesClusterStatus
object KubernetesClusterStatus {
  case object StatusUnspecified extends KubernetesClusterStatus {
    override def toString: String = "STATUS_UNSPECIFIED"
  }

  case object Provisioning extends KubernetesClusterStatus {
    override def toString: String = "PROVISIONING"
  }

  case object Running extends KubernetesClusterStatus {
    override def toString: String = "RUNNING"
  }

  case object Reconciling extends KubernetesClusterStatus {
    override def toString: String = "RECONCILING"
  }

  case object Stopping extends KubernetesClusterStatus {
    override def toString: String = "STOPPING"
  }

  case object Error extends KubernetesClusterStatus {
    override def toString: String = "ERROR"
  }

  case object Degraded extends KubernetesClusterStatus {
    override def toString: String = "DEGRADED"
  }

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

  case object Stopping extends NodepoolStatus {
    override def toString: String = "STOPPING"
  }

  case object Error extends NodepoolStatus {
    override def toString: String = "ERROR"
  }

  case object RunningWithError extends NodepoolStatus {
    override def toString: String = "RUNNING_WITH_ERROR"
  }

  def values: Set[NodepoolStatus] = sealerate.values[NodepoolStatus]
  def stringToObject: Map[String, NodepoolStatus] = values.map(v => v.toString -> v).toMap
}

final case class KubernetesClusterLeoId(id: Long)
final case class KubernetesNamespaceId(id: Long)

final case class Nodepool(id: NodepoolLeoId,
                    clusterId: KubernetesClusterLeoId,
                    nodepoolName: NodepoolName,
                    status: NodepoolStatus,
                    auditInfo: AuditInfo,
                    machineType: MachineTypeName,
                    numNodes: NumNodes,
                    autoScalingEnabled: Boolean,
                    autoscalingConfig: Option[NodepoolAutoscaling])

final case class NodepoolAutoscaling(autoScalingMin: AutoScalingMin,
                               autoScalingMax: AutoScalingMax)

final case class NodepoolLeoId(id: Long)
final case class NumNodes(amount: Int)
final case class AutoScalingMin(amount: Int)
final case class AutoScalingMax(amount: Int)

