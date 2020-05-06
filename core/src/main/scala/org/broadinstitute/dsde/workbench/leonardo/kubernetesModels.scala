package org.broadinstitute.dsde.workbench.leonardo

import enumeratum.{Enum, EnumEntry}
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
                             asyncFields: KubernetesClusterAsyncFields,
                             namespaces: Set[KubernetesNamespaceName],
                             labels: LabelMap,
                             nodepools: Set[Nodepool]
                            )

object KubernetesCluster {

  implicit class EnrichedKubernetesCluster(cluster: KubernetesCluster) {
    def getGKEClusterId: KubernetesClusterId = KubernetesClusterId(cluster.googleProject, cluster.location, cluster.clusterName)
  }

}

case class KubernetesClusterSamResource(resourceId: String)

case class KubernetesClusterAsyncFields(apiServerIp: Option[KubernetesApiServerIp],
                                        networkInfo: Option[NetworkFields]
                                       )

case class NetworkFields(networkName: NetworkName,
                         subNetworkName: SubnetworkName,
                         subNetworkIpRange: IpRange)


/** Google Container Cluster statuses
 *  see: https://cloud.google.com/kubernetes-engine/docs/reference/rest/v1/projects.locations.clusters#Cluster.Status
 */

//TODO: possibly consolidate Kubernetes status when monitoring is implemented
sealed trait KubernetesClusterStatus extends EnumEntry with Product with Serializable
object KubernetesClusterStatus extends Enum[KubernetesClusterStatus] {
  val values = findValues

  // NOTE: Remember to update the definition of this enum in Swagger when you add new ones
  case object Status_Unspecified extends KubernetesClusterStatus
  case object Provisioning extends KubernetesClusterStatus
  case object Running extends KubernetesClusterStatus
  case object Reconciling extends KubernetesClusterStatus
  case object Stopping extends KubernetesClusterStatus
  case object Error extends KubernetesClusterStatus
  case object Degraded extends KubernetesClusterStatus
}

/** Google Container Nodepool statuses
 * See https://googleapis.github.io/googleapis/java/all/latest/apidocs/com/google/container/v1/NodePool.Status.html
 */

//TODO: possibly consolidate Kubernetes status when monitoring is implemented
sealed trait NodepoolStatus extends EnumEntry with Product with Serializable
object NodepoolStatus extends Enum[NodepoolStatus] {
  val values = findValues

  // NOTE: Remember to update the definition of this enum in Swagger when you add new ones
  case object Status_Unspecified extends NodepoolStatus
  case object Provisioning extends NodepoolStatus
  case object Running extends NodepoolStatus
  case object Running_with_Error extends NodepoolStatus //consistent with Degraded from KubernetesClusterStatus
  case object Reconciling extends NodepoolStatus
  case object Stopping extends NodepoolStatus
  case object Error extends NodepoolStatus
}

case class KubernetesClusterLeoId(id: Long)
case class KubernetesNamespaceId(id: Long)

case class Nodepool(id: NodepoolLeoId,
                    clusterId: KubernetesClusterLeoId,
                    nodepoolName: NodepoolName,
                    status: NodepoolStatus,
                    auditInfo: AuditInfo,
                    machineType: MachineTypeName,
                    numNodes: NumNodes,
                    autoScalingEnabled: Boolean,
                    autoscalingConfig: Option[NodepoolAutoscaling])

case class NodepoolAutoscaling(autoScalingMin: AutoScalingMin,
                               autoScalingMax: AutoScalingMax)

case class NodepoolLeoId(id: Long)
case class NumNodes(amount: Int)
case class AutoScalingMin(amount: Int)
case class AutoScalingMax(amount: Int)

