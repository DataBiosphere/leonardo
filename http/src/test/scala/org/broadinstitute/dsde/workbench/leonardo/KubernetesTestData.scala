package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.google2.GKEModels.{KubernetesClusterName, NodePoolName}
import org.broadinstitute.dsde.workbench.google2.KubernetesModels.KubernetesMasterIP
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.KubernetesNamespaceName
import org.broadinstitute.dsde.workbench.google2.{Location, MachineTypeName}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._

object KubernetesTestData {
  val kubeName0 = KubernetesClusterName("clustername00")
  val kubeName1 = KubernetesClusterName("clustername01")

  val kubeClusterSamId = KubernetesClusterSamResource("067e2867-5d4a-47f3-a53c-fd711529b289")
  val location = Location("us-central1-a")

  val apiServerIp = KubernetesMasterIP("0.0.0.0")
  val namespace0 = KubernetesNamespaceName("namespace00")
  val namespace1 = KubernetesNamespaceName("namespace01")

  val nodepoolName0 = NodePoolName("nodepoolname00")
  val nodepoolName1 = NodePoolName("nodepoolname01")

  val autoscalingConfig = NodepoolAutoscaling(AutoScalingMin(0), AutoScalingMax(2))

  def makeNodepool(index: Int, clusterId: KubernetesClusterLeoId) = {
    val name = NodePoolName("nodepoolname" + index)
    Nodepool(
      NodepoolLeoId(-1), //will be replaced
      clusterId,
      name,
      NodepoolStatus.Status_Unspecified,
      auditInfo,
      MachineTypeName("n1-standard-4"),
      NumNodes(2),
      false,
      None
    )
  }

  def makeKubeCluster(index: Int): KubernetesCluster = {
    val name = KubernetesClusterName("kubecluster" + index)
    KubernetesCluster(
      KubernetesClusterLeoId(-1),
      project,
      name,
      location,
      KubernetesClusterStatus.Status_Unspecified,
      serviceAccountEmail,
      kubeClusterSamId,
      auditInfo,
      KubernetesClusterAsyncFields(None, None),
      Set(),
      Map(),
      Set(makeNodepool(index, KubernetesClusterLeoId(-1)))
    )
  }
}
