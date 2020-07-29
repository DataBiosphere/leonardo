package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.google2.GKEModels.{KubernetesClusterName, NodepoolName}
import org.broadinstitute.dsde.workbench.google2.KubernetesModels.Protocol
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{NamespaceName, ServiceName}
import org.broadinstitute.dsde.workbench.google2.{Location, MachineTypeName}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.db.GetAppResult
import org.broadinstitute.dsde.workbench.leonardo.http.service.{CreateAppRequest, GetAppResponse, ListAppResponse}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

object KubernetesTestData {
  val kubeName0 = KubernetesClusterName("clustername00")
  val kubeName1 = KubernetesClusterName("clustername01")

  val appSamId = AppSamResourceId("067e2867-5d4a-47f3-a53c-fd711529b289")
  val location = Location("us-central1-a")

  val externalIp = IP("0.0.0.0")
  val namespace0 = NamespaceName("namespace00")
  val namespace1 = NamespaceName("namespace01")

  val nodepoolName0 = NodepoolName("nodepoolname00")
  val nodepoolName1 = NodepoolName("nodepoolname01")

  val autoscalingConfig = AutoscalingConfig(AutoscalingMin(1), AutoscalingMax(2))

  val galaxyApp = AppType.Galaxy

  val serviceKind = KubernetesServiceKindName("ClusterIP")
  val protocol = Protocol("TCP")

  val kubernetesRuntimeConfig = KubernetesRuntimeConfig(
    NumNodes(2),
    MachineTypeName("n1-standard-4"),
    true
  )

  val createAppRequest = CreateAppRequest(
    Some(kubernetesRuntimeConfig),
    AppType.Galaxy,
    None,
    Map.empty,
    Map.empty
  )

  val testCluster = makeKubeCluster(1)
  val testNodepool = makeNodepool(1, testCluster.id)
  val testApp = makeApp(1, testNodepool.id)

  val getAppResponse =
    GetAppResponse.fromDbResult(
      GetAppResult(
        testCluster,
        testNodepool,
        testApp
      )
    )

  val listAppResponse =
    ListAppResponse.fromCluster(testCluster.copy(nodepools = List(testNodepool.copy(apps = List(testApp))))).toVector

  def makeNodepool(index: Int, clusterId: KubernetesClusterLeoId, prefix: String = "", isDefault: Boolean = false) = {
    val name = NodepoolName(prefix + "nodepoolname" + index)
    Nodepool(
      NodepoolLeoId(-1), //will be replaced
      clusterId,
      name,
      NodepoolStatus.Unspecified,
      auditInfo,
      MachineTypeName("n1-standard-4"),
      NumNodes(2),
      false,
      None,
      List(),
      List(),
      isDefault
    )
  }

  def makeKubeCluster(index: Int): KubernetesCluster = {
    val name = KubernetesClusterName("kubecluster" + index)
    val uniqueProject = GoogleProject(project.value + index)
    KubernetesCluster(
      KubernetesClusterLeoId(-1),
      uniqueProject,
      name,
      location,
      KubernetesClusterStatus.Unspecified,
      serviceAccountEmail,
      auditInfo,
      None,
      List(),
      List(makeNodepool(index, KubernetesClusterLeoId(-1), "cluster", true)),
      List()
    )
  }

  def makeNamespace(index: Int, prefix: String = ""): Namespace = {
    val name = NamespaceName(prefix + "namespace" + index)
    Namespace(NamespaceId(-1), name)
  }

  def makeApp(index: Int, nodepoolId: NodepoolLeoId): App = {
    val name = AppName("app" + index)
    val namespace = makeNamespace(index, "app")
    App(AppId(-1),
        nodepoolId,
        galaxyApp,
        name,
        AppStatus.Unspecified,
        appSamId,
        auditInfo,
        Map.empty,
        AppResources(
          namespace,
          None,
          List.empty
        ),
        List.empty,
        Map.empty)
  }

  def makeService(index: Int): KubernetesService = {
    val name = ServiceName("service" + index)
    KubernetesService(
      ServiceId(-1),
      ServiceConfig(name, serviceKind)
    )
  }
}
