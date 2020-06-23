package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.google2.GKEModels.{KubernetesClusterName, NodepoolName}
import org.broadinstitute.dsde.workbench.google2.KubernetesModels.{
  KubernetesApiServerIp,
  PortName,
  PortNum,
  Protocol,
  ServicePort,
  TargetPortNum
}
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{NamespaceName, ServiceName}
import org.broadinstitute.dsde.workbench.google2.{Location, MachineTypeName}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.SamResource.AppSamResource
import org.broadinstitute.dsde.workbench.leonardo.db.GetAppResult
import org.broadinstitute.dsde.workbench.leonardo.http.service.{CreateAppRequest, GetAppResponse, ListAppResponse}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.util.Random

object KubernetesTestData {
  val kubeName0 = KubernetesClusterName("clustername00")
  val kubeName1 = KubernetesClusterName("clustername01")

  val appSamId = AppSamResource("067e2867-5d4a-47f3-a53c-fd711529b289")
  val location = Location("us-central1-a")

  val apiServerIp = KubernetesApiServerIp("0.0.0.0")
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

  def makeNodepool(index: Int, clusterId: KubernetesClusterLeoId, prefix: String = "") = {
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
      List()
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
      List(makeNodepool(index, KubernetesClusterLeoId(-1), "cluster")),
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
        List.empty)
  }

  def makeService(index: Int): KubernetesService = {
    val name = ServiceName("service" + index)
    KubernetesService(
      ServiceId(-1),
      ServiceConfig(name, serviceKind, makeRandomPort(index.toString))
    )
  }

  def makeRandomPort(suffix: String): ServicePort = {
    val name = PortName("port" + suffix)
      ServicePort(
        PortNum(Random.nextInt() % 65535),
        name,
        TargetPortNum(Random.nextInt() % 65535),
        protocol
      )
  }
}
