package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.azure.{AzureCloudContext, ManagedResourceGroupName, SubscriptionId, TenantId}
import org.broadinstitute.dsde.workbench.google2.GKEModels.{KubernetesClusterName, NodepoolName}
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{
  NamespaceName,
  ServiceAccountName,
  ServiceName
}
import org.broadinstitute.dsde.workbench.google2.{Location, MachineTypeName, RegionName}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.AppSamResourceId
import org.broadinstitute.dsde.workbench.leonardo.dao.CustomAppService
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.dummyDate
import org.broadinstitute.dsde.workbench.model.IP
import org.broadinstitute.dsde.workbench.leonardo.http.{
  CreateAppRequest,
  GetAppResponse,
  GetAppResult,
  ListAppResponse,
  PersistentDiskRequest
}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsp.{ChartName, ChartVersion, Release}

import java.util.UUID

object KubernetesTestData {
  val kubeName0 = KubernetesClusterName("clustername00")
  val kubeName1 = KubernetesClusterName("clustername01")

  val appSamId = AppSamResourceId("067e2867-5d4a-47f3-a53c-fd711529b289", None)
  val sharedAppSamId =
    AppSamResourceId("067e2867-5d4a-47f3-a53c-fd711529b290", Some(AppAccessScope.WorkspaceShared))
  val location = Location("us-central1-a")
  val region = RegionName("us-central1")

  val loadBalancerIp = IP("0.0.0.0")
  val apiServerIp = IP("1.2.3.4")
  val namespace0 = NamespaceName("namespace00")
  val namespace1 = NamespaceName("namespace01")

  val nodepoolName0 = NodepoolName("nodepoolname00")
  val nodepoolName1 = NodepoolName("nodepoolname01")

  val autoscalingConfig = AutoscalingConfig(AutoscalingMin(1), AutoscalingMax(2))

  val galaxyApp = AppType.Galaxy

  val galaxyChartName = ChartName("/leonardo/galaxykubeman")
  val galaxyChartVersion = ChartVersion("2.9.0")
  val galaxyChart = Chart(galaxyChartName, galaxyChartVersion)

  val galaxyReleasePrefix = "gxy-release"

  val ingressChartName = ChartName("stable/nginx-ingress")
  val ingressChartVersion = ChartVersion("1.41.3")
  val ingressChart = Chart(ingressChartName, ingressChartVersion)

  val coaChartName = ChartName("cromwell-helm/cromwell-on-azure")
  val coaChartVersion = ChartVersion("0.2.494")

  val coaChart = Chart(coaChartName, coaChartVersion)

  val serviceKind = KubernetesServiceKindName("ClusterIP")

  val kubernetesRuntimeConfig = KubernetesRuntimeConfig(
    NumNodes(2),
    MachineTypeName("n1-standard-4"),
    true
  )

  val createAppRequest = CreateAppRequest(
    Some(kubernetesRuntimeConfig),
    AppType.Galaxy,
    None,
    None,
    None,
    Map.empty,
    Map.empty,
    None,
    List.empty,
    None,
    None,
    None,
    None
  )

  val customEnvironmentVariables =
    Map("WORKSPACE_NAME" -> "testWorkspace", "RELAY_HYBRID_CONNECTION_NAME" -> s"app1-${workspaceId.value}")

  val testCluster = makeKubeCluster(1)
  val testNodepool = makeNodepool(1, testCluster.id)
  val testApp = makeApp(1, testNodepool.id)

  val getAppResponse =
    GetAppResponse.fromDbResult(
      GetAppResult(
        testCluster,
        testNodepool,
        testApp
      ),
      "https://leo/proxy/"
    )

  val listAppResponse =
    ListAppResponse
      .fromCluster(testCluster.copy(nodepools = List(testNodepool.copy(apps = List(testApp)))),
                   "https://leo/proxy/",
                   List.empty
      )
      .toVector

  def cromwellAppCreateRequest(diskConfig: Option[PersistentDiskRequest],
                               customEnvVars: Map[String, String] = customEnvironmentVariables
  ) =
    CreateAppRequest(
      kubernetesRuntimeConfig = None,
      appType = AppType.Cromwell,
      None,
      None,
      diskConfig = diskConfig,
      labels = Map.empty,
      customEnvironmentVariables = customEnvVars,
      descriptorPath = None,
      extraArgs = List.empty,
      None,
      None,
      None,
      None
    )

  def makeNodepool(index: Int,
                   clusterId: KubernetesClusterLeoId,
                   prefix: String = "",
                   isDefault: Boolean = false,
                   status: NodepoolStatus = NodepoolStatus.Unspecified
  ) = {
    val name = NodepoolName(prefix + "nodepoolname" + index)
    Nodepool(
      NodepoolLeoId(-1), // will be replaced
      clusterId,
      name,
      status,
      auditInfo.copy(dateAccessed = dummyDate),
      MachineTypeName("n1-standard-4"),
      NumNodes(if (isDefault) 1 else 2),
      !isDefault,
      if (isDefault) None else Some(AutoscalingConfig(AutoscalingMin(0), AutoscalingMax(2))),
      List(),
      isDefault
    )
  }

  def makeKubeCluster(index: Int,
                      withDefaultNodepool: Boolean = true,
                      status: KubernetesClusterStatus = KubernetesClusterStatus.Unspecified
  ): KubernetesCluster = {
    val name = KubernetesClusterName("kubecluster" + index)
    val uniqueCloudContextGcp = CloudContext.Gcp(GoogleProject(project.value + index))
    KubernetesCluster(
      KubernetesClusterLeoId(-1),
      uniqueCloudContextGcp,
      name,
      location,
      region,
      status,
      ingressChart,
      auditInfo,
      None,
      List(makeNodepool(index, KubernetesClusterLeoId(-1), "cluster", withDefaultNodepool))
    )
  }

  def makeAzureCluster(index: Int,
                       withDefaultNodepool: Boolean = true,
                       status: KubernetesClusterStatus = KubernetesClusterStatus.Unspecified
  ): KubernetesCluster = {
    val name = KubernetesClusterName("kubecluster" + index)
    val uniqueCloudContextAzure = CloudContext.Azure(
      AzureCloudContext(tenantId = TenantId("tenant-id" + index),
                        subscriptionId = SubscriptionId("sub-id"),
                        managedResourceGroupName = ManagedResourceGroupName("mrg-name")
      )
    )
    KubernetesCluster(
      KubernetesClusterLeoId(-1),
      uniqueCloudContextAzure,
      name,
      location,
      region,
      status,
      ingressChart,
      auditInfo,
      None,
      List(makeNodepool(index, KubernetesClusterLeoId(-1), "cluster", withDefaultNodepool))
    )
  }

  def makeNamespace(index: Int, prefix: String = ""): NamespaceName = NamespaceName(prefix + "namespace" + index)

  def makeApp(index: Int,
              nodepoolId: NodepoolLeoId,
              customEnvironmentVariables: Map[String, String] = customEnvironmentVariables,
              status: AppStatus = AppStatus.Unspecified,
              appType: AppType = galaxyApp,
              workspaceId: WorkspaceId = WorkspaceId(UUID.randomUUID()),
              appAccessScope: AppAccessScope = AppAccessScope.UserPrivate,
              chart: Chart = galaxyChart,
              releasePrefix: String = galaxyReleasePrefix,
              disk: Option[PersistentDisk] = None,
              kubernetesServiceAccountName: Option[ServiceAccountName] = None,
              autodeleteThreshold: Int = 0,
              autodeleteEnabled: Boolean = false
  ): App = {
    val name = AppName("app" + index)
    val namespace = makeNamespace(index, "app")
    val samId = appAccessScope match {
      case AppAccessScope.WorkspaceShared => sharedAppSamId
      case _                              => appSamId
    }
    App(
      AppId(-1),
      nodepoolId,
      appType,
      name,
      None,
      Some(workspaceId),
      status,
      chart,
      Release(releasePrefix + index),
      samId,
      serviceAccountEmail,
      auditInfo,
      Map.empty,
      AppResources(
        namespace,
        disk,
        List.empty,
        kubernetesServiceAccountName
      ),
      List.empty,
      customEnvironmentVariables,
      None,
      List.empty,
      None,
      Some(1),
      Some(autodeleteThreshold),
      autodeleteEnabled
    )
  }

  def makeService(index: Int): KubernetesService = {
    val name = ServiceName("service" + index)
    KubernetesService(
      ServiceId(-1),
      ServiceConfig(name, serviceKind)
    )
  }

  def makeCustomAppService(): CustomAppService =
    CustomAppService(
      ContainerImage.fromImageUrl("us.gcr.io/anvil-gcr-public/anvil-rstudio-bioconductor:0.0.10").get,
      8001,
      "/",
      List("/bin/sh", "-c"),
      List("sed -i 's/^www-address.*$//' $RSTUDIO_HOME/rserver.conf && /init"),
      "/data",
      "ReadWriteOnce",
      Map(
        "WORKSPACE_NAME" -> "my-ws",
        "WORKSPACE_NAMESPACE" -> "my-proj",
        "USER" -> "rstudio"
      )
    )
}
