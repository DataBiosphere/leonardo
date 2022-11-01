package org.broadinstitute.dsde.workbench.leonardo.model

import org.broadinstitute.dsde.workbench.azure.{AzureCloudContext, ManagedResourceGroupName, SubscriptionId, TenantId}
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.google2.{NetworkName, SubnetworkName}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.proxyUrlBase
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.{makeKubeCluster, makeService, testApp}
import org.broadinstitute.dsde.workbench.leonardo.{
  Chart,
  CloudContext,
  IpRange,
  KubernetesClusterAsyncFields,
  LeoLenses,
  LeonardoTestSuite,
  NetworkFields
}
import org.broadinstitute.dsde.workbench.model.IP
import org.broadinstitute.dsp.{ChartName, ChartVersion}
import org.scalatest.flatspec.AnyFlatSpecLike

import java.net.URL

class KubernetesModelSpec extends LeonardoTestSuite with AnyFlatSpecLike {
  "App" should "generate valid GCP proxy urls" in {
    val services = (1 to 3).map(makeService).toList
    val cluster = makeKubeCluster(1)
    val app = LeoLenses.appToServices.modify(_ => services)(testApp)
    app.getProxyUrls(cluster, proxyUrlBase) shouldBe Map(
      ServiceName("service1") -> new URL(
        s"https://leo/proxy/google/v1/apps/${cluster.cloudContext.asString}/${app.appName.value}/service1"
      ),
      ServiceName("service2") -> new URL(
        s"https://leo/proxy/google/v1/apps/${cluster.cloudContext.asString}/${app.appName.value}/service2"
      ),
      ServiceName("service3") -> new URL(
        s"https://leo/proxy/google/v1/apps/${cluster.cloudContext.asString}/${app.appName.value}/service3"
      )
    )
  }

  it should "generate valid Azure proxy urls" in {
    val services = (1 to 3).map(makeService).toList
    val cluster = makeKubeCluster(1).copy(
      cloudContext = CloudContext.Azure(
        AzureCloudContext(TenantId("tenant"), SubscriptionId("sub"), ManagedResourceGroupName("mrg"))
      ),
      asyncFields = Some(
        KubernetesClusterAsyncFields(IP("https://relay.windows.net/"),
                                     IP("unused"),
                                     NetworkFields(NetworkName("unused"), SubnetworkName("unused"), IpRange("unused"))
        )
      )
    )
    val app = LeoLenses.appToServices.modify(_ => services)(testApp)
    app.getProxyUrls(cluster, proxyUrlBase) shouldBe Map(
      ServiceName("service1") -> new URL(
        s"https://relay.windows.net/${app.appName.value}/service1"
      ),
      ServiceName("service2") -> new URL(
        s"https://relay.windows.net/${app.appName.value}/service2"
      ),
      ServiceName("service3") -> new URL(
        s"https://relay.windows.net/${app.appName.value}/service3"
      )
    )
  }

  it should "generate not generate Azure proxy URLs if there is no relay endpoint" in {
    val services = (1 to 3).map(makeService).toList
    val cluster = makeKubeCluster(1).copy(
      cloudContext = CloudContext.Azure(
        AzureCloudContext(TenantId("tenant"), SubscriptionId("sub"), ManagedResourceGroupName("mrg"))
      )
    )
    val app = LeoLenses.appToServices.modify(_ => services)(testApp)
    app.getProxyUrls(cluster, proxyUrlBase) shouldBe Map.empty
  }

  "Chart strings" should "be parsed correctly" in {
    val validChartStr1 = "galaxy/galaxykubeman-1.2.3"
    val validChartStr2 = "stable/nginx-ingress-4.56.78"

    val invalidChartStr1 = "galaxy0.1.2"
    val invalidChartStr2 = "-7.8.9"
    val invalidChartStr3 = "galaxykubeman-1.2.3-"

    Chart.fromString(validChartStr1) shouldBe Some(Chart(ChartName("galaxy/galaxykubeman"), ChartVersion("1.2.3")))
    Chart.fromString(validChartStr2) shouldBe Some(Chart(ChartName("stable/nginx-ingress"), ChartVersion("4.56.78")))

    Chart.fromString(invalidChartStr1) shouldBe None
    Chart.fromString(invalidChartStr2) shouldBe None
    Chart.fromString(invalidChartStr3) shouldBe None
  }

//  "App" should "generate valid proxy urls for v2 api." in {
//    val services = (1 to 3).map(makeService).toList
//    val app = LeoLenses.appToServices.modify(_ => services)(testApp)
//
//    // GCP Project
//    app.getProxyUrls(CloudContext.Gcp(project), Some(workspaceId), proxyUrlBase) shouldBe Map(
//      ServiceName("service1") -> new URL(
//        s"https://leo/proxy/apps/v2/${workspaceId.toString}/${app.appName.value}/service1"
//      ),
//      ServiceName("service2") -> new URL(
//        s"https://leo/proxy/apps/v2/${workspaceId.toString}/${app.appName.value}/service2"
//      ),
//      ServiceName("service3") -> new URL(
//        s"https://leo/proxy/apps/v2/${workspaceId.toString}/${app.appName.value}/service3"
//      )
//    )
//
//    app.getProxyUrls(CloudContext.Azure(azureCloudContext), Some(workspaceId), proxyUrlBase) shouldBe Map(
//      ServiceName("service1") -> new URL(
//        s"https://leo/proxy/apps/v2/${workspaceId.toString}/${app.appName.value}/service1"
//      ),
//      ServiceName("service2") -> new URL(
//        s"https://leo/proxy/apps/v2/${workspaceId.toString}/${app.appName.value}/service2"
//      ),
//      ServiceName("service3") -> new URL(
//        s"https://leo/proxy/apps/v2/${workspaceId.toString}/${app.appName.value}/service3"
//      )
//    )
//  }
}
