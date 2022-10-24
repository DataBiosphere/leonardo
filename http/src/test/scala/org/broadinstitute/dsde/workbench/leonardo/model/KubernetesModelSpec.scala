package org.broadinstitute.dsde.workbench.leonardo.model

import java.net.URL
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.leonardo.{Chart, CloudContext, LeoLenses, LeonardoTestSuite}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData._
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsp.{ChartName, ChartVersion}

class KubernetesModelSpec extends LeonardoTestSuite with AnyFlatSpecLike {
  "App" should "generate valid proxy urls" in {
    val services = (1 to 3).map(makeService).toList
    val app = LeoLenses.appToServices.modify(_ => services)(testApp)
    app.getProxyUrls(CloudContext.Gcp(project), None, proxyUrlBase, "v1") shouldBe Map(
      ServiceName("service1") -> new URL(
        s"https://leo/proxy/google/v1/apps/${project.value}/${app.appName.value}/service1"
      ),
      ServiceName("service2") -> new URL(
        s"https://leo/proxy/google/v1/apps/${project.value}/${app.appName.value}/service2"
      ),
      ServiceName("service3") -> new URL(
        s"https://leo/proxy/google/v1/apps/${project.value}/${app.appName.value}/service3"
      )
    )
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

  // TODO: Add tests for V2 proxy URLs.
}
