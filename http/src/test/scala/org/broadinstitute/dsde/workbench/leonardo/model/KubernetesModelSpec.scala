package org.broadinstitute.dsde.workbench.leonardo.model

import java.net.URL

import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.leonardo.{LeoLenses, LeonardoTestSuite}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData._
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._

class KubernetesModelSpec extends LeonardoTestSuite with AnyFlatSpecLike {
  "App" should "generate valid proxy urls" in {
    val services = (1 to 3).map(makeService).toList
    val app = LeoLenses.appToServices.modify(_ => services)(testApp)
    app.getProxyUrls(project, proxyUrlBase) shouldBe Map(
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

}
