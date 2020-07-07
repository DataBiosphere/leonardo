package org.broadinstitute.dsde.workbench.leonardo.db

import org.scalatest.FlatSpecLike
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData._
import org.broadinstitute.dsde.workbench.leonardo.TestUtils._

import scala.concurrent.ExecutionContext.Implicits.global

class ServiceComponentSpec extends FlatSpecLike with TestComponent {

  it should "save a record" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val savedApp1 = makeApp(1, savedNodepool1.id).save()

    val service1 = makeService(1)
    val serviceToSave = service1.copy(config = service1.config)
    val savedService1 = dbFutureValue(serviceQuery.saveForApp(savedApp1.id, serviceToSave))

    serviceToSave shouldEqual savedService1
  }

}
