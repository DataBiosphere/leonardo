package org.broadinstitute.dsde.workbench.leonardo.db

import org.scalatest.FlatSpecLike
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData._
import org.broadinstitute.dsde.workbench.leonardo.TestUtils._

import scala.concurrent.ExecutionContext.Implicits.global

class ServiceComponentSpec extends FlatSpecLike with TestComponent {

  it should "save with ports" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val savedApp1 = makeApp(1, savedNodepool1.id).save()
    val savedApp2 = makeApp(2, savedNodepool1.id).save()

    val service1 = makeService(1)
    val savedService1 = dbFutureValue(serviceQuery.saveForApp(savedApp1.id, service1))
    val service2 = makeService(2)
    val savedService2 = dbFutureValue(serviceQuery.saveForApp(savedApp1.id, service2))
    val service3 = makeService(3)
    val savedService3 = dbFutureValue(serviceQuery.saveForApp(savedApp2.id, service3))

    service1 shouldEqual savedService1
    service2 shouldEqual savedService2
    service3 shouldEqual savedService3
  }

  it should "save without ports" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val savedApp1 = makeApp(1, savedNodepool1.id).save()

    val service1 = makeService(1)
    val serviceToSave = service1.copy(config = service1.config.copy(ports = List()))
    val savedService1 = dbFutureValue(serviceQuery.saveForApp(savedApp1.id, serviceToSave))

    serviceToSave shouldEqual savedService1
  }

}
