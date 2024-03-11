package org.broadinstitute.dsde.workbench.leonardo.util

import cats.effect.unsafe.implicits.global
import cats.effect.{Async, IO}
import org.scalatest.flatspec.AnyFlatSpecLike

class ServicesRegistrySpec extends AnyFlatSpecLike {

  it should "register a concrete implementation and look up using the instance type" in {

    val echoValue = "foo"
    val service = new EchoService(echoValue)
    val servicesRegistry = ServicesRegistry()

    servicesRegistry.register[EchoService](service)

    val result = servicesRegistry.lookup[EchoService].getOrElse(fail("service not found"))

    assertServiceIsTheExpectedInstance(echoValue, service, result)
  }

  it should "register an instance and look up using the trait" in {

    val echoValue = "foo"
    val service: TestEchoService = new EchoService(echoValue)
    val servicesRegistry = ServicesRegistry()

    servicesRegistry.register[TestEchoService](service)

    val result = servicesRegistry.lookup[TestEchoService].getOrElse(fail("service not found"))

    assertServiceIsTheExpectedInstance(echoValue, service, result)
  }

  it should "register an object and look up the instance using the trait" in {

    val echoValue = "foo"
    val service = EchoService(echoValue)
    val servicesRegistry = ServicesRegistry()

    servicesRegistry.register[TestEchoService](service)

    val result = servicesRegistry.lookup[TestEchoService].getOrElse(fail("service not found"))

    assertServiceIsTheExpectedInstance(echoValue, service, result)
  }

  it should "register async instance and look up the instance using the async trait" in {
    val echoValue = "foo"
    val service = new AsyncService[IO](echoValue)
    val servicesRegistry = ServicesRegistry()

    servicesRegistry.register[AsyncTestEchoService[IO]](service)

    val result = servicesRegistry.lookup[AsyncTestEchoService[IO]].getOrElse(fail("service not found"))

    val stringVal = result.getValue.unsafeRunSync()

    assert(stringVal == echoValue)
    assert(result.hashCode() == service.hashCode())
  }

  private def assertServiceIsTheExpectedInstance(echoValue: String,
                                                 service: TestEchoService,
                                                 result: TestEchoService
  ) = {
    assert(result.hashCode() == service.hashCode())
    assert(result.getValue == echoValue)
  }
}

private trait AsyncTestEchoService[F[_]] {
  def getValue: F[String]
}

private class AsyncService[F[_]](value: String)(implicit F: Async[F]) extends AsyncTestEchoService[F] {
  def getValue: F[String] = F.pure(value)
}

private trait TestEchoService {
  def getValue: String
}

private class EchoService(value: String) extends TestEchoService {
  def getValue: String = value
}

private object EchoService {
  def apply(value: String): EchoService = new EchoService(value)
}
