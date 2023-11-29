package org.broadinstitute.dede.workbench.leonardo.provider

import akka.http.scaladsl.Http
import akka.http.scaladsl.testkit.ScalatestRouteTest
import cats.effect.IO
import cats.effect.unsafe.implicits._
import com.comcast.ip4s.{Host, Port}
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import pact4s.scalatest.PactVerifier

import java.lang.Thread.sleep
import scala.concurrent.duration.DurationInt

class ScalaTestVerifyPacts extends AnyFlatSpec with ScalatestRouteTest with BeforeAndAfterAll with PactVerifier with LazyLogging {


  override def beforeAll(): Unit = {
    startLeo.unsafeToFuture()
    startLeo.start
    sleep(5000)
  }

  def startLeo: IO[Http.ServerBinding] =
  for {
    binding <- IO
      .fromFuture(IO(Http().newServerAt("localhost",8080).bind(genLeoRoutes(genLeoDependencies, defaultLeoUser).route)))
      .onError { t: Throwable =>
        IO(logger.error("FATAL - failure starting http server", t)) *> IO.raiseError(t)
      }
    _ <- IO.fromFuture(IO(binding.whenTerminated))
    _ <- IO(system.terminate())
  } yield binding



  it should "Verify pacts" in {
    verifyPacts(
      publishVerificationResults = None,
      providerVerificationOptions = Nil,
      verificationTimeout = Some(10.seconds)
    )
  }
}
