package org.broadinstitute.dsde.workbench.leonardo.http

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import org.typelevel.log4cats.StructuredLogger
import akka.actor.ActorSystem
import org.broadinstitute.dsde.workbench.leonardo.config.Config.applicationConfig
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.broadinstitute.dsde.workbench.util2.messaging.CloudSubscriber

import scala.concurrent.ExecutionContext

class BaselineDependyBuilderSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  it should "create gcp specific interpreters when azure hosting mode is false" in {

    implicit val dbRef = mock[DbReference[IO]]
    implicit val logger = mock[StructuredLogger[IO]]
    implicit val exContext = mock[ExecutionContext]
    implicit val system = ActorSystem(applicationConfig.applicationName)
    implicit val openTelemetryMetrics = mock[OpenTelemetryMetrics[IO]]

    val dependyBuilder = BaselineDependyBuilder()
    val dependenciesResource = dependyBuilder.createBaselineDependencies[IO]()

    val serviceTypeCheck: IO[Boolean] = dependenciesResource.use { deps =>
      IO(deps.subscriber.isInstanceOf[CloudSubscriber[IO, LeoPubsubMessage]])
    }

    val result = serviceTypeCheck.unsafeRunSync()
    result should be(true)
  }
}
