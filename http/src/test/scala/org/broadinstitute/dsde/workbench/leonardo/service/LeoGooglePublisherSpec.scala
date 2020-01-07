package org.broadinstitute.dsde.workbench.leonardo.service

import akka.http.scaladsl.testkit.ScalatestRouteTest
import cats.effect.IO
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.scalatest.{FreeSpec, Matchers}

class LeoGooglePublisherSpec extends FreeSpec
with Matchers
with TestComponent
with CommonTestData
  with ScalatestRouteTest {

  implicit val cs = IO.contextShift(executor)
  implicit val timer = IO.timer(executor)
  implicit def unsafeLogger = Slf4jLogger.getLogger[IO]


//  val googlePublisher = new LeoGooglePublisher[IO](pubsubConfig)
//  val dummyMessage = StopUpdateMessage(defaultMachineConfig)
//
//  "should publish" in {
//    googlePublisher.publish(dummyMessage)
//  }

}
