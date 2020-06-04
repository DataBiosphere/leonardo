package org.broadinstitute.dsde.workbench.leonardo

import cats.effect.IO
import org.http4s.HttpApp
import org.http4s.client.Client
import org.http4s.dsl.Http4sDsl

object FakeHttpClient extends Http4sDsl[IO] {
  val client = Client
    .fromHttpApp[IO](
//      HttpApp.liftF[IO](Ok(""))
      HttpApp.liftF[IO](Conflict(""))
    )
}
