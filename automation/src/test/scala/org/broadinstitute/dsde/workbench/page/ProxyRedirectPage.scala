package org.broadinstitute.dsde.workbench.page

import cats.effect.{IO, Timer}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.ProxyRedirectClient
import org.broadinstitute.dsde.workbench.service.test.WebBrowserUtil
import org.openqa.selenium.WebDriver
import org.scalatestplus.selenium.Page

import scala.concurrent.duration._

trait ProxyRedirectPage[P <: Page] extends Page with PageUtil[P] with WebBrowserUtil with LazyLogging { self: P =>
  implicit val authToken: AuthToken
  implicit val timer: Timer[IO]

  override def open(implicit webDriver: WebDriver): P = {
    val res = for {
      // Go to the target page. This will initially return 401.
      _ <- IO(go.to(this))

      // Set the LeoToken cookie. Note this needs to be done from the same domain as the target page.
      _ <- IO(addCookie("LeoToken", authToken.value))

      proxyRedirectPage = ProxyRedirectClient.get(url)

      // Go to the proxy redirect page, specifying the target page as the `rurl`. This will automatically
      // redirect to the target page. This is done so the Referer is set correctly.
      redirect = for {
        _ <- IO(go.to(proxyRedirectPage))
        res <- IO(awaitLoaded())
      } yield res

      // Retry above operation
      res <- fs2.Stream
        .retry(
          redirect,
          2 seconds,
          _ * 2,
          3
        )
        .compile
        .lastOrError
    } yield res

    res.unsafeRunSync()
  }

}
