package org.broadinstitute.dsde.workbench.page

import cats.effect.IO
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.{LeonardoApiClient, ProxyRedirectClient}
import org.broadinstitute.dsde.workbench.service.test.WebBrowserUtil
import org.openqa.selenium.WebDriver
import org.scalatestplus.selenium.Page

import scala.concurrent.duration._

trait ProxyRedirectPage[P <: Page] extends Page with PageUtil[P] with WebBrowserUtil with LazyLogging { self: P =>
  implicit val authToken: AuthToken

  override def open(implicit webDriver: WebDriver): P = {
    val res = for {
      // Go to the target page. This will initially return 401.
      _ <- IO(go.to(this))

      // Set the LeoToken cookie. Note this needs to be done from the same domain as the target page.
      _ <- IO(logger.info(s"Setting LeoToken cookie for page ${url}"))
      _ <- IO(addCookie("LeoToken", authToken.value))

      // Test connection to the proxy redirect server before loading it in WebDriver
      proxyRedirectUrl <- ProxyRedirectClient.get(url).map(_.renderString)
      res <- LeonardoApiClient.client.use { implicit client =>
        for {
          _ <- fs2.Stream
            .retry(
              IO(logger.info(s"Testing connection to ${proxyRedirectUrl}")) >> ProxyRedirectClient.testConnection(url),
              2 seconds,
              _ * 2,
              5
            )
            .compile
            .lastOrError
          _ <- IO(logger.info(s"Proxy redirect server is up at ${proxyRedirectUrl}"))

          // Go to the proxy redirect page, specifying the target page as the `rurl`. This will automatically
          // redirect to the target page. This is done so the Referer is set correctly.
          redirect = for {
            _ <- IO(logger.info(s"Going to redirect page at url ${proxyRedirectUrl}"))
            _ <- IO(go.to(proxyRedirectUrl))
            _ <- IO(logger.info(s"Waiting for redirect page at url ${proxyRedirectUrl} to redirect"))
            res <- IO(awaitLoaded())
            _ <- IO(logger.info(s"Successfully redirected to ${url}"))
          } yield res

          // Retry above operation
          r <- fs2.Stream
            .retry(
              redirect,
              2 seconds,
              _ * 2,
              3
            )
            .compile
            .lastOrError
        } yield r
      }

    } yield res

    res.unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }

}
