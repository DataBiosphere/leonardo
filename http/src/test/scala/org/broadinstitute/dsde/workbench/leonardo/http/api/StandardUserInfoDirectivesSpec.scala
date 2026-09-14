package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class StandardUserInfoDirectivesSpec extends AnyFlatSpec with Matchers with ScalatestRouteTest {
  private val maxAgeRegex = "Max-Age=(-?\\d+);".r

  private val route = StandardUserInfoDirectives.requireUserInfo { userInfo =>
    CookieSupport.setTokenCookie(userInfo)(complete(StatusCodes.OK))
  }

  /** Drives the directive with the given `OIDC_CLAIM_expires_in` and returns the cookie's Max-Age. */
  private def maxAgeFor(expiresIn: Long): Long =
    Get("/").withHeaders(
      RawHeader("OIDC_access_token", "accessToken"),
      RawHeader("OIDC_CLAIM_user_id", "user1"),
      RawHeader("OIDC_CLAIM_expires_in", expiresIn.toString),
      RawHeader("OIDC_CLAIM_email", "user1@example.com")
    ) ~> route ~> check {
      status shouldEqual StatusCodes.OK
      val setCookie = header("Set-Cookie")
      setCookie shouldBe defined
      val m = maxAgeRegex.findFirstMatchIn(setCookie.get.value)
      m shouldBe defined
      m.get.group(1).toLong
    }

  // `OIDC_CLAIM_expires_in` actually carries an absolute timestamp; see StandardUserInfoDirectives.
  "requireUserInfo" should "convert an absolute expiry into a remaining-seconds Max-Age" in {
    val maxAge = maxAgeFor(Instant.now().getEpochSecond + 3600)
    maxAge should (be <= 3600L and be > 3500L)
  }

  it should "pass a genuine duration through unchanged" in {
    maxAgeFor(3600) shouldBe 3600
  }

  // The bug this guards: a raw `exp` reached Max-Age, so the cookie outlived its token by ~56 years.
  // 86400 is the max B2C token lifetime, the same threshold StandardUserInfoDirectives converts on.
  it should "never emit a timestamp-sized Max-Age" in {
    maxAgeFor(Instant.now().getEpochSecond + 3600) should be < 86400L
  }

  it should "not keep the cookie alive for an already-expired token" in {
    maxAgeFor(Instant.now().getEpochSecond - 3600) should be <= 0L
  }
}
