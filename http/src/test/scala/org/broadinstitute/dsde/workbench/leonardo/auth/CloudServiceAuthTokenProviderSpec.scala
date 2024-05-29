package org.broadinstitute.dsde.workbench.leonardo.auth
import cats.effect.unsafe.implicits.global
import cats.effect.{Async, IO}
import org.broadinstitute.dsde.workbench.leonardo.{CloudProvider, LeonardoTestSuite}
import org.http4s.Credentials
import org.http4s.headers.Authorization
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar

import scala.collection.mutable

class CloudServiceAuthTokenProviderSpec
    extends AnyFlatSpec
    with LeonardoTestSuite
    with BeforeAndAfter
    with MockitoSugar {

  val firstTokenValue = "token1"
  val secondTokenValue = "token2"
  val provider: CloudProvider = CloudProvider.Gcp
  var tokens: mutable.Stack[CloudToken] = _

  before {
    tokens = new mutable.Stack[CloudToken]

    val result = for {
      now <- IO.realTimeInstant
    } yield {
      // set secondTokenValue to greater than the tokenCacheTimeInMilli which is 30 minutes
      tokens.push(CloudToken(secondTokenValue, now.plusSeconds(40 * 60)))
      tokens.push(CloudToken(firstTokenValue, now.plusSeconds(2)))
    }

    result.unsafeRunSync()
  }

  it should "get a new token if call for the first time" in {

    val testProvider = new TestCloudServiceAuthTokenProvider[IO](provider, tokens)

    val result = getTokenAndAssertResult(testProvider, firstTokenValue)

    assertTokenInResultIsValid(result)

  }

  it should "get a new token if the first token is expired" in {

    val testProvider = new TestCloudServiceAuthTokenProvider[IO](provider, tokens)

    val result = getTokenAndAssertResult(testProvider, firstTokenValue)

    assertTokenInResultIsValid(result)

    // wait for the first token to expire
    Thread.sleep(3 * 1000)

    val result2 = getTokenAndAssertResult(testProvider, secondTokenValue)

    assertTokenInResultIsValid(result2)
  }

  it should "return the same token if the token is not expired" in {

    val testProvider = new TestCloudServiceAuthTokenProvider[IO](provider, tokens)

    val result = getTokenAndAssertResult(testProvider, firstTokenValue)

    assertTokenInResultIsValid(result)

    // wait for the first token to expire
    Thread.sleep(3 * 1000)

    val result2 = getTokenAndAssertResult(testProvider, secondTokenValue)

    assertTokenInResultIsValid(result2)

    val result3 = getTokenAndAssertResult(testProvider, secondTokenValue)

    assertTokenInResultIsValid(result3)
  }

  private def getTokenAndAssertResult(testProvider: TestCloudServiceAuthTokenProvider[IO],
                                      expectedTokenValue: String
  ) = {
    val result2 = testProvider.getAuthToken
      .map {
        case Authorization(Credentials.Token(_, value)) => value == expectedTokenValue
        case _                                          => false
      }
      .attempt
      .unsafeRunSync()
    result2
  }

  private def assertTokenInResultIsValid(result: Either[Throwable, Boolean]) =
    result match {
      case Left(error)         => fail(s"Unexpected error: $error")
      case Right(isTokenValid) => assert(isTokenValid)
    }
}

/**
 * A test CloudServiceAuthTokenProvider that returns a fixed token.
 */
class TestCloudServiceAuthTokenProvider[F[_]](provider: CloudProvider, cloudTokens: mutable.Stack[CloudToken])(implicit
  F: Async[F]
) extends CloudServiceAuthTokenProvider[F](provider) {
  override def getCloudProviderAuthToken: F[CloudToken] = {
    val token = cloudTokens.pop()
    F.pure(token)
  }
}
