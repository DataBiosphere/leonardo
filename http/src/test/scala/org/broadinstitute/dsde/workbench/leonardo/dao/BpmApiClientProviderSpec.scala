package org.broadinstitute.dsde.workbench.leonardo.dao

import bio.terra.profile.api.ProfileApi
import bio.terra.profile.model.{Organization, ProfileModel}
import cats.effect.IO
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.tokenValue
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, LeonardoTestSuite}
import org.http4s._
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import scala.jdk.CollectionConverters._

import java.util.UUID

class BpmApiClientProviderSpec extends AnyFlatSpec with LeonardoTestSuite with BeforeAndAfterAll with MockitoSugar {

  val profileId = UUID.randomUUID()

  def newBpmProvider() =
    new HttpBpmClientProvider[IO](baseBpmUrl = Uri.unsafeFromString("test")) {
      override def getProfileApi(token: String)(implicit
        ev: Ask[IO, AppContext]
      ): IO[ProfileApi] = IO.pure(setUpMockProfileApi)
    }

  val bpmProvider = newBpmProvider()

  it should "return a profile" in {
    val res = for {
      bp <- bpmProvider.getProfile(tokenValue, profileId)
    } yield bp
  }

  private def setUpMockProfileApi: ProfileApi = {
    val api = mock[ProfileApi]
    when {
      api.getProfile(profileId)
    } thenAnswer { _ =>
      new ProfileModel().id(profileId).organization(new Organization().limits(Map("autopause" -> "30").asJava))
    }
    api
  }
}
