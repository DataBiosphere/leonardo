package org.broadinstitute.dsde.workbench.leonardo.dao

import bio.terra.profile.api.ProfileApi
import bio.terra.profile.model.{Organization, ProfileModel}
import cats.effect.IO
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.tokenValue
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.LeonardoTestSuite
import org.http4s._
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import scala.jdk.CollectionConverters._
import cats.effect.unsafe.implicits.global
import java.util.UUID

class BpmApiClientProviderSpec extends AnyFlatSpec with LeonardoTestSuite with BeforeAndAfterAll with MockitoSugar {

  val profileId = UUID.randomUUID()

  def newBpmProvider() =
    new HttpBpmClientProvider[IO](baseBpmUrl = Uri.unsafeFromString("test")) {
      override def getProfileApi(token: String): IO[ProfileApi] = IO.pure(setUpMockProfileApi)
    }

  val bpmProvider = newBpmProvider()

  it should "return a profile" in {
    val resIO = bpmProvider.getProfile(tokenValue, profileId)
    val res = resIO.unsafeRunSync()
    res.get.getId shouldBe profileId
    res.get.getOrganization.getLimits shouldBe Map("autopause" -> "30").asJava
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
