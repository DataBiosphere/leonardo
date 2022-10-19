package org.broadinstitute.dsde.workbench.leonardo.auth

import cats.effect.IO
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.CloudContext
import org.broadinstitute.dsde.workbench.leonardo.dao.MockSamDAO
import org.broadinstitute.dsde.workbench.leonardo.model.ServiceAccountProvider
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}
import org.http4s.headers.Authorization
import org.http4s.{AuthScheme, Credentials}

import java.util.UUID

/**
 * Created by rtitle on 12/11/17.
 */
class MockPetClusterServiceAccountProvider extends ServiceAccountProvider[IO] {
  val samDao = new MockSamDAO
  override def getClusterServiceAccount(userInfo: UserInfo, cloudContext: CloudContext)(implicit
    ev: Ask[IO, TraceId]
  ): IO[Option[WorkbenchEmail]] = {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, s"TokenFor${userInfo.userEmail}"))
    // Pretend we're asking Sam for the pet
    cloudContext match {
      case CloudContext.Gcp(googleProject) => samDao.getPetServiceAccount(authHeader, googleProject)
      case CloudContext.Azure(_)           => samDao.getPetManagedIdentity(authHeader)
    }
  }

  override def getNotebookServiceAccount(userInfo: UserInfo, googleProject: GoogleProject)(implicit
    ev: Ask[IO, TraceId]
  ): IO[Option[WorkbenchEmail]] =
    IO.pure(None)

  def listGroupsStagingBucketReaders(
    userEmail: WorkbenchEmail
  )(implicit ev: Ask[IO, TraceId]): IO[List[WorkbenchEmail]] =
    IO.pure(List.empty[WorkbenchEmail])

  def listUsersStagingBucketReaders(userEmail: WorkbenchEmail): IO[List[WorkbenchEmail]] = {
    implicit val traceId = Ask.const[IO, TraceId](TraceId(UUID.randomUUID()))
    samDao.getUserProxy(userEmail).map(_.toList)
  }

  override def getAccessToken(userEmail: WorkbenchEmail, googleProject: GoogleProject)(implicit
    ev: Ask[IO, TraceId]
  ): IO[Option[String]] =
    samDao.getCachedPetAccessToken(userEmail, googleProject)
}
