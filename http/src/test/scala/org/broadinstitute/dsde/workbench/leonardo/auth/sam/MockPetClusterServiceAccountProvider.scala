package org.broadinstitute.dsde.workbench.leonardo.auth.sam

import java.util.UUID

import cats.effect.IO
import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.leonardo.dao.MockSamDAO
import org.broadinstitute.dsde.workbench.leonardo.model.ServiceAccountProvider
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.http4s.headers.Authorization
import org.http4s.{AuthScheme, Credentials}

/**
 * Created by rtitle on 12/11/17.
 */
class MockPetClusterServiceAccountProvider extends ServiceAccountProvider[IO] {
  val samDao = new MockSamDAO
  override def getClusterServiceAccount(userInfo: UserInfo, googleProject: GoogleProject)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Option[WorkbenchEmail]] = {
    val auth = Authorization(Credentials.Token(AuthScheme.Bearer, s"TokenFor${userInfo.userEmail}"))

    // Pretend we're asking Sam for the pet
    samDao.getPetServiceAccount(auth, googleProject)
  }

  override def getNotebookServiceAccount(userInfo: UserInfo, googleProject: GoogleProject)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Option[WorkbenchEmail]] =
    IO.pure(None)

  def listGroupsStagingBucketReaders(
    userEmail: WorkbenchEmail
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[List[WorkbenchEmail]] =
    IO.pure(List.empty[WorkbenchEmail])

  def listUsersStagingBucketReaders(userEmail: WorkbenchEmail): IO[List[WorkbenchEmail]] = {
    implicit val traceId = ApplicativeAsk.const[IO, TraceId](TraceId(UUID.randomUUID()))
    samDao.getUserProxy(userEmail).map(_.toList)
  }

  override def getAccessToken(userEmail: WorkbenchEmail, googleProject: GoogleProject)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Option[String]] =
    samDao.getCachedPetAccessToken(userEmail, googleProject)
}
