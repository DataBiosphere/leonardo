package org.broadinstitute.dsde.workbench.leonardo.auth.sam

import cats.implicits._
import cats.Monad
import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.leonardo.dao.SamDAO
import org.broadinstitute.dsde.workbench.leonardo.model.ServiceAccountProvider
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}
import org.http4s.headers.Authorization
import org.http4s.{AuthScheme, Credentials}

class PetClusterServiceAccountProvider[F[_]: Monad](sam: SamDAO[F]) extends ServiceAccountProvider[F] {
  override def getClusterServiceAccount(userInfo: UserInfo, googleProject: GoogleProject)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[Option[WorkbenchEmail]] = {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    sam.getPetServiceAccount(authHeader, googleProject)
  }

  override def getNotebookServiceAccount(userInfo: UserInfo, googleProject: GoogleProject)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[Option[WorkbenchEmail]] = Monad[F].pure(None)

  override def listGroupsStagingBucketReaders(
    userEmail: WorkbenchEmail
  )(implicit ev: ApplicativeAsk[F, TraceId]): F[List[WorkbenchEmail]] =
    sam.getUserProxy(userEmail).map(_.toList)

  override def listUsersStagingBucketReaders(userEmail: WorkbenchEmail): F[List[WorkbenchEmail]] =
    Monad[F].pure(List.empty)

  override def getAccessToken(userEmail: WorkbenchEmail, googleProject: GoogleProject)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[Option[String]] = sam.getCachedPetAccessToken(userEmail, googleProject)
}
