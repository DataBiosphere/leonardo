package org.broadinstitute.dsde.workbench.leonardo.auth

import cats.effect.IO
import cats.mtl.ApplicativeAsk
import com.typesafe.config.Config
import org.broadinstitute.dsde.workbench.leonardo.model.ServiceAccountProvider
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}

/**
 * A ServiceAccountProvider which uses all defaults.
 * TOOD: is this still needed? Or move this to test code?
 */
class DefaultServiceAccountProvider(config: Config) extends ServiceAccountProvider[IO] {
  override def getClusterServiceAccount(userInfo: UserInfo, googleProject: GoogleProject)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Option[WorkbenchEmail]] = IO.pure(None)
  override def getNotebookServiceAccount(userInfo: UserInfo, googleProject: GoogleProject)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Option[WorkbenchEmail]] = IO.pure(None)
  override def listGroupsStagingBucketReaders(userEmail: WorkbenchEmail)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[List[WorkbenchEmail]] = IO.pure(List.empty)
  override def listUsersStagingBucketReaders(userEmail: WorkbenchEmail): IO[List[WorkbenchEmail]] = IO.pure(List.empty)
  override def getAccessToken(userEmail: WorkbenchEmail, googleProject: GoogleProject)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Option[String]] = IO.pure(None)
}
