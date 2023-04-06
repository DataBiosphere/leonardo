package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, AppType}
import org.http4s.Uri
import org.http4s.headers.Authorization

trait WdsDAO[F[_]] {
  def getStatus(baseUri: Uri, authHeader: Authorization, appType: AppType)(implicit
    ev: Ask[F, AppContext]
  ): F[Boolean]
}
