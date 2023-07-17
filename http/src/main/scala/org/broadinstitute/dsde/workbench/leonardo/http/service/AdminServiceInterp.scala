package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import cats.Parallel
import cats.effect.Async
//import cats.effect.std.Queue
import cats.mtl.Ask
import cats.syntax.all._
//import org.broadinstitute.dsde.workbench.leonardo.config.CustomAppConfig
//import org.broadinstitute.dsde.workbench.leonardo.dao.{SamDAO, WsmDao}
//import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoAuthProvider, NotAnAdminError}
//import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage
import org.broadinstitute.dsde.workbench.model.UserInfo
//import org.typelevel.log4cats.StructuredLogger

//import scala.concurrent.ExecutionContext

final class AdminServiceInterp[F[_]: Parallel](authProvider: LeoAuthProvider[F],
                                            //   publisherQueue: Queue[F, LeoPubsubMessage],
                                            //   customAppConfig: CustomAppConfig,
                                            //   wsmDao: WsmDao[F],
                                            //   samDAO: SamDAO[F]
)(implicit
  F: Async[F],
  //  log: StructuredLogger[F],
  //  dbReference: DbReference[F],
 //   ec: ExecutionContext
 ) extends AdminService[F] {

  def updateApps (
    userInfo: UserInfo,
    req: UpdateAppsRequest
  )(implicit as: Ask[F, AppContext]): F[Unit] = {
    for {
      ctx: AppContext <- as.ask
      hasPermission: Boolean <- authProvider.isAdminUser(userInfo)
      _ <- F.raiseWhen(!hasPermission)(NotAnAdminError(userInfo.userEmail, Option(ctx.traceId)))
      // TODO get latest chart version from config
    } yield ()
  }
}
