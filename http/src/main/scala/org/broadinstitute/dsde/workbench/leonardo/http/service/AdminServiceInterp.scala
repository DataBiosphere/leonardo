package org.broadinstitute.dsde.workbench.leonardo.http.service

import cats.Parallel
import cats.effect.Async
import cats.effect.std.Queue
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.AppContext
import org.broadinstitute.dsde.workbench.leonardo.config.CustomAppConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.{SamDAO, WsmDao}
//import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.http.UpdateAppsRequest
import org.broadinstitute.dsde.workbench.leonardo.model.LeoAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage
import org.broadinstitute.dsde.workbench.model.UserInfo
//import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
//import org.typelevel.log4cats.StructuredLogger

//import scala.concurrent.ExecutionContext

final class AdminServiceInterp[F[_]: Parallel](authProvider: LeoAuthProvider[F],
                                               publisherQueue: Queue[F, LeoPubsubMessage],
                                               customAppConfig: CustomAppConfig,
                                               wsmDao: WsmDao[F],
                                               samDAO: SamDAO[F]
)(implicit
  F: Async[F],
//  log: StructuredLogger[F],
//  dbReference: DbReference[F],
//  metrics: OpenTelemetryMetrics[F],
//  ec: ExecutionContext
 ) extends AdminService[F] {

  def updateApps (
    userInfo: UserInfo,
    req: UpdateAppsRequest
  )(implicit as: Ask[F, AppContext]): F[Unit] = {
    F.unit
  }
}
