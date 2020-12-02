package org.broadinstitute.dsde.workbench.leonardo
package db

import java.time.Instant

import cats.syntax.all._
import org.broadinstitute.dsde.workbench.google2.OperationName
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.mappedColumnImplicits._
import org.broadinstitute.dsde.workbench.model.IP
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import slick.dbio.DBIO

import scala.concurrent.ExecutionContext

object ZombieMonitorQueries {
  private def unconfirmedRuntimeLabels: Query[LabelTable, LabelRecord, Seq] =
    labelQuery
      .filter(_.resourceType === LabelResourceType.runtime)
      .filter(_.key === Config.zombieRuntimeMonitorConfig.deletionConfirmationLabelKey)
      .filter(_.value === "false")

  def listInactiveZombieQuery(implicit ec: ExecutionContext): DBIO[List[ZombieCandidate]] = {
    val runtimes = clusterQuery.filter(_.status === (RuntimeStatus.Deleted: RuntimeStatus))
    val unconfirmedRuntimeQuery = runtimes.join(unconfirmedRuntimeLabels).on(_.id === _.resourceId)
    val joinedQuery =
      unconfirmedRuntimeQuery.join(RuntimeConfigQueries.runtimeConfigs).on(_._1.runtimeConfigId === _.id)
    joinedQuery.result.map { rs =>
      rs.toList.map { r =>
        val runtimeRec = r._1._1
        val cloudService = r._2.runtimeConfig.cloudService
        val asyncRuntimeFields = (runtimeRec.googleId, runtimeRec.operationName, runtimeRec.stagingBucket).mapN {
          (googleId, operationName, stagingBucket) =>
            AsyncRuntimeFields(googleId,
                               OperationName(operationName),
                               GcsBucketName(stagingBucket),
                               runtimeRec.hostIp map IP)
        }
        ZombieCandidate(
          runtimeRec.id,
          runtimeRec.googleProject,
          runtimeRec.runtimeName,
          runtimeRec.status,
          runtimeRec.auditInfo.createdDate,
          asyncRuntimeFields,
          cloudService
        )
      }
    }
  }
}

final case class ZombieCandidate(id: Long,
                                 googleProject: GoogleProject,
                                 runtimeName: RuntimeName,
                                 status: RuntimeStatus,
                                 createdDate: Instant,
                                 asyncRuntimeFields: Option[AsyncRuntimeFields],
                                 cloudService: CloudService)
