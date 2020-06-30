package org.broadinstitute.dsde.workbench.leonardo
package db

import java.time.Instant

import cats.data.Chain
import cats.implicits._
import org.broadinstitute.dsde.workbench.google2.OperationName
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.mappedColumnImplicits._
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import slick.dbio.DBIO

import scala.concurrent.ExecutionContext

object ZombieMonitorQueries {

  def listActiveZombieQuery(implicit ec: ExecutionContext): DBIO[Map[GoogleProject, Chain[ZombieCandidate]]] = {
    val runtimes = clusterQuery.filter(_.status inSetBind RuntimeStatus.activeStatuses.map(_.toString))
    val joinedQuery = runtimes.join(RuntimeConfigQueries.runtimeConfigs).on(_.runtimeConfigId === _.id)
    joinedQuery.result.map { rs =>
      rs.toList.foldMap { r =>
        val asyncRuntimeFields = (r._1.googleId, r._1.operationName, r._1.stagingBucket).mapN {
          (googleId, operationName, stagingBucket) =>
            AsyncRuntimeFields(googleId, OperationName(operationName), GcsBucketName(stagingBucket), r._1.hostIp map IP)
        }
        Map(
          r._1.googleProject -> Chain(
            ZombieCandidate(
              r._1.id,
              r._1.googleProject,
              r._1.clusterName,
              RuntimeStatus.withName(r._1.status),
              r._1.auditInfo.createdDate,
              asyncRuntimeFields,
              r._2.runtimeConfig.cloudService
            )
          )
        )
      }
    }
  }

  private def unconfirmedRuntimeLabels: Query[LabelTable, LabelRecord, Seq] =
    labelQuery
      .filter(_.resourceType === LabelResourceType.runtime)
      .filter(_.key === Config.zombieRuntimeMonitorConfig.deletionConfirmationLabelKey)
      .filter(_.value === "false")

  def listInactiveZombieQuery(implicit ec: ExecutionContext): DBIO[List[ZombieCandidate]] = {
    val runtimes = clusterQuery.filter(_.status === RuntimeStatus.Deleted.toString)
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
          runtimeRec.clusterName,
          RuntimeStatus.withName(runtimeRec.status),
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
