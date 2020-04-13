package org.broadinstitute.dsde.workbench.leonardo
package db

import cats.data.Chain
import cats.implicits._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.mappedColumnImplicits._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import slick.dbio.DBIO
import LeoProfile.api._

import scala.concurrent.ExecutionContext

object ZombieMonitorQueries {

  def listZombieQuery(implicit ec: ExecutionContext): DBIO[Map[GoogleProject, Chain[PotentialZombieRuntime]]] = {
    val clusters = clusterQuery.filter(_.status inSetBind RuntimeStatus.activeStatuses.map(_.toString))
    val joinedQuery = clusters.joinLeft(RuntimeConfigQueries.runtimeConfigs).on(_.runtimeConfigId === _.id)
    joinedQuery.result.map { cs =>
      cs.toList.foldMap { c =>
        Map(
          c._1.googleProject -> Chain(
            PotentialZombieRuntime(
              c._1.id,
              c._1.googleProject,
              c._1.clusterName,
              RuntimeStatus.withName(c._1.status),
              c._1.auditInfo,
              c._2.get.runtimeConfig.cloudService
            ) // .get here should be okay since every cluster record in database should have a runtimeConfig
          )
        )
      }
    }
  }

}

final case class PotentialZombieRuntime(id: Long,
                                        googleProject: GoogleProject,
                                        runtimeName: RuntimeName,
                                        status: RuntimeStatus,
                                        auditInfo: AuditInfo,
                                        cloudService: CloudService)
