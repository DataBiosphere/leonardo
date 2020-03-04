package org.broadinstitute.dsde.workbench.leonardo.db

import cats.data.Chain
import cats.implicits._
import org.broadinstitute.dsde.workbench.leonardo.{AuditInfo, RuntimeName, RuntimeStatus}
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import slick.dbio.DBIO

import scala.concurrent.ExecutionContext

object ZombieMonitorQueries {

  def listZombieQuery(implicit ec: ExecutionContext): DBIO[Map[GoogleProject, Chain[PotentialZombieRuntime]]] =
    clusterQuery.filter { _.status inSetBind RuntimeStatus.activeStatuses.map(_.toString) }.result.map { cs =>
      cs.toList.foldMap { c =>
        Map(
          c.googleProject -> Chain(
            PotentialZombieRuntime(c.id, c.googleProject, c.clusterName, RuntimeStatus.withName(c.status), c.auditInfo)
          )
        )
      }
    }

}

final case class PotentialZombieRuntime(id: Long,
                                        googleProject: GoogleProject,
                                        runtimeName: RuntimeName,
                                        status: RuntimeStatus,
                                        auditInfo: AuditInfo)
