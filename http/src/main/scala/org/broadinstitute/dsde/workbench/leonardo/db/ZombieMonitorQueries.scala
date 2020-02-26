package org.broadinstitute.dsde.workbench.leonardo.db

import cats.data.Chain
import cats.implicits._
import org.broadinstitute.dsde.workbench.leonardo.AuditInfo
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
import org.broadinstitute.dsde.workbench.leonardo.model.google.{ClusterName, ClusterStatus}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import slick.dbio.DBIO

import scala.concurrent.ExecutionContext

object ZombieMonitorQueries {

  def listZombieQuery(implicit ec: ExecutionContext): DBIO[Map[GoogleProject, Chain[PotentialZombieCluster]]] =
    clusterQuery.filter { _.status inSetBind ClusterStatus.activeStatuses.map(_.toString) }.result.map { cs =>
      cs.toList.foldMap { c =>
        Map(
          c.googleProject -> Chain(
            PotentialZombieCluster(c.id, c.googleProject, c.clusterName, ClusterStatus.withName(c.status), c.auditInfo)
          )
        )
      }
    }

}

final case class PotentialZombieCluster(id: Long,
                                        googleProject: GoogleProject,
                                        clusterName: ClusterName,
                                        status: ClusterStatus,
                                        auditInfo: AuditInfo)
