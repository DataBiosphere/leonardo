package org.broadinstitute.dsde.workbench.leonardo
package db

import java.time.Instant

import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.mappedColumnImplicits._
import org.broadinstitute.dsde.workbench.leonardo.db.LeonardoServiceDbQueries.ClusterJoinLabel

import scala.concurrent.ExecutionContext

object RuntimeConfigQueries {
  type ClusterJoinLabelJoinRuntimeConfig =
    Query[((ClusterTable, Rep[Option[LabelTable]]), Rep[Option[RuntimeConfigTable]]),
          ((ClusterRecord, Option[LabelRecord]), Option[RuntimeConfigRecord]),
          Seq]

  val runtimeConfigs = TableQuery[RuntimeConfigTable]

  def clusterLabelRuntimeConfigQuery(baseQuery: ClusterJoinLabel): ClusterJoinLabelJoinRuntimeConfig =
    for {
      ((cluster, label), runTimeConfig) <- baseQuery
        .joinLeft(runtimeConfigs)
        .on(_._1.runtimeConfigId === _.id)
    } yield ((cluster, label), runTimeConfig)

  /**
   * return DB generated id
   */
  def insertRuntimeConfig(runtimeConfig: RuntimeConfig, dateAccessed: Instant): DBIO[RuntimeConfigId] =
    runtimeConfigs.returning(runtimeConfigs.map(_.id)) += RuntimeConfigRecord(RuntimeConfigId(0),
                                                                              runtimeConfig,
                                                                              dateAccessed)

  def getRuntimeConfig(id: RuntimeConfigId)(implicit ec: ExecutionContext): DBIO[RuntimeConfig] =
    runtimeConfigs.filter(x => x.id === id).result.flatMap { x =>
      val res = x.headOption.map { x =>
        x.runtimeConfig
      }
      res.fold[DBIO[RuntimeConfig]](DBIO.failed(new Exception(s"no runtimeConfig found for ${id}")))(
        x => DBIO.successful(x)
      )
    }

  def updateNumberOfWorkers(id: RuntimeConfigId, numberOfWorkers: Int, dateAccessed: Instant): DBIO[Int] =
    runtimeConfigs
      .filter(x => x.id === id)
      .map(c => (c.numberOfWorkers, c.dateAccessed))
      .update((numberOfWorkers, dateAccessed))

  def updateNumberOfPreemptibleWorkers(id: RuntimeConfigId,
                                       numberOfPreemptibleWorkers: Option[Int],
                                       dateAccessed: Instant): DBIO[Int] =
    runtimeConfigs
      .filter(x => x.id === id)
      .map(c => (c.numberOfPreemptibleWorkers, c.dateAccessed))
      .update((numberOfPreemptibleWorkers, dateAccessed))

  def updateMachineType(id: RuntimeConfigId, machineType: MachineTypeName, dateAccessed: Instant): DBIO[Int] =
    runtimeConfigs
      .filter(x => x.id === id)
      .map(c => (c.machineType, c.dateAccessed))
      .update((machineType, dateAccessed))

  def updateDiskSize(id: RuntimeConfigId, newSizeGb: Int, dateAccessed: Instant): DBIO[Int] =
    runtimeConfigs
      .filter(x => x.id === id)
      .map(c => (c.diskSize, c.dateAccessed))
      .update((newSizeGb, dateAccessed))
}
