package org.broadinstitute.dsde.workbench.leonardo
package db

import cats.implicits._
import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.mappedColumnImplicits._

import java.time.Instant
import scala.concurrent.ExecutionContext

object RuntimeConfigQueries {
  type RuntimeJoinLabelJoinRuntimeConfigJoinPatch =
    Query[(((ClusterTable, Rep[Option[LabelTable]]), RuntimeConfigTable), Rep[Option[PatchTable]]),
          (((ClusterRecord, Option[LabelRecord]), RuntimeConfigRecord), Option[PatchRecord]),
          Seq
    ]

  val runtimeConfigs = TableQuery[RuntimeConfigTable]

  /**
   * return DB generated id
   */
  def insertRuntimeConfig(runtimeConfig: RuntimeConfig, dateAccessed: Instant): DBIO[RuntimeConfigId] =
    runtimeConfigs.returning(runtimeConfigs.map(_.id)) += RuntimeConfigRecord(RuntimeConfigId(0),
                                                                              runtimeConfig,
                                                                              dateAccessed
    )

  def getRuntimeConfig(id: RuntimeConfigId)(implicit ec: ExecutionContext): DBIO[RuntimeConfig] =
    runtimeConfigs.filter(x => x.id === id).result.flatMap { x =>
      val res = x.headOption.map(x => x.runtimeConfig)
      res.fold[DBIO[RuntimeConfig]](DBIO.failed(new Exception(s"no runtimeConfig found for ${id}")))(x =>
        DBIO.successful(x)
      )
    }

  def getDiskId(id: RuntimeConfigId)(implicit ec: ExecutionContext): DBIO[Option[DiskId]] =
    runtimeConfigs.filter(x => x.id === id).result.flatMap { x =>
      val runtimeConfig = x.headOption.map(x => x.runtimeConfig)
      val diskId = runtimeConfig match {
        case Some(value) =>
          value match {
            case RuntimeConfig.GceConfig(_, _, _, _, _)                        => none[DiskId]
            case RuntimeConfig.GceWithPdConfig(_, persistentDiskId, _, _, _)   => persistentDiskId
            case RuntimeConfig.DataprocConfig(_, _, _, _, _, _, _, _, _, _, _) => none[DiskId]
            case RuntimeConfig.AzureConfig(_, persistentDiskId, _)             => persistentDiskId
          }
        case None => none[DiskId]
      }

      DBIO.successful(diskId)
    }

  def updateNumberOfWorkers(id: RuntimeConfigId, numberOfWorkers: Int, dateAccessed: Instant): DBIO[Int] =
    runtimeConfigs
      .filter(x => x.id === id)
      .map(c => (c.numberOfWorkers, c.dateAccessed))
      .update((numberOfWorkers, dateAccessed))

  def updateNumberOfPreemptibleWorkers(id: RuntimeConfigId,
                                       numberOfPreemptibleWorkers: Option[Int],
                                       dateAccessed: Instant
  ): DBIO[Int] =
    runtimeConfigs
      .filter(x => x.id === id)
      .map(c => (c.numberOfPreemptibleWorkers, c.dateAccessed))
      .update((numberOfPreemptibleWorkers, dateAccessed))

  def updateMachineType(id: RuntimeConfigId, machineType: MachineTypeName, dateAccessed: Instant): DBIO[Int] =
    runtimeConfigs
      .filter(x => x.id === id)
      .map(c => (c.machineType, c.dateAccessed))
      .update((machineType, dateAccessed))

  def updatePersistentDiskId(id: RuntimeConfigId, persistentDiskId: Option[DiskId], dateAccessed: Instant): DBIO[Int] =
    runtimeConfigs
      .filter(x => x.id === id)
      .map(c => (c.persistentDiskId, c.dateAccessed))
      .update((persistentDiskId, dateAccessed))

  // This function only applies to Runtimes that don't use persistent disk
  def updateDiskSize(id: RuntimeConfigId, newSizeGb: DiskSize, dateAccessed: Instant): DBIO[Int] =
    runtimeConfigs
      .filter(x => x.id === id)
      .map(c => (c.diskSize, c.dateAccessed))
      .update((Some(newSizeGb), dateAccessed))

  def isDiskAttached(diskId: DiskId)(implicit ec: ExecutionContext): DBIO[Boolean] =
    RuntimeConfigQueries.runtimeConfigs
      .filter(x => x.persistentDiskId.isDefined && x.persistentDiskId === diskId)
      .length
      .result
      .map(_ > 0)
}
