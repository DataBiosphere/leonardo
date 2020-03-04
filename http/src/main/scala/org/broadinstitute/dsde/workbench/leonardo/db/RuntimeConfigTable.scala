package org.broadinstitute.dsde.workbench.leonardo
package db

import java.time.Instant

import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.mappedColumnImplicits._

class RuntimeConfigTable(tag: Tag) extends Table[RuntimeConfigRecord](tag, "RUNTIME_CONFIG") {
  def id = column[RuntimeConfigId]("id", O.PrimaryKey, O.AutoInc)
  def cloudService = column[CloudService]("cloudService", O.Length(254))
  def numberOfWorkers = column[Int]("numberOfWorkers")
  def machineType = column[MachineTypeName]("machineType", O.Length(254))
  def diskSize = column[Int]("diskSize")
  def workerMachineType = column[Option[MachineTypeName]]("workerMachineType", O.Length(254))
  def workerDiskSize = column[Option[Int]]("workerDiskSize")
  def numberOfWorkerLocalSSDs = column[Option[Int]]("numberOfWorkerLocalSSDs")
  def numberOfPreemptibleWorkers = column[Option[Int]]("numberOfPreemptibleWorkers")
  def dateAccessed = column[Instant]("dateAccessed", O.SqlType("TIMESTAMP(6)"))

  def * =
    (
      id,
      (cloudService,
       numberOfWorkers,
       machineType,
       diskSize,
       workerMachineType,
       workerDiskSize,
       numberOfWorkerLocalSSDs,
       numberOfPreemptibleWorkers),
      dateAccessed
    ).shaped <> ({
      case (id,
            (cloudService,
             numberOfWorkers,
             machineType,
             diskSize,
             workerMachineType,
             workerDiskSize,
             numberOfWorkerLocalSSDs,
             numberOfPreemptibleWorkers),
            dateAccessed) =>
        val r = cloudService match {
          case CloudService.GCE =>
            RuntimeConfig.GceConfig(machineType, diskSize)

          case CloudService.Dataproc =>
            RuntimeConfig.DataprocConfig(numberOfWorkers,
                                         machineType,
                                         diskSize,
                                         workerMachineType,
                                         workerDiskSize,
                                         numberOfWorkerLocalSSDs,
                                         numberOfPreemptibleWorkers)
        }
        RuntimeConfigRecord(id, r, dateAccessed)
    }, { x: RuntimeConfigRecord =>
      x.runtimeConfig match {
        case r: RuntimeConfig.GceConfig =>
          Some(x.id,
               (CloudService.GCE: CloudService, 0, r.machineType, r.diskSize, None, None, None, None),
               x.dateAccessed)
        case r: RuntimeConfig.DataprocConfig =>
          Some(
            x.id,
            (CloudService.Dataproc: CloudService,
             r.numberOfWorkers,
             r.masterMachineType,
             r.masterDiskSize,
             r.workerMachineType,
             r.workerDiskSize,
             r.numberOfWorkerLocalSSDs,
             r.numberOfPreemptibleWorkers),
            x.dateAccessed
          )
      }
    })
}

final case class RuntimeConfigRecord(id: RuntimeConfigId, runtimeConfig: RuntimeConfig, dateAccessed: Instant)
