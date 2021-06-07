package org.broadinstitute.dsde.workbench.leonardo
package db

import java.sql.SQLDataException
import java.time.Instant

import org.broadinstitute.dsde.workbench.google2.{MachineTypeName, RegionName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.mappedColumnImplicits._

class RuntimeConfigTable(tag: Tag) extends Table[RuntimeConfigRecord](tag, "RUNTIME_CONFIG") {
  def id = column[RuntimeConfigId]("id", O.PrimaryKey, O.AutoInc)
  def cloudService = column[CloudService]("cloudService", O.Length(254))
  def numberOfWorkers = column[Int]("numberOfWorkers")
  def machineType = column[MachineTypeName]("machineType", O.Length(254))
  def diskSize = column[Option[DiskSize]]("diskSize")
  def bootDiskSize = column[Option[DiskSize]]("bootDiskSize")
  def workerMachineType = column[Option[MachineTypeName]]("workerMachineType", O.Length(254))
  def workerDiskSize = column[Option[DiskSize]]("workerDiskSize")
  def numberOfWorkerLocalSSDs = column[Option[Int]]("numberOfWorkerLocalSSDs")
  def numberOfPreemptibleWorkers = column[Option[Int]]("numberOfPreemptibleWorkers")
  def dateAccessed = column[Instant]("dateAccessed", O.SqlType("TIMESTAMP(6)"))
  def dataprocProperties = column[Option[Map[String, String]]]("dataprocProperties")
  def persistentDiskId = column[Option[DiskId]]("persistentDiskId")
  def zone = column[Option[ZoneName]]("zone", O.Length(254))
  def region = column[Option[RegionName]]("region", O.Length(254))
  def gpuType = column[Option[GpuType]]("gpuType", O.Length(254))
  def numOfGpus = column[Option[Int]]("numOfGpus")

  def * =
    (
      id,
      (
        cloudService,
        numberOfWorkers,
        machineType,
        diskSize,
        bootDiskSize,
        workerMachineType,
        workerDiskSize,
        numberOfWorkerLocalSSDs,
        numberOfPreemptibleWorkers,
        dataprocProperties,
        persistentDiskId,
        zone,
        region,
        (gpuType, numOfGpus)
      ),
      dateAccessed
    ).shaped <> ({
      case (id,
            (cloudService,
             numberOfWorkers,
             machineType,
             diskSize,
             bootDiskSize,
             workerMachineType,
             workerDiskSize,
             numberOfWorkerLocalSSDs,
             numberOfPreemptibleWorkers,
             dataprocProperties,
             persistentDiskId,
             zone,
             region,
             gpuConfig),
            dateAccessed) =>
        val r = cloudService match {
          case CloudService.GCE =>
            diskSize match {
              case Some(size) =>
                RuntimeConfig.GceConfig(machineType,
                                        size,
                                        bootDiskSize,
                                        zone.getOrElse(throw new SQLDataException("zone should not be null for GCE")),
                                        getGpuConfig(gpuConfig._1, gpuConfig._2))
              case None =>
                val bds =
                  bootDiskSize.getOrElse(throw new SQLDataException("gce runtime with PD has to have a boot disk"))
                persistentDiskId.fold(
                  RuntimeConfig.GceWithPdConfig(
                    machineType,
                    None,
                    bds,
                    zone.getOrElse(throw new SQLDataException("zone should not be null for GCE")),
                    getGpuConfig(gpuConfig._1, gpuConfig._2)
                  )
                )(diskId =>
                  RuntimeConfig.GceWithPdConfig(
                    machineType,
                    Some(diskId),
                    bds,
                    zone.getOrElse(throw new SQLDataException("zone should not be null for GCE")),
                    getGpuConfig(gpuConfig._1, gpuConfig._2)
                  )
                )
            }
          case CloudService.Dataproc =>
            RuntimeConfig.DataprocConfig(
              numberOfWorkers,
              machineType,
              diskSize.getOrElse(throw new SQLDataException("diskSize field should not be null for Dataproc.")),
              workerMachineType,
              workerDiskSize,
              numberOfWorkerLocalSSDs,
              numberOfPreemptibleWorkers,
              dataprocProperties.getOrElse(Map.empty),
              region.getOrElse(throw new SQLDataException("region should not be null for Dataproc"))
            )
        }
        RuntimeConfigRecord(id, r, dateAccessed)
    }, { x: RuntimeConfigRecord =>
      x.runtimeConfig match {
        case r: RuntimeConfig.GceConfig =>
          Some(
            x.id,
            (CloudService.GCE: CloudService,
             0,
             r.machineType,
             Some(r.diskSize),
             r.bootDiskSize,
             None,
             None,
             None,
             None,
             None,
             None,
             Some(r.zone),
             None,
             (r.gpuConfig.map(_.gpuType), r.gpuConfig.map(_.numOfGpus))),
            x.dateAccessed
          )
        case r: RuntimeConfig.DataprocConfig =>
          Some(
            x.id,
            (CloudService.Dataproc: CloudService,
             r.numberOfWorkers,
             r.masterMachineType,
             Some(r.masterDiskSize),
             None,
             r.workerMachineType,
             r.workerDiskSize,
             r.numberOfWorkerLocalSSDs,
             r.numberOfPreemptibleWorkers,
             Some(r.properties),
             None,
             None,
             Some(r.region),
             (None, None)),
            x.dateAccessed
          )
        case r: RuntimeConfig.GceWithPdConfig =>
          Some(
            x.id,
            (CloudService.GCE: CloudService,
             0,
             r.machineType,
             None,
             Some(r.bootDiskSize),
             None,
             None,
             None,
             None,
             None,
             r.persistentDiskId,
             Some(r.zone),
             None,
             (r.gpuConfig.map(_.gpuType), r.gpuConfig.map(_.numOfGpus))),
            x.dateAccessed
          )
      }
    })

  def getGpuConfig(gpuType: Option[GpuType], numOfGpus: Option[Int]): Option[GpuConfig] =
    (gpuType, numOfGpus) match {
      case (Some(gpuType), Some(numOfGpus)) => Some(GpuConfig(gpuType, numOfGpus))
      case _                                => None
    }
}

final case class RuntimeConfigRecord(id: RuntimeConfigId, runtimeConfig: RuntimeConfig, dateAccessed: Instant)
