package org.broadinstitute.dsde.workbench.leonardo
package db

import cats.syntax.all._
import org.broadinstitute.dsde.workbench.azure.AzureCloudContext
import org.broadinstitute.dsde.workbench.google2.{DiskName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.AppRestore.{GalaxyRestore, Other}
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.PersistentDiskSamResourceId
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.mappedColumnImplicits._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.{dummyDate, unmarshalDestroyedDate}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import java.sql.SQLDataException
import java.time.Instant
import scala.concurrent.ExecutionContext

final case class PersistentDiskRecord(id: DiskId,
                                      cloudContext: CloudContext,
                                      zone: ZoneName,
                                      name: DiskName,
                                      serviceAccount: WorkbenchEmail,
                                      samResource: PersistentDiskSamResourceId,
                                      status: DiskStatus,
                                      creator: WorkbenchEmail,
                                      createdDate: Instant,
                                      destroyedDate: Instant,
                                      dateAccessed: Instant,
                                      size: DiskSize,
                                      diskType: DiskType,
                                      blockSize: BlockSize,
                                      formattedBy: Option[FormattedBy],
                                      appRestore: Option[AppRestore],
                                      sourceDisk: Option[DiskLink],
                                      wsmResourceId: Option[WsmControlledResourceId],
                                      workspaceId: Option[WorkspaceId]
)

class PersistentDiskTable(tag: Tag) extends Table[PersistentDiskRecord](tag, "PERSISTENT_DISK") {
  def id = column[DiskId]("id", O.PrimaryKey, O.AutoInc)
  def cloudContext = column[CloudContextDb]("cloudContext", O.Length(255))
  def cloudProvider = column[CloudProvider]("cloudProvider", O.Length(50))
  def zone = column[ZoneName]("zone", O.Length(255))
  def name = column[DiskName]("name", O.Length(255))
  def serviceAccount = column[WorkbenchEmail]("serviceAccount", O.Length(255))
  def samResourceId = column[PersistentDiskSamResourceId]("samResourceId", O.Length(255))
  def status = column[DiskStatus]("status", O.Length(255))
  def creator = column[WorkbenchEmail]("creator", O.Length(255))
  def createdDate = column[Instant]("createdDate", O.SqlType("TIMESTAMP(6)"))
  def destroyedDate = column[Instant]("destroyedDate", O.SqlType("TIMESTAMP(6)"))
  def dateAccessed = column[Instant]("dateAccessed", O.SqlType("TIMESTAMP(6)"))
  def size = column[DiskSize]("sizeGb")
  def diskType = column[DiskType]("type", O.Length(255))
  def blockSize = column[BlockSize]("blockSizeBytes")
  def formattedBy = column[Option[FormattedBy]]("formattedBy", O.Length(255))
  def galaxyPvcId = column[Option[PvcId]]("galaxyPvcId", O.Length(254))
  def lastUsedBy = column[Option[AppId]]("lastUsedBy")
  def sourceDisk = column[Option[DiskLink]]("sourceDisk", O.Length(1024))
  def wsmResourceId = column[Option[WsmControlledResourceId]]("wsmResourceId")
  def workspaceId = column[Option[WorkspaceId]]("workspaceId")

  override def * =
    (id,
     (cloudProvider, cloudContext),
     zone,
     name,
     serviceAccount,
     samResourceId,
     status,
     creator,
     createdDate,
     destroyedDate,
     dateAccessed,
     size,
     diskType,
     blockSize,
     formattedBy,
     (galaxyPvcId, lastUsedBy),
     sourceDisk,
     wsmResourceId,
     workspaceId
    ) <> ({
      case (id,
            (cloudProvider, cloudContextDb),
            zone,
            name,
            serviceAccount,
            samResourceId,
            status,
            creator,
            createdDate,
            destroyedDate,
            dateAccessed,
            size,
            diskType,
            blockSize,
            formattedBy,
            (galaxyPvcId, lastUsedBy),
            sourceDisk,
            wsmResourceId,
            workspaceId
          ) =>
        PersistentDiskRecord(
          id,
          cloudProvider match {
            case CloudProvider.Gcp =>
              CloudContext.Gcp(GoogleProject(cloudContextDb.value)): CloudContext
            case CloudProvider.Azure =>
              val context =
                AzureCloudContext.fromString(cloudContextDb.value).fold(s => throw new SQLDataException(s), identity)

              CloudContext.Azure(context): CloudContext
          },
          zone,
          name,
          serviceAccount,
          samResourceId,
          status,
          creator,
          createdDate,
          destroyedDate,
          dateAccessed,
          size,
          diskType,
          blockSize,
          formattedBy,
          formattedBy.flatMap {
            case FormattedBy.Galaxy =>
              (galaxyPvcId, lastUsedBy).mapN((gp, lb) => GalaxyRestore(gp, lb))
            case FormattedBy.Cromwell                 => lastUsedBy.map(Other)
            case FormattedBy.Jupyter                  => lastUsedBy.map(Other) // TODO (LM)
            case FormattedBy.Allowed                  => lastUsedBy.map(Other)
            case FormattedBy.GCE | FormattedBy.Custom => None
          },
          sourceDisk,
          wsmResourceId,
          workspaceId
        )
    }, { record: PersistentDiskRecord =>
      Some(
        record.id,
        record.cloudContext match {
          case CloudContext.Gcp(value) =>
            (CloudProvider.Gcp, CloudContextDb(value.value))
          case CloudContext.Azure(value) =>
            (CloudProvider.Azure, CloudContextDb(value.asString))
        },
        record.zone,
        record.name,
        record.serviceAccount,
        record.samResource,
        record.status,
        record.creator,
        record.createdDate,
        record.destroyedDate,
        record.dateAccessed,
        record.size,
        record.diskType,
        record.blockSize,
        record.formattedBy,
        record.appRestore match {
          case None                     => (None, None)
          case Some(app: Other)         => (None, Some(app.lastUsedBy))
          case Some(app: GalaxyRestore) => (Some(app.galaxyPvcId), Some(app.lastUsedBy))
        },
        record.sourceDisk,
        record.wsmResourceId,
        record.workspaceId
      )
    })
}

object persistentDiskQuery {
  val tableQuery = TableQuery[PersistentDiskTable]

  private[db] def findByIdQuery(id: DiskId) = tableQuery.filter(_.id === id)

  private[db] def findActiveByIdQuery(id: DiskId) =
    tableQuery
      .filter(_.id === id)
      .filter(_.destroyedDate === dummyDate)

  private[db] def findActiveByNameQuery(cloudContext: CloudContext, name: DiskName) =
    tableQuery
      .filter(_.cloudContext === cloudContext.asCloudContextDb)
      .filter(_.name === name)
      .filter(_.destroyedDate === dummyDate)

  private[db] def joinLabelQuery(baseQuery: Query[PersistentDiskTable, PersistentDiskRecord, Seq]) =
    for {
      (disk, label) <- baseQuery joinLeft labelQuery on { case (d, lbl) =>
        lbl.resourceId.mapTo[DiskId] === d.id && lbl.resourceType === LabelResourceType.persistentDisk
      }
    } yield (disk, label)

  def updateGalaxyDiskRestore(id: DiskId, galaxyDiskRestore: GalaxyRestore): DBIO[Int] =
    findByIdQuery(id)
      .map(x => (x.galaxyPvcId, x.lastUsedBy))
      .update(
        (Some(galaxyDiskRestore.galaxyPvcId), Some(galaxyDiskRestore.lastUsedBy))
      )

  def updateLastUsedBy(id: DiskId, lastUsedBy: AppId): DBIO[Int] =
    findByIdQuery(id)
      .map(x => x.lastUsedBy)
      .update(
        Some(lastUsedBy)
      )

  def getAppDiskRestore(id: DiskId)(implicit ec: ExecutionContext): DBIO[Option[AppRestore]] =
    findByIdQuery(id).result
      .map(_.headOption.flatMap(_.appRestore))

  def save(disk: PersistentDisk)(implicit ec: ExecutionContext): DBIO[PersistentDisk] =
    for {
      diskId <- (tableQuery returning tableQuery.map(_.id)) += marshalPersistentDisk(disk)
      _ <- labelQuery.saveAllForResource(diskId.value, LabelResourceType.PersistentDisk, disk.labels)
    } yield disk.copy(diskId)

  def getById(id: DiskId)(implicit ec: ExecutionContext): DBIO[Option[PersistentDisk]] =
    joinLabelQuery(findByIdQuery(id)).result.map(aggregateLabels).map(_.headOption)

  def getStatus(id: DiskId)(implicit ec: ExecutionContext): DBIO[Option[DiskStatus]] =
    getPersistentDiskRecord(id).map(_.map(_.status))

  def getPersistentDiskRecord(id: DiskId): DBIO[Option[PersistentDiskRecord]] =
    findByIdQuery(id).result.headOption

  def getActiveById(id: DiskId)(implicit
    ec: ExecutionContext
  ): DBIO[Option[PersistentDisk]] =
    joinLabelQuery(findActiveByIdQuery(id)).result.map(aggregateLabels).map(_.headOption)

  def getActiveByName(cloudContext: CloudContext, name: DiskName)(implicit
    ec: ExecutionContext
  ): DBIO[Option[PersistentDisk]] =
    joinLabelQuery(findActiveByNameQuery(cloudContext, name)).result.map(aggregateLabels).map(_.headOption)

  def updateStatus(id: DiskId, newStatus: DiskStatus, dateAccessed: Instant): DBIO[Int] =
    findByIdQuery(id).map(d => (d.status, d.dateAccessed)).update((newStatus, dateAccessed))

  def updateStatusAndIsFormatted(id: DiskId, newStatus: DiskStatus, formattedBy: FormattedBy, dateAccessed: Instant) =
    findByIdQuery(id)
      .map(d => (d.status, d.formattedBy, d.dateAccessed))
      .update((newStatus, Some(formattedBy), dateAccessed))

  def updateWSMResourceId(id: DiskId, newWSMResourceId: WsmControlledResourceId, dateAccessed: Instant) =
    findByIdQuery(id).map(d => (d.wsmResourceId, d.dateAccessed)).update((Some(newWSMResourceId), dateAccessed))

  def markPendingDeletion(id: DiskId, dateAccessed: Instant): DBIO[Int] =
    findByIdQuery(id)
      .map(d => (d.status, d.dateAccessed))
      .update((DiskStatus.Deleting, dateAccessed))

  def nullifyDiskIds = persistentDiskQuery.tableQuery.map(x => x.lastUsedBy).update(None)

  def delete(id: DiskId, destroyedDate: Instant): DBIO[Int] =
    findByIdQuery(id)
      .map(d => (d.status, d.destroyedDate, d.dateAccessed))
      .update((DiskStatus.Deleted, destroyedDate, destroyedDate))

  def updateSize(id: DiskId, newSize: DiskSize, dateAccessed: Instant) =
    findByIdQuery(id).map(d => (d.size, d.dateAccessed)).update((newSize, dateAccessed))

  def getFormattedBy(id: DiskId)(implicit ec: ExecutionContext): DBIO[Option[FormattedBy]] =
    findByIdQuery(id).map(_.formattedBy).result.headOption.map(_.flatten)

  def isDiskAttached(diskId: DiskId)(implicit ec: ExecutionContext): DBIO[Boolean] =
    for {
      formattedBy <- getFormattedBy(diskId)
      r <- formattedBy match {
        case None =>
          for {
            isAttachedToRuntime <- RuntimeConfigQueries.isDiskAttached(diskId)
            isAttached <- if (isAttachedToRuntime) DBIO.successful(true) else appQuery.isDiskAttached(diskId)
          } yield isAttached
        case Some(
              FormattedBy.Galaxy | FormattedBy.Custom | FormattedBy.Cromwell | FormattedBy.Allowed | FormattedBy.Jupyter
            ) =>
          appQuery.isDiskAttached(diskId)
        case Some(FormattedBy.GCE) =>
          RuntimeConfigQueries.isDiskAttached(diskId)
      }
    } yield r

  private[db] def marshalPersistentDisk(disk: PersistentDisk): PersistentDiskRecord =
    PersistentDiskRecord(
      disk.id,
      disk.cloudContext,
      disk.zone,
      disk.name,
      disk.serviceAccount,
      disk.samResource,
      disk.status,
      disk.auditInfo.creator,
      disk.auditInfo.createdDate,
      disk.auditInfo.destroyedDate.getOrElse(dummyDate),
      disk.auditInfo.dateAccessed,
      disk.size,
      disk.diskType,
      disk.blockSize,
      disk.formattedBy,
      disk.appRestore,
      disk.sourceDisk,
      disk.wsmResourceId,
      disk.workspaceId
    )

  private[db] def aggregateLabels(
    recs: Seq[(PersistentDiskRecord, Option[LabelRecord])]
  ): Seq[PersistentDisk] = {
    val pdLabelMap: Map[PersistentDiskRecord, Map[String, String]] =
      recs.toList.foldMap { case (rec, labelRecOpt) =>
        val labelMap = labelRecOpt.map(lblRec => Map(lblRec.key -> lblRec.value)).getOrElse(Map.empty)
        Map(rec -> labelMap)
      }

    pdLabelMap.toList.map { case (rec, labels) =>
      unmarshalPersistentDisk(rec, labels)
    }
  }

  private[db] def unmarshalPersistentDisk(rec: PersistentDiskRecord, labels: LabelMap): PersistentDisk =
    PersistentDisk(
      rec.id,
      rec.cloudContext,
      rec.zone,
      rec.name,
      rec.serviceAccount,
      rec.samResource,
      rec.status,
      AuditInfo(
        rec.creator,
        rec.createdDate,
        unmarshalDestroyedDate(rec.destroyedDate),
        rec.dateAccessed
      ),
      rec.size,
      rec.diskType,
      rec.blockSize,
      rec.formattedBy,
      rec.appRestore,
      labels,
      rec.sourceDisk,
      rec.wsmResourceId,
      rec.workspaceId
    )
}
