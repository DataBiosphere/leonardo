package org.broadinstitute.dsde.workbench.leonardo
package db

import cats.syntax.all._
import org.broadinstitute.dsde.workbench.google2.DiskName
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.mappedColumnImplicits._
import org.broadinstitute.dsde.workbench.leonardo.http.GetPersistentDiskResponse
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.unmarshalDestroyedDate
import org.broadinstitute.dsde.workbench.leonardo.db.persistentDiskQuery.unmarshalPersistentDisk
import org.broadinstitute.dsde.workbench.leonardo.http.service.DiskNotFoundException
import org.broadinstitute.dsde.workbench.model.TraceId

import scala.concurrent.ExecutionContext

object DiskServiceDbQueries {

  def listDisks(labelMap: LabelMap, includeDeleted: Boolean, googleProjectOpt: Option[GoogleProject] = None)(
    implicit ec: ExecutionContext
  ): DBIO[List[PersistentDisk]] = {
    val diskQueryFilteredByDeletion =
      if (includeDeleted) persistentDiskQuery.tableQuery
      else persistentDiskQuery.tableQuery.filterNot(_.status === (DiskStatus.Deleted: DiskStatus))

    val diskQueryFilteredByProject =
      googleProjectOpt.fold(diskQueryFilteredByDeletion)(p => diskQueryFilteredByDeletion.filter(_.googleProject === p))

    val diskQueryJoinedWithLabel = persistentDiskQuery.joinLabelQuery(diskQueryFilteredByProject)

    val diskQueryFilteredByLabel = if (labelMap.isEmpty) {
      diskQueryJoinedWithLabel
    } else {
      diskQueryJoinedWithLabel.filter {
        case (diskRec, _) =>
          labelQuery
            .filter(lbl =>
              lbl.resourceId.mapTo[DiskId] === diskRec.id && lbl.resourceType === LabelResourceType.persistentDisk
            )
            // The following confusing line is equivalent to the much simpler:
            // .filter { lbl => (lbl.key, lbl.value) inSetBind labelMap.toSet }
            // Unfortunately slick doesn't support inSet/inSetBind for tuples.
            // https://github.com/slick/slick/issues/517
            .filter(lbl => labelMap.map { case (k, v) => lbl.key === k && lbl.value === v }.reduce(_ || _))
            .length === labelMap.size
      }
    }
    diskQueryFilteredByLabel.result.map { x =>
      val diskLabelMap: Map[PersistentDiskRecord, Map[String, String]] =
        x.toList.foldMap {
          case (diskRec, labelRecOpt) =>
            val labelMap = labelRecOpt.map(labelRec => labelRec.key -> labelRec.value).toMap
            Map(diskRec -> labelMap)
        }
      diskLabelMap.map {
        case (diskRec, labelMap) =>
          unmarshalPersistentDisk(diskRec, labelMap)
      }.toList
    }
  }

  def getGetPersistentDiskResponse(googleProject: GoogleProject, diskName: DiskName, traceId: TraceId)(
    implicit executionContext: ExecutionContext
  ): DBIO[GetPersistentDiskResponse] = {
    val diskQuery = persistentDiskQuery.findActiveByNameQuery(googleProject, diskName)
    val diskQueryJoinedWithLabels = persistentDiskQuery.joinLabelQuery(diskQuery)

    diskQueryJoinedWithLabels.result.flatMap { x =>
      val diskWithLabel = x.toList.foldMap {
        case (diskRec, labelRecOpt) =>
          val labelMap = labelRecOpt.map(labelRec => labelRec.key -> labelRec.value).toMap
          Map(diskRec -> labelMap)
      }.headOption
      diskWithLabel.fold[DBIO[GetPersistentDiskResponse]](
        DBIO.failed(DiskNotFoundException(googleProject, diskName, traceId))
      ) { d =>
        val diskRec = d._1
        val labelMap = d._2
        val getDiskResponse = GetPersistentDiskResponse(
          diskRec.id,
          diskRec.googleProject,
          diskRec.zone,
          diskRec.name,
          diskRec.googleId,
          diskRec.serviceAccount,
          diskRec.samResource,
          diskRec.status,
          AuditInfo(diskRec.creator,
                    diskRec.createdDate,
                    unmarshalDestroyedDate(diskRec.destroyedDate),
                    diskRec.dateAccessed),
          diskRec.size,
          diskRec.diskType,
          diskRec.blockSize,
          labelMap
        )
        DBIO.successful(getDiskResponse)
      }
    }
  }
}
