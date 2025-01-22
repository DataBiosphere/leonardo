package org.broadinstitute.dsde.workbench.leonardo
package db

import cats.syntax.all._
import org.broadinstitute.dsde.workbench.google2.DiskName
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.mappedColumnImplicits._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.unmarshalDestroyedDate
import org.broadinstitute.dsde.workbench.leonardo.db.persistentDiskQuery.unmarshalPersistentDisk
import org.broadinstitute.dsde.workbench.leonardo.http.{GetPersistentDiskResponse, GetPersistentDiskV2Response}
import org.broadinstitute.dsde.workbench.leonardo.http.service.{DiskNotFoundByIdException, DiskNotFoundException}
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}

import scala.concurrent.ExecutionContext

object DiskServiceDbQueries {

  def listDisks(labelMap: LabelMap,
                includeDeleted: Boolean,
                creatorOnly: Option[WorkbenchEmail],
                cloudContextOpt: Option[CloudContext] = None,
                workspaceOpt: Option[WorkspaceId] = None
  )(implicit
    ec: ExecutionContext
  ): DBIO[List[PersistentDisk]] = {

    // filtered by creator first as it may have great impact
    val diskQueryFilteredByCreator = creatorOnly match {
      case Some(email) => persistentDiskQuery.tableQuery.filter(_.creator === email)
      case None        => persistentDiskQuery.tableQuery
    }

    val diskQueryFilteredByDeletion =
      if (includeDeleted) diskQueryFilteredByCreator
      else diskQueryFilteredByCreator.filterNot(_.status === (DiskStatus.Deleted: DiskStatus))

    val diskQueryFilteredByProject =
      cloudContextOpt.fold(diskQueryFilteredByDeletion)(p =>
        diskQueryFilteredByDeletion
          .filter(_.cloudProvider === p.cloudProvider)
          .filter(_.cloudContext === p.asCloudContextDb)
      )

    val diskQueryFilteredByWorkspace =
      workspaceOpt.fold(diskQueryFilteredByProject)(workspaceId =>
        diskQueryFilteredByProject
          .filter(_.workspaceId === workspaceId)
      )

    val diskQueryJoinedWithLabel = persistentDiskQuery.joinLabelQuery(diskQueryFilteredByWorkspace)

    val diskQueryFilteredByLabel = if (labelMap.isEmpty) {
      diskQueryJoinedWithLabel
    } else {
      diskQueryJoinedWithLabel.filter { case (diskRec, _) =>
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
        x.toList.foldMap { case (diskRec, labelRecOpt) =>
          val labelMap = labelRecOpt.map(labelRec => labelRec.key -> labelRec.value).toMap
          Map(diskRec -> labelMap)
        }
      diskLabelMap.map { case (diskRec, labelMap) =>
        unmarshalPersistentDisk(diskRec, labelMap)
      }.toList
    }
  }

  def getGetPersistentDiskResponse(cloudContext: CloudContext, diskName: DiskName, traceId: TraceId)(implicit
    executionContext: ExecutionContext
  ): DBIO[GetPersistentDiskResponse] = {
    val diskQuery = persistentDiskQuery.findActiveByNameQuery(cloudContext, diskName)
    val diskQueryJoinedWithLabels = persistentDiskQuery.joinLabelQuery(diskQuery)

    diskQueryJoinedWithLabels.result.flatMap { x =>
      val diskWithLabel = x.toList.foldMap { case (diskRec, labelRecOpt) =>
        val labelMap = labelRecOpt.map(labelRec => labelRec.key -> labelRec.value).toMap
        Map(diskRec -> labelMap)
      }.headOption
      diskWithLabel.fold[DBIO[GetPersistentDiskResponse]](
        DBIO.failed(DiskNotFoundException(cloudContext, diskName, traceId))
      ) { d =>
        val diskRec = d._1
        val labelMap = d._2
        val getDiskResponse = GetPersistentDiskResponse(
          diskRec.id,
          diskRec.cloudContext,
          diskRec.zone,
          diskRec.name,
          diskRec.serviceAccount,
          diskRec.samResource,
          diskRec.status,
          AuditInfo(diskRec.creator,
                    diskRec.createdDate,
                    unmarshalDestroyedDate(diskRec.destroyedDate),
                    diskRec.dateAccessed
          ),
          diskRec.size,
          diskRec.diskType,
          diskRec.blockSize,
          labelMap,
          diskRec.formattedBy,
          diskRec.workspaceId
        )
        DBIO.successful(getDiskResponse)
      }
    }
  }

  def getGetPersistentDiskResponseV2(diskId: DiskId, traceId: TraceId)(implicit
    executionContext: ExecutionContext
  ): DBIO[GetPersistentDiskV2Response] = {
    val diskQuery = persistentDiskQuery.findByIdQuery(diskId)
    val diskQueryJoinedWithLabels = persistentDiskQuery.joinLabelQuery(diskQuery)

    diskQueryJoinedWithLabels.result.flatMap { x =>
      val diskWithLabel = x.toList.foldMap { case (diskRec, labelRecOpt) =>
        val labelMap = labelRecOpt.map(labelRec => labelRec.key -> labelRec.value).toMap
        Map(diskRec -> labelMap)
      }.headOption
      diskWithLabel.fold[DBIO[GetPersistentDiskV2Response]](
        DBIO.failed(DiskNotFoundByIdException(diskId, traceId))
      ) { d =>
        val diskRec = d._1
        val labelMap = d._2
        val getDiskResponse = GetPersistentDiskV2Response(
          diskRec.id,
          diskRec.cloudContext,
          diskRec.zone,
          diskRec.name,
          diskRec.serviceAccount,
          diskRec.samResource,
          diskRec.status,
          AuditInfo(diskRec.creator,
                    diskRec.createdDate,
                    unmarshalDestroyedDate(diskRec.destroyedDate),
                    diskRec.dateAccessed
          ),
          diskRec.size,
          diskRec.diskType,
          diskRec.blockSize,
          labelMap,
          diskRec.workspaceId,
          diskRec.formattedBy
        )
        DBIO.successful(getDiskResponse)
      }
    }
  }
}
