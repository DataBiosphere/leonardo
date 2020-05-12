package org.broadinstitute.dsde.workbench.leonardo
package db

import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.mappedColumnImplicits._

import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.ExecutionContext

object DiskServiceDbQueries {

  def listDisks(labelMap: LabelMap, includeDeleted: Boolean, googleProjectOpt: Option[GoogleProject] = None)(
    implicit ec: ExecutionContext
  ): DBIO[List[PersistentDisk]] = {
    val diskQueryFilteredByDeletion =
      if (includeDeleted) persistentDiskQuery
      else persistentDiskQuery.filterNot(_.status === (DiskStatus.Deleted: DiskStatus))
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

    diskQueryFilteredByLabel.result.map(x => persistentDiskQuery.aggregateLabels(x).toList)
  }
}
