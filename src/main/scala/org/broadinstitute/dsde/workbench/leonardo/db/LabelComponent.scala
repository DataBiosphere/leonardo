package org.broadinstitute.dsde.workbench.leonardo.db

import org.broadinstitute.dsde.workbench.leonardo.model.TypedString.LabelMap

case class LabelRecord(clusterId: Long, key: String, value: String)

trait LabelComponent extends LeoComponent {
  this: ClusterComponent =>

  import profile.api._

  class LabelTable(tag: Tag) extends Table[LabelRecord](tag, "LABEL") {
    def clusterId = column[Long]  ("clusterId")
    def key =       column[String]("key", O.Length(254))
    def value =     column[String]("value", O.Length(254))

    def cluster = foreignKey("FK_CLUSTER_ID", clusterId, clusterQuery)(_.id)
    def uniqueKey = index("IDX_LABEL_UNIQUE", (clusterId, key), unique = true)

    def * = (clusterId, key, value) <> (LabelRecord.tupled, LabelRecord.unapply)
  }

  object labelQuery extends TableQuery(new LabelTable(_)) {

    def save(clusterId: Long, key: String, value: String): DBIO[Int] = {
      labelQuery += LabelRecord(clusterId, key, value)
    }

    // ++= does not actually produce a useful return value
    def saveAllForCluster(clusterId: Long, m: LabelMap): DBIO[Option[Int]] = {
      labelQuery ++= m map { case (key, value) => LabelRecord(clusterId, key, value) }
    }

    def getAllForCluster(clusterId: Long): DBIO[LabelMap] = {
      labelQuery.filter { _.clusterId === clusterId}.result map { recs =>
        val tuples = recs map { rec =>
          rec.key -> rec.value
        }
        tuples.toMap
      }
    }

    private def clusterKeyFilter(clusterId: Long, key: String): Query[LabelTable, LabelRecord, Seq] = {
      labelQuery.filter { _.clusterId === clusterId }.filter { _.key === key }
    }

    def get(clusterId: Long, key: String): DBIO[Option[String]] = {
      clusterKeyFilter(clusterId, key).map { _.value }.result.headOption
    }

    def delete(clusterId: Long, key: String): DBIO[Int] = {
      clusterKeyFilter(clusterId, key).delete
    }

    def deleteAllForCluster(clusterId: Long): DBIO[Int] = {
      labelQuery.filter { _.clusterId === clusterId }.delete
    }
  }
}
