package org.broadinstitute.dsde.workbench.leonardo.db

import java.util.UUID

case class LabelRecord(clusterId: UUID, key: String, value: String)

trait LabelComponent extends LeoComponent {
  this: ClusterComponent =>

  import profile.api._

  class LabelTable(tag: Tag) extends Table[LabelRecord](tag, "LABEL") {
    def clusterId = column[UUID]  ("clusterId")
    def key =       column[String]("key", O.Length(254))
    def value =     column[String]("value", O.Length(254))

    def cluster = foreignKey("FK_CLUSTER_ID", clusterId, clusterQuery)(_.clusterId)
    def uniqueKey = index("IDX_LABEL_UNIQUE", (clusterId, key), unique = true)

    def * = (clusterId, key, value) <> (LabelRecord.tupled, LabelRecord.unapply)
  }

  object labelQuery extends TableQuery(new LabelTable(_)) {

    def save(clusterId: UUID, key: String, value: String): DBIO[Int] = {
      labelQuery += LabelRecord(clusterId, key, value)
    }

    // ++= does not actually produce a useful return value
    def saveAll(clusterId: UUID, m: Map[String, String]): DBIO[Option[Int]] = {
      labelQuery ++= m map { case (key, value) => LabelRecord(clusterId, key, value) }
    }

    def getAll(clusterId: UUID): DBIO[Map[String, String]] = {
      labelQuery.filter { _.clusterId === clusterId}.result map { recs =>
        val tuples = recs map { rec =>
          rec.key -> rec.value
        }
        tuples.toMap
      }
    }

    private def clusterKeyFilter(clusterId: UUID, key: String): Query[LabelTable, LabelRecord, Seq] = {
      labelQuery.filter { _.clusterId === clusterId }.filter { _.key === key }
    }

    def get(clusterId: UUID, key: String): DBIO[Option[String]] = {
      clusterKeyFilter(clusterId, key).map { _.value }.result.headOption
    }

    def delete(clusterId: UUID, key: String): DBIO[Int] = {
      clusterKeyFilter(clusterId, key).delete
    }

    def deleteAll(clusterId: UUID): DBIO[Int] = {
      labelQuery.filter { _.clusterId === clusterId }.delete
    }
  }
}
