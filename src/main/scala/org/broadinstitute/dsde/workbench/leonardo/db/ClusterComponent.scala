package org.broadinstitute.dsde.workbench.leonardo.db

import java.sql.Timestamp
import java.util.UUID

import org.broadinstitute.dsde.workbench.leonardo.model.TypedString.LabelMap
import org.broadinstitute.dsde.workbench.leonardo.model._

case class ClusterRecord(id: Long,
                         clusterName: String,
                         googleId: UUID,
                         googleProject: String,
                         googleServiceAccount: String,
                         googleBucket: String,
                         operationName: String,
                         status: String,
                         hostIp: Option[String],
                         createdDate: Timestamp,
                         destroyedDate: Option[Timestamp])

trait ClusterComponent extends LeoComponent {
  this: LabelComponent =>

  import profile.api._

  class ClusterTable(tag: Tag) extends Table[ClusterRecord](tag, "CLUSTER") {
    def id =                    column[Long]              ("id",                    O.PrimaryKey, O.AutoInc)
    def clusterName =           column[String]            ("clusterName",           O.Length(254))
    def googleId =              column[UUID]              ("googleId",              O.Unique)
    def googleProject =         column[String]            ("googleProject",         O.Length(254))
    def googleServiceAccount =  column[String]            ("googleServiceAccount",  O.Length(254))
    def googleBucket =          column[String]            ("googleBucket",          O.Length(254))
    def operationName =         column[String]            ("operationName",         O.Length(254))
    def status =                column[String]            ("status",                O.Length(254))
    def hostIp =                column[Option[String]]    ("hostIp",                O.Length(254))
    def createdDate =           column[Timestamp]         ("createdDate",           O.SqlType("TIMESTAMP(6)"))
    def destroyedDate =         column[Option[Timestamp]] ("destroyedDate",         O.SqlType("TIMESTAMP(6)"))

    def uniqueKey = index("IDX_CLUSTER_UNIQUE", (googleProject, clusterName), unique = true)

    def * = (id, clusterName, googleId, googleProject, googleServiceAccount, googleBucket, operationName, status, hostIp, createdDate, destroyedDate) <> (ClusterRecord.tupled, ClusterRecord.unapply)
  }

  object clusterQuery extends TableQuery(new ClusterTable(_)) {

    def save(cluster: Cluster): DBIO[Cluster] = {
      (clusterQuery returning clusterQuery.map(_.id) += marshalCluster(cluster)) flatMap { clusterId =>
        labelQuery.saveAllForCluster(clusterId, cluster.labels)
      } map { _ => cluster }
    }

    def list(): DBIO[Seq[Cluster]] = {
      clusterQuery.result flatMap { recs =>
        DBIO.sequence(recs map unmarshalWithLabels)
      }
    }

    def getByName(project: GoogleProject, name: ClusterName): DBIO[Option[Cluster]] = {
      clusterQuery.filter { _.googleProject === project.s }.filter { _.clusterName === name.s }.result flatMap { recs =>
        DBIO.sequence(recs map unmarshalWithLabels) map { _.headOption }
      }
    }

    def getByGoogleId(googleId: UUID): DBIO[Option[Cluster]] = {
      clusterQuery.filter { _.googleId === googleId }.result flatMap { recs =>
        DBIO.sequence(recs map unmarshalWithLabels) map { _.headOption }
      }
    }

    def deleteByGoogleId(googleId: UUID): DBIO[Int] = {
      clusterQuery.filter { _.googleId === googleId }.result flatMap { recs =>
        DBIO.fold(recs map { r => deleteById(r.id) }, 0)(_ + _)
      }
    }

    private def deleteById(id: Long): DBIO[Int] = {
      labelQuery.deleteAllForCluster(id) flatMap { _ =>
        clusterQuery.filter { _.id === id }.delete
      }
    }

    def getIdByGoogleId(googleId: UUID): DBIO[Option[Long]] = {
      clusterQuery.filter { _.googleId === googleId }.result map { recs =>
        recs.headOption map { _.id }
      }
    }

    private def marshalCluster(cluster: Cluster): ClusterRecord = {
      ClusterRecord(
        id = 0,    // DB AutoInc
        cluster.clusterName.s,
        cluster.googleId,
        cluster.googleProject.s,
        cluster.googleServiceAccount.s,
        cluster.googleBucket.s,
        cluster.operationName.s,
        cluster.status.toString,
        cluster.hostIp map(_.s),
        Timestamp.from(cluster.createdDate),
        cluster.destroyedDate map Timestamp.from
      )
    }

    private def unmarshalWithLabels(clusterRecord: ClusterRecord): DBIO[Cluster] = {
      labelQuery.getAllForCluster(clusterRecord.id) map { labels =>
        unmarshalCluster(clusterRecord, labels)
      }
    }

    private def unmarshalCluster(clusterRecord: ClusterRecord, labels: LabelMap): Cluster = {
      val name = ClusterName(clusterRecord.clusterName)
      val project = GoogleProject(clusterRecord.googleProject)
      Cluster(
        name,
        clusterRecord.googleId,
        project,
        GoogleServiceAccount(clusterRecord.googleServiceAccount),
        GoogleBucket(clusterRecord.googleBucket),
        Cluster.getClusterUrl(project, name),
        OperationName(clusterRecord.operationName),
        ClusterStatus.withName(clusterRecord.status),
        clusterRecord.hostIp map IP,
        clusterRecord.createdDate.toInstant,
        clusterRecord.destroyedDate map { _.toInstant },
        labels
      )
    }

  }

}
