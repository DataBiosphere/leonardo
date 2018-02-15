package org.broadinstitute.dsde.workbench.leonardo.db

import java.sql.Timestamp
import java.time.Instant

import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

/**
  * Created by rtitle on 2/13/18.
  */
// TODO should track dataproc role in this table
case class InstanceRecord(id: Long,
                          clusterId: Long,
                          googleProject: String,
                          zone: String,
                          instanceName: String,
                          googleId: BigDecimal,
                          status: String,
                          ip: Option[String],
                          dataprocRole: Option[String],
                          createdDate: Timestamp,
                          destroyedDate: Option[Timestamp])

trait InstanceComponent extends LeoComponent {
  this: ClusterComponent =>

  import profile.api._

  class InstanceTable(tag: Tag) extends Table[InstanceRecord](tag, "LABEL") {
    def id =            column[Long]              ("id",            O.PrimaryKey, O.AutoInc)
    def clusterId =     column[Long]              ("clusterId")
    def googleProject = column[String]            ("googleProject", O.Length(254))
    def zone =          column[String]            ("zone",          O.Length(254))
    def instanceName =  column[String]            ("name",          O.Length(254))
    def googleId =      column[BigDecimal]        ("googleId")
    def status =        column[String]            ("status",        O.Length(254))
    def ip =            column[Option[String]]    ("ip",            O.Length(254))
    def dataprocRole =  column[Option[String]]    ("dataprocRole",  O.Length(254))
    def createdDate =   column[Timestamp]         ("createdDate",   O.SqlType("TIMESTAMP(6)"))
    def destroyedDate = column[Option[Timestamp]] ("destroyedDate", O.SqlType("TIMESTAMP(6)"))

    // TODO add destoyed date?
    def uniqueKey = index("IDX_INSTANCE_UNIQUE", (googleProject, zone, instanceName), unique = true)
    def cluster = foreignKey("FK_CLUSTER_ID", clusterId, clusterQuery)(_.id)

    def * = (id, clusterId, googleProject, zone, instanceName, googleId, status, ip, dataprocRole, createdDate, destroyedDate) <> (InstanceRecord.tupled, InstanceRecord.unapply)
  }

  object instanceQuery extends TableQuery(new InstanceTable(_)) {

    def save(clusterId: Long, instance: Instance): DBIO[Int] = {
      instanceQuery += marshalInstance(clusterId, instance)
    }

    def saveAllForCluster(clusterId: Long, instances: Seq[Instance]) = {
      instanceQuery ++= instances map { marshalInstance(clusterId, _) }
    }

    def upsert(clusterId: Long, instance: Instance): DBIO[Int] = {
      instanceQuery.insertOrUpdate(marshalInstance(clusterId, instance))
    }

    def upsertAllForCluster(clusterId: Long, instances: Seq[Instance]) = {
      DBIO.sequence(instances map { upsert(clusterId, _) })
    }

    def getAllForCluster(clusterId: Long): DBIO[Seq[Instance]] = {
      instanceQuery.filter { _.clusterId === clusterId}.result map { recs =>
        recs.map(unmarshalInstance)
      }
    }

    def instanceByKeyQuery(instanceKey: InstanceKey) = {
      instanceQuery.filter { _.googleProject === instanceKey.project.value }
        .filter { _.zone === instanceKey.zone.value }
        .filter { _.instanceName === instanceKey.name.value }
    }

    def getInstanceByKey(instanceKey: InstanceKey): DBIO[Option[Instance]] = {
      instanceByKeyQuery(instanceKey).result.map { _.headOption.map(unmarshalInstance) }
    }

    def updateStatusAndIp(instanceKey: InstanceKey, newStatus: InstanceStatus, newIp: Option[IP]) = {
      instanceByKeyQuery(instanceKey).map(inst => (inst.status, inst.ip)).update(newStatus.entryName, newIp.map(_.value))
    }

    def updateStatusAndIpForCluster(clusterId: Long, newStatus: InstanceStatus, newIp: Option[IP]) = {
      instanceQuery.filter { _.clusterId === clusterId }
        .map(inst => (inst.status, inst.ip))
        .update(newStatus.entryName, newIp.map(_.value))
    }

    def markPendingDeletionForCluster(clusterId: Long) = {
      instanceQuery.filter { _.clusterId === clusterId }
        .map(inst => (inst.status, inst.ip, inst.destroyedDate))
        .update(InstanceStatus.Deleting.entryName, None, Some(Timestamp.from(Instant.now())))
    }

    def completeDeletionForCluster(clusterId: Long) = {
      instanceQuery.filter { _.clusterId === clusterId }
        .map(_.status)
        .update(InstanceStatus.Deleted.entryName)
    }

    private def marshalInstance(clusterId: Long, instance: Instance): InstanceRecord = {
      InstanceRecord(
        id = 0,    // DB AutoInc
        clusterId,
        googleProject = instance.key.project.value,
        zone = instance.key.zone.value,
        instanceName = instance.key.name.value,
        googleId = BigDecimal(instance.googleId),
        status = instance.status.entryName,
        ip = instance.ip.map(_.value),
        dataprocRole = instance.dataprocRole.map(_.entryName),
        createdDate = Timestamp.from(instance.createdDate),
        destroyedDate = instance.destroyedDate.map(Timestamp.from)
      )
    }

    private[db] def unmarshalInstance(record: InstanceRecord): Instance = {
      Instance(
        InstanceKey(
          GoogleProject(record.googleProject),
          ZoneUri(record.zone),
          InstanceName(record.instanceName)
        ),
        record.googleId.toBigInt,
        InstanceStatus.withName(record.status),
        record.ip map IP,
        record.dataprocRole.map(DataprocRole.withName),
        record.createdDate.toInstant,
        record.destroyedDate.map(_.toInstant) // TODO
      )
    }
  }

}