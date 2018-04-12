package org.broadinstitute.dsde.workbench.leonardo.db

import java.sql.Timestamp

import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

/**
  * Created by rtitle on 2/13/18.
  */
case class InstanceRecord(id: Long,
                          clusterId: Long,
                          googleProject: String,
                          zone: String,
                          name: String,
                          googleId: BigDecimal,
                          status: String,
                          ip: Option[String],
                          dataprocRole: Option[String],
                          createdDate: Timestamp)

trait InstanceComponent extends LeoComponent {
  this: ClusterComponent =>

  import profile.api._

  class InstanceTable(tag: Tag) extends Table[InstanceRecord](tag, "INSTANCE") {
    def id =            column[Long]              ("id",            O.PrimaryKey, O.AutoInc)
    def clusterId =     column[Long]              ("clusterId")
    def googleProject = column[String]            ("googleProject", O.Length(254))
    def zone =          column[String]            ("zone",          O.Length(254))
    def name =          column[String]            ("name",          O.Length(254))
    def googleId =      column[BigDecimal]        ("googleId")
    def status =        column[String]            ("status",        O.Length(254))
    def ip =            column[Option[String]]    ("ip",            O.Length(254))
    def dataprocRole =  column[Option[String]]    ("dataprocRole",  O.Length(254))
    def createdDate =   column[Timestamp]         ("createdDate",   O.SqlType("TIMESTAMP(6)"))

    def uniqueKey = index("IDX_INSTANCE_UNIQUE", (clusterId, googleProject, zone, name), unique = true)
    def cluster = foreignKey("FK_INSTANCE_CLUSTER_ID", clusterId, clusterQuery)(_.id)

    def * = (id, clusterId, googleProject, zone, name, googleId, status, ip, dataprocRole, createdDate) <> (InstanceRecord.tupled, InstanceRecord.unapply)
  }

  object instanceQuery extends TableQuery(new InstanceTable(_)) {

    def save(clusterId: Long, instance: Instance): DBIO[Int] = {
      instanceQuery += marshalInstance(clusterId, instance)
    }

    def saveAllForCluster(clusterId: Long, instances: Seq[Instance]) = {
      instanceQuery ++= instances map { marshalInstance(clusterId, _) }
    }

    def mergeForCluster(clusterId: Long, instances: Seq[Instance]): DBIO[Int] = {
      for {
        // upsert all incoming instances passed to this method
        upserted <- upsertAllForCluster(clusterId, instances)

        // delete all instances that exist in the DB but were NOT passed to this method
        existing <- getAllForCluster(clusterId)
        instancesToDelete = {
          val incoming = instances.map(_.googleId).toSet
          existing.filterNot(i => incoming.contains(i.googleId))
        }
        deleted <- deleteAllForCluster(clusterId, instancesToDelete)

      } yield upserted + deleted
    }

    def upsert(clusterId: Long, instance: Instance): DBIO[Int] = {
      instanceQuery.insertOrUpdate(marshalInstance(clusterId, instance))
    }

    def delete(instance: Instance): DBIO[Int] = {
      instanceByKeyQuery(instance.key).delete
    }

    def upsertAllForCluster(clusterId: Long, instances: Seq[Instance]): DBIO[Int] = {
      DBIO.fold(instances map { upsert(clusterId, _) }, 0)(_ + _)
    }

    def deleteAllForCluster(clusterId: Long, instances: Seq[Instance]): DBIO[Int] = {
      DBIO.fold(instances map { delete }, 0)(_ + _)
    }

    def getAllForCluster(clusterId: Long): DBIO[Seq[Instance]] = {
      instanceQuery.filter { _.clusterId === clusterId}.result map { recs =>
        recs.map(unmarshalInstance)
      }
    }

    def instanceByKeyQuery(instanceKey: InstanceKey) = {
      instanceQuery.filter { _.googleProject === instanceKey.project.value }
        .filter { _.zone === instanceKey.zone.value }
        .filter { _.name === instanceKey.name.value }
    }

    def getInstanceByKey(instanceKey: InstanceKey): DBIO[Option[Instance]] = {
      instanceByKeyQuery(instanceKey).result.map { _.headOption.map(unmarshalInstance) }
    }

    def updateStatusAndIpForCluster(clusterId: Long, newStatus: InstanceStatus, newIp: Option[IP]) = {
      instanceQuery.filter { _.clusterId === clusterId }
        .map(inst => (inst.status, inst.ip))
        .update(newStatus.entryName, newIp.map(_.value))
    }

    private def marshalInstance(clusterId: Long, instance: Instance): InstanceRecord = {
      InstanceRecord(
        id = 0,    // DB AutoInc
        clusterId,
        googleProject = instance.key.project.value,
        zone = instance.key.zone.value,
        name = instance.key.name.value,
        googleId = BigDecimal(instance.googleId),
        status = instance.status.entryName,
        ip = instance.ip.map(_.value),
        dataprocRole = instance.dataprocRole.map(_.entryName),
        createdDate = Timestamp.from(instance.createdDate)
      )
    }

    private[db] def unmarshalInstance(record: InstanceRecord): Instance = {
      Instance(
        InstanceKey(
          GoogleProject(record.googleProject),
          ZoneUri(record.zone),
          InstanceName(record.name)
        ),
        record.googleId.toBigInt,
        InstanceStatus.withName(record.status),
        record.ip map IP,
        record.dataprocRole.map(DataprocRole.withName),
        record.createdDate.toInstant
      )
    }
  }

}