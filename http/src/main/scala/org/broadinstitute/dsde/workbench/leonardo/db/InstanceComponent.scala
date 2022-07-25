package org.broadinstitute.dsde.workbench.leonardo
package db

import java.sql.{SQLDataException, Timestamp}

import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import LeoProfile.api._
import org.broadinstitute.dsde.workbench.google2.{DataprocRole, ZoneName}
import org.broadinstitute.dsde.workbench.model.IP
import org.broadinstitute.dsde.workbench.util2.InstanceName

import scala.concurrent.ExecutionContext

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
                          createdDate: Timestamp
)

class InstanceTable(tag: Tag) extends Table[InstanceRecord](tag, "INSTANCE") {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
  def clusterId = column[Long]("clusterId")
  def googleProject = column[String]("googleProject", O.Length(254))
  def zone = column[String]("zone", O.Length(254))
  def name = column[String]("name", O.Length(254))
  def googleId = column[BigDecimal]("googleId")
  def status = column[String]("status", O.Length(254))
  def ip = column[Option[String]]("ip", O.Length(254))
  def dataprocRole = column[Option[String]]("dataprocRole", O.Length(254))
  def createdDate = column[Timestamp]("createdDate", O.SqlType("TIMESTAMP(6)"))

  def uniqueKey = index("IDX_INSTANCE_UNIQUE", (clusterId, googleProject, zone, name), unique = true)
  def cluster = foreignKey("FK_INSTANCE_CLUSTER_ID", clusterId, clusterQuery)(_.id)

  def * =
    (id,
     clusterId,
     googleProject,
     zone,
     name,
     googleId,
     status,
     ip,
     dataprocRole,
     createdDate
    ) <> (InstanceRecord.tupled, InstanceRecord.unapply)
}

object instanceQuery extends TableQuery(new InstanceTable(_)) {

  def save(clusterId: Long, instance: DataprocInstance): DBIO[Int] =
    instanceQuery += marshalInstance(clusterId, instance)

  def saveAllForCluster(clusterId: Long, instances: Seq[DataprocInstance]) =
    instanceQuery ++= instances map { marshalInstance(clusterId, _) }

  def upsert(clusterId: Long, instance: DataprocInstance): DBIO[Int] =
    instanceQuery.insertOrUpdate(marshalInstance(clusterId, instance))

  def delete(instance: DataprocInstance): DBIO[Int] =
    instanceByKeyQuery(instance.key).delete

  def upsertAllForCluster(clusterId: Long, instances: Seq[DataprocInstance])(implicit ec: ExecutionContext): DBIO[Int] =
    DBIO.fold(instances map { upsert(clusterId, _) }, 0)(_ + _)

  def deleteAllForCluster(clusterId: Long, instances: Seq[DataprocInstance])(implicit ec: ExecutionContext): DBIO[Int] =
    DBIO.fold(instances map delete, 0)(_ + _)

  def getMasterForCluster(clusterId: Long)(implicit ec: ExecutionContext): DBIO[DataprocInstance] =
    instanceQuery
      .filter(_.clusterId === clusterId)
      .filter(_.dataprocRole === Option(DataprocRole.Master.toString))
      .result map { recs =>
      recs.headOption
        .map(unmarshalInstance)
        .getOrElse(throw new SQLDataException(s"no master instance found for ${clusterId}"))
    }

  def instanceByKeyQuery(instanceKey: DataprocInstanceKey) =
    instanceQuery
      .filter(_.googleProject === instanceKey.project.value)
      .filter(_.zone === instanceKey.zone.value)
      .filter(_.name === instanceKey.name.value)

  def getInstanceByKey(
    instanceKey: DataprocInstanceKey
  )(implicit ec: ExecutionContext): DBIO[Option[DataprocInstance]] =
    instanceByKeyQuery(instanceKey).result.map(_.headOption.map(unmarshalInstance))

  def updateStatusAndIpForCluster(clusterId: Long, newStatus: GceInstanceStatus, newIp: Option[IP]) =
    instanceQuery
      .filter(_.clusterId === clusterId)
      .map(inst => (inst.status, inst.ip))
      .update(newStatus.entryName, newIp.map(_.asString))

  private def marshalInstance(clusterId: Long, instance: DataprocInstance): InstanceRecord =
    InstanceRecord(
      id = 0, // DB AutoInc
      clusterId,
      googleProject = instance.key.project.value,
      zone = instance.key.zone.value,
      name = instance.key.name.value,
      googleId = BigDecimal(instance.googleId),
      status = instance.status.entryName,
      ip = instance.ip.map(_.asString),
      dataprocRole = Some(instance.dataprocRole.toString),
      createdDate = Timestamp.from(instance.createdDate)
    )

  private[db] def unmarshalInstance(record: InstanceRecord): DataprocInstance =
    DataprocInstance(
      DataprocInstanceKey(
        GoogleProject(record.googleProject),
        ZoneName(record.zone),
        InstanceName(record.name)
      ),
      record.googleId.toBigInt,
      GceInstanceStatus.withName(record.status),
      record.ip map IP,
      record.dataprocRole
        .toRight(new Exception("dataproc role is null"))
        .flatMap(s => DataprocRole.stringToDataprocRole.get(s).toRight(new Exception(s"invalid dataproc role ${s}")))
        .fold[DataprocRole](e => throw e, identity),
      record.createdDate.toInstant
    )
}
