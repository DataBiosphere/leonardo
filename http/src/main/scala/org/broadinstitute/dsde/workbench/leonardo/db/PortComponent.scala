package org.broadinstitute.dsde.workbench.leonardo.db

import org.broadinstitute.dsde.workbench.leonardo.{PortId, ServiceId}
import slick.lifted.Tag
import LeoProfile.api._
import LeoProfile.mappedColumnImplicits._
import org.broadinstitute.dsde.workbench.google2.KubernetesModels.{
  PortName,
  PortNum,
  Protocol,
  ServicePort,
  TargetPortNum
}

import scala.concurrent.ExecutionContext

final case class PortRecord(id: PortId,
                            serviceId: ServiceId,
                            portName: PortName,
                            portNum: PortNum,
                            targetPortNum: TargetPortNum,
                            protocol: Protocol)

class PortTable(tag: Tag) extends Table[PortRecord](tag, "PORT") {
  def id = column[PortId]("id", O.AutoInc, O.PrimaryKey)
  def serviceId = column[ServiceId]("serviceId")
  def portName = column[PortName]("portName", O.Length(254))
  def portNum = column[PortNum]("portNum")
  def targetPortNum = column[TargetPortNum]("targetPortNum")
  def protocol = column[Protocol]("protocol", O.Length(254))

  def * = (id, serviceId, portName, portNum, targetPortNum, protocol) <> (PortRecord.tupled, PortRecord.unapply)
}

object portQuery extends TableQuery(new PortTable(_)) {

  def saveForService(serviceId: ServiceId, port: ServicePort)(implicit ec: ExecutionContext): DBIO[ServicePort] =
    for {
      portId <- portQuery returning portQuery.map(_.id) += PortRecord(PortId(-1),
                                                                      serviceId,
                                                                      port.name,
                                                                      port.num,
                                                                      port.targetPort,
                                                                      port.protocol)
    } yield port

  def unmarshalPort(rec: PortRecord): ServicePort =
                   ServicePort(
                     rec.portNum,
                     rec.portName,
                     rec.targetPortNum,
                     rec.protocol
                   )
}
