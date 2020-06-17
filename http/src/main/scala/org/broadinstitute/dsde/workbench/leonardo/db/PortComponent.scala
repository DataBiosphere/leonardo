package org.broadinstitute.dsde.workbench.leonardo.db

import org.broadinstitute.dsde.workbench.leonardo.{KubernetesPort, KubernetesService, PortId, ServiceId}
import slick.lifted.Tag
import LeoProfile.api._
import LeoProfile.mappedColumnImplicits._
import cats.implicits._
import com.rms.miu.slickcats.DBIOInstances._
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

  def saveAllForService(service: KubernetesService)(implicit ec: ExecutionContext): DBIO[List[KubernetesPort]] =
    service.config.ports.traverse(p => saveForService(service.id, p))

  def saveForService(serviceId: ServiceId, port: KubernetesPort)(implicit ec: ExecutionContext): DBIO[KubernetesPort] =
    for {
      portId <- portQuery returning portQuery.map(_.id) += PortRecord(port.id,
                                                                      serviceId,
                                                                      port.servicePort.name,
                                                                      port.servicePort.num,
                                                                      port.servicePort.targetPort,
                                                                      port.servicePort.protocol)
    } yield port.copy(id = portId)

  def unmarshalPort(rec: PortRecord): KubernetesPort =
    KubernetesPort(rec.id,
                   ServicePort(
                     rec.portNum,
                     rec.portName,
                     rec.targetPortNum,
                     rec.protocol
                   ))
}
