package org.broadinstitute.dsde.workbench.leonardo.db

import org.broadinstitute.dsde.workbench.leonardo.{
  AppId,
  KubernetesService,
  KubernetesServiceKindName,
  ServiceConfig,
  ServiceId
}
import slick.lifted.Tag
import LeoProfile.api._
import LeoProfile.mappedColumnImplicits._
import cats.implicits._
import com.rms.miu.slickcats.DBIOInstances._
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName._

import scala.concurrent.ExecutionContext

final case class ServiceRecord(id: ServiceId,
                               appId: AppId,
                               serviceName: ServiceName,
                               serviceKind: KubernetesServiceKindName)

class ServiceTable(tag: Tag) extends Table[ServiceRecord](tag, "SERVICE") {
  def id = column[ServiceId]("id", O.AutoInc, O.PrimaryKey)
  def appId = column[AppId]("appId")
  def serviceName = column[ServiceName]("serviceName", O.Length(254))
  def serviceKind = column[KubernetesServiceKindName]("serviceKind", O.Length(254))

  override def * = (id, appId, serviceName, serviceKind) <> (ServiceRecord.tupled, ServiceRecord.unapply)
}

object serviceQuery extends TableQuery(new ServiceTable(_)) {

  def saveAllForApp(appId: AppId,
                    services: List[KubernetesService])(implicit ec: ExecutionContext): DBIO[List[KubernetesService]] =
    services.traverse(s => saveForApp(appId, s))

  def saveForApp(appId: AppId, service: KubernetesService)(implicit ec: ExecutionContext): DBIO[KubernetesService] =
    for {
      serviceId <- serviceQuery returning serviceQuery.map(_.id) += ServiceRecord(ServiceId(-1),
                                                                                  appId,
                                                                                  service.config.name,
                                                                                  service.config.kind)
      ports <- portQuery.saveAllForService(service.copy(id = serviceId))
    } yield service.copy(id = serviceId, service.config.copy(ports = ports))

  private[db] def unmarshalService(rec: ServiceRecord, ports: List[PortRecord]): KubernetesService =
    KubernetesService(
      rec.id,
      ServiceConfig(
        rec.serviceName,
        rec.serviceKind,
        ports.map(p => portQuery.unmarshalPort(p))
      )
    )

}
