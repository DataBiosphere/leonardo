package org.broadinstitute.dsde.workbench.leonardo.db

import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.NamespaceName
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.mappedColumnImplicits._
import org.broadinstitute.dsde.workbench.leonardo.{KubernetesClusterLeoId, Namespace, NamespaceId}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import slick.lifted.Tag

import scala.concurrent.ExecutionContext

final case class NamespaceRecord(id: NamespaceId,
                                 clusterId: KubernetesClusterLeoId,
                                 namespaceName: NamespaceName,
                                 creator: WorkbenchEmail)

class NamespaceTable(tag: Tag) extends Table[NamespaceRecord](tag, "NAMESPACE") {
  def id = column[NamespaceId]("id", O.PrimaryKey, O.AutoInc)
  def clusterId = column[KubernetesClusterLeoId]("clusterId")
  def namespaceName = column[NamespaceName]("namespaceName", O.Length(254))
  def creator = column[WorkbenchEmail]("creator", O.Length(254))

  def * = (id, clusterId, namespaceName, creator) <> (NamespaceRecord.tupled, NamespaceRecord.unapply)
  def cluster = foreignKey("FK_NAMESPACE_CLUSTER_ID", clusterId, kubernetesClusterQuery)(_.id)
}

object namespaceQuery extends TableQuery(new NamespaceTable(_)) {

  def save(clusterId: KubernetesClusterLeoId, namespace: NamespaceName, creator: WorkbenchEmail): DBIO[NamespaceId] =
    namespaceQuery returning namespaceQuery.map(_.id) +=
      NamespaceRecord(
        NamespaceId(0), //AutoInc
        clusterId,
        namespace,
        creator
      )

//  def saveAllForCluster(clusterId: KubernetesClusterLeoId,
//                        namespaces: List[NamespaceName])(implicit ec: ExecutionContext): DBIO[Unit] =
//    (namespaceQuery ++= namespaces.map(name =>
//      NamespaceRecord(
//        NamespaceId(0),
//        clusterId,
//        NamespaceName(name.value),
//        WorkbenchEmail("dummy")
//      )
//    ))
//    //the option[int] that this returns is fairly useless, as it doesn't represent the number of records inserted and in practice we .void it anyways
//      .map(_ => ())

  def delete(clusterId: KubernetesClusterLeoId, namespace: NamespaceName): DBIO[Int] =
    namespaceQuery
      .filter(_.clusterId === clusterId)
      .filter(_.namespaceName === namespace)
      .delete

  def deleteAllForCluster(clusterId: KubernetesClusterLeoId): DBIO[Int] =
    namespaceQuery
      .filter(_.clusterId === clusterId)
      .delete

  def getForUser(clusterId: KubernetesClusterLeoId,
                 user: WorkbenchEmail)(implicit ec: ExecutionContext): DBIO[Option[Namespace]] =
    namespaceQuery
      .filter(_.clusterId === clusterId)
      .filter(_.creator === user)
      .result
      .map(_.map(unmarshalNamespace).headOption)

  def getByName(clusterId: KubernetesClusterLeoId,
                name: NamespaceName)(implicit ec: ExecutionContext): DBIO[Option[Namespace]] =
    namespaceQuery
      .filter(_.clusterId === clusterId)
      .filter(_.namespaceName === name)
      .result
      .map(_.map(unmarshalNamespace).headOption)

//  def getAllForCluster(
//    clusterId: KubernetesClusterLeoId
//  )(implicit ec: ExecutionContext): DBIO[List[Namespace]] =
//    namespaceQuery
//      .filter(_.clusterId === clusterId)
//      .result
//      .map(rowOpt => rowOpt.map(row => Namespace(row.id, row.namespaceName)).toList)

  private[db] def unmarshalNamespace(rec: NamespaceRecord): Namespace =
    Namespace(rec.id, rec.namespaceName, rec.creator)
}
