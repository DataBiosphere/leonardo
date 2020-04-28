package org.broadinstitute.dsde.workbench.leonardo.db

import LeoProfile.api._
import LeoProfile.mappedColumnImplicits._
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.KubernetesNamespaceName
import org.broadinstitute.dsde.workbench.leonardo.{KubernetesClusterLeoId, KubernetesNamespaceId}
import slick.lifted.Tag

import scala.concurrent.ExecutionContext

case class NamespaceRecord(id: KubernetesNamespaceId,
                           clusterId: KubernetesClusterLeoId,
                           namespaceName: KubernetesNamespaceName)


class NamespaceTable(tag: Tag) extends Table[NamespaceRecord](tag, "NAMESPACE") {
  def id = column[KubernetesNamespaceId]("id", O.PrimaryKey, O.AutoInc)
  def clusterId = column[KubernetesClusterLeoId]("clusterId")
  def namespaceName = column[KubernetesNamespaceName]("namespaceName", O.Length(254))

  def * = (id, clusterId, namespaceName) <> (NamespaceRecord.tupled, NamespaceRecord.unapply)
  def cluster = foreignKey("FK_NAMESPACE_CLUSTER_ID", clusterId, kubernetesClusterQuery)(_.id)
}

object namespaceQuery extends TableQuery(new NamespaceTable(_)) {

  def save(clusterId: KubernetesClusterLeoId, namespace: KubernetesNamespaceName): DBIO[Int] =
    namespaceQuery +=
      NamespaceRecord(
        KubernetesNamespaceId(0), //AutoInc
        clusterId,
        KubernetesNamespaceName(namespace.value)
      )

  def saveAllForCluster(clusterId: KubernetesClusterLeoId, namespaces: Set[KubernetesNamespaceName])
                       (implicit ec: ExecutionContext): DBIO[Unit] =
    (namespaceQuery ++= namespaces.map(name => NamespaceRecord(
        KubernetesNamespaceId(0),
        clusterId,
        KubernetesNamespaceName(name.value)
      )))
      //the option[int] that this returns is fairly useless, as it doesn't represent the number of records inserted and in practice we .void it anyways
    .map(_ => ())


  def delete(clusterId: KubernetesClusterLeoId, namespace: KubernetesNamespaceName): DBIO[Int] =
    namespaceQuery
      .filter(_.clusterId === clusterId)
      .filter(_.namespaceName === namespace)
      .delete

  def deleteAllForCluster(clusterId: KubernetesClusterLeoId): DBIO[Int] =
    namespaceQuery
      .filter(_.clusterId === clusterId)
      .delete

  def getAllForCluster(clusterId: KubernetesClusterLeoId)
                      (implicit ec: ExecutionContext): DBIO[Set[KubernetesNamespaceName]] =
    namespaceQuery
    .filter(_.clusterId === clusterId)
    .result
    .map(rowOpt =>
      rowOpt.map(row =>
        row.namespaceName
      )
        .toSet
    )

}
