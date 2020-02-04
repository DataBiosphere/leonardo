package org.broadinstitute.dsde.workbench.leonardo
package db

import java.time.Instant

import LeoProfile.api._
import LeoProfile.mappedColumnImplicits._
import slick.lifted.ProvenShape

import scala.concurrent.ExecutionContext

case class ClusterImageRecord(clusterId: Long, imageType: RuntimeImageType, imageUrl: String, timestamp: Instant)

class ClusterImageTable(tag: Tag) extends Table[ClusterImageRecord](tag, "CLUSTER_IMAGE") {
  def clusterId = column[Long]("clusterId")

  def imageType = column[RuntimeImageType]("imageType", O.Length(254))

  def imageUrl = column[String]("imageUrl", O.Length(1024))

  def timestamp = column[Instant]("timestamp", O.SqlType("TIMESTAMP(6)"))

  def cluster = foreignKey("FK_CLUSTER_ID", clusterId, clusterQuery)(_.id)

  def uniqueKey = index("IDX_CLUSTER_IMAGE_UNIQUE", (clusterId, imageType), unique = true)

  def * : ProvenShape[ClusterImageRecord] =
    (clusterId, imageType, imageUrl, timestamp) <> (ClusterImageRecord.tupled, ClusterImageRecord.unapply)

  def pk = primaryKey("cluster_image_pk", (clusterId, imageType))
}

object clusterImageQuery extends TableQuery(new ClusterImageTable(_)) {

  def save(clusterId: Long, clusterImage: ClusterImage)(implicit ec: ExecutionContext): DBIO[Int] =
    for {
      exists <- getRecord(clusterId, clusterImage.imageType)
      res <- if (exists.headOption.isDefined) DBIO.successful(0)
      else clusterImageQuery += marshallClusterImage(clusterId, clusterImage)
    } yield res

  def saveAllForCluster(clusterId: Long, clusterImages: Seq[ClusterImage]): DBIO[Option[Int]] =
    clusterImageQuery ++= clusterImages.map { c =>
      marshallClusterImage(clusterId, c)
    }

  def upsert(clusterId: Long, clusterImage: ClusterImage): DBIO[Int] =
    clusterImageQuery.insertOrUpdate(marshallClusterImage(clusterId, clusterImage))

  def get(clusterId: Long, imageType: RuntimeImageType)(implicit ec: ExecutionContext): DBIO[Option[ClusterImage]] =
    getRecord(clusterId, imageType).headOption
      .map(_.map(unmarshalClusterImage))

  def getRecord(clusterId: Long, imageType: RuntimeImageType) =
    clusterImageQuery
      .filter { _.clusterId === clusterId }
      .filter { _.imageType === imageType }
      .result

  def getAllForCluster(clusterId: Long)(implicit ec: ExecutionContext): DBIO[Seq[ClusterImage]] =
    clusterImageQuery
      .filter { _.clusterId === clusterId }
      .result
      .map(_.map(unmarshalClusterImage))

  def marshallClusterImage(clusterId: Long, clusterImage: ClusterImage): ClusterImageRecord =
    ClusterImageRecord(
      clusterId,
      clusterImage.imageType,
      clusterImage.imageUrl,
      clusterImage.timestamp
    )

  def unmarshalClusterImage(clusterImageRecord: ClusterImageRecord): ClusterImage =
    RuntimeImage(
      clusterImageRecord.imageType,
      clusterImageRecord.imageUrl,
      clusterImageRecord.timestamp
    )
}
