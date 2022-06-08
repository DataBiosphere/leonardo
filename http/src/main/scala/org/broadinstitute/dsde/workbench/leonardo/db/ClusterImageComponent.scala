package org.broadinstitute.dsde.workbench.leonardo
package db

import java.time.Instant
import LeoProfile.api._
import LeoProfile.mappedColumnImplicits._
import slick.lifted.ProvenShape

import java.nio.file.Path
import scala.concurrent.ExecutionContext

case class ClusterImageRecord(clusterId: Long,
                              imageType: RuntimeImageType,
                              imageUrl: String,
                              homeDirectory: Option[Path],
                              timestamp: Instant
)

class ClusterImageTable(tag: Tag) extends Table[ClusterImageRecord](tag, "CLUSTER_IMAGE") {
  def clusterId = column[Long]("clusterId")

  def imageType = column[RuntimeImageType]("imageType", O.Length(254))

  def imageUrl = column[String]("imageUrl", O.Length(1024))

  def timestamp = column[Instant]("timestamp", O.SqlType("TIMESTAMP(6)"))
  def homeDirectory = column[Option[Path]]("homeDirectory", O.Length(254))

  def cluster = foreignKey("FK_CLUSTER_ID", clusterId, clusterQuery)(_.id)

  def uniqueKey = index("IDX_CLUSTER_IMAGE_UNIQUE", (clusterId, imageType), unique = true)

  def * : ProvenShape[ClusterImageRecord] =
    (clusterId,
     imageType,
     imageUrl,
     homeDirectory,
     timestamp
    ) <> (ClusterImageRecord.tupled, ClusterImageRecord.unapply)

  def pk = primaryKey("cluster_image_pk", (clusterId, imageType))
}

object clusterImageQuery extends TableQuery(new ClusterImageTable(_)) {

  def save(clusterId: Long, clusterImage: RuntimeImage)(implicit ec: ExecutionContext): DBIO[Int] =
    for {
      exists <- getRecord(clusterId, clusterImage.imageType)
      res <-
        if (exists.headOption.isDefined) DBIO.successful(0)
        else clusterImageQuery += marshallClusterImage(clusterId, clusterImage)
    } yield res

  def saveAllForCluster(clusterId: Long, clusterImages: Seq[RuntimeImage]): DBIO[Option[Int]] =
    clusterImageQuery ++= clusterImages.map(c => marshallClusterImage(clusterId, c))

  def upsert(clusterId: Long, clusterImage: RuntimeImage): DBIO[Int] =
    clusterImageQuery.insertOrUpdate(marshallClusterImage(clusterId, clusterImage))

  def get(clusterId: Long, imageType: RuntimeImageType)(implicit ec: ExecutionContext): DBIO[Option[RuntimeImage]] =
    getRecord(clusterId, imageType).headOption.map(_.map(unmarshalClusterImage))

  def getRecord(clusterId: Long, imageType: RuntimeImageType) =
    clusterImageQuery
      .filter(_.clusterId === clusterId)
      .filter(_.imageType === imageType)
      .result

  def getAllForCluster(clusterId: Long)(implicit ec: ExecutionContext): DBIO[Seq[RuntimeImageType]] =
    clusterImageQuery
      .filter(_.clusterId === clusterId)
      .result
      .map(_.map(_.imageType))

  def getAllImagesForCluster(clusterId: Long)(implicit ec: ExecutionContext): DBIO[Seq[RuntimeImage]] =
    clusterImageQuery
      .filter(_.clusterId === clusterId)
      .result
      .map(_.map(x => unmarshalClusterImage(x)))

  def marshallClusterImage(clusterId: Long, clusterImage: RuntimeImage): ClusterImageRecord =
    ClusterImageRecord(
      clusterId,
      clusterImage.imageType,
      clusterImage.imageUrl,
      clusterImage.homeDirectory,
      clusterImage.timestamp
    )

  def unmarshalClusterImage(clusterImageRecord: ClusterImageRecord): RuntimeImage =
    RuntimeImage(
      clusterImageRecord.imageType,
      clusterImageRecord.imageUrl,
      clusterImageRecord.homeDirectory,
      clusterImageRecord.timestamp
    )
}
