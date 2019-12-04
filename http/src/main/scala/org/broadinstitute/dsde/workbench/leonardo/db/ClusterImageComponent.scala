package org.broadinstitute.dsde.workbench.leonardo
package db

import java.sql.Timestamp
import java.time.Instant

import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterImage, ClusterImageType}
import slick.lifted.ProvenShape

case class ClusterImageRecord(clusterId: Long, imageType: ClusterImageType, imageUrl: String, timestamp: Instant)

trait ClusterImageComponent extends LeoComponent {
  this: ClusterComponent =>

  import profile.api._

  implicit def customImageTypeMapper = MappedColumnType.base[ClusterImageType, String](
    _.toString,
    s => ClusterImageType.withName(s)
  )

  implicit def timestampMapper = MappedColumnType.base[Instant, Timestamp](
    i => Timestamp.from(i),
    ts => ts.toInstant
  )

  class ClusterImageTable(tag: Tag) extends Table[ClusterImageRecord](tag, "CLUSTER_IMAGE") {
    def clusterId = column[Long]("clusterId")

    def imageType = column[ClusterImageType]("imageType", O.Length(254))

    def imageUrl = column[String]("imageUrl", O.Length(1024))

    def timestamp = column[Instant]("timestamp", O.SqlType("TIMESTAMP(6)"))

    def cluster = foreignKey("FK_CLUSTER_ID", clusterId, clusterQuery)(_.id)

    def uniqueKey = index("IDX_CLUSTER_IMAGE_UNIQUE", (clusterId, imageType), unique = true)

    def * : ProvenShape[ClusterImageRecord] =
      (clusterId, imageType, imageUrl, timestamp) <> (ClusterImageRecord.tupled, ClusterImageRecord.unapply)

    def pk = primaryKey("cluster_image_pk", (clusterId, imageType))
  }

  object clusterImageQuery extends TableQuery(new ClusterImageTable(_)) {

    def save(clusterId: Long, clusterImage: ClusterImage): DBIO[Int] =
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

    def get(clusterId: Long, imageType: ClusterImageType): DBIO[Option[ClusterImage]] =
      getRecord(clusterId, imageType).headOption
        .map(_.map(unmarshalClusterImage))

    def getRecord(clusterId: Long, imageType: ClusterImageType) =
      clusterImageQuery
        .filter { _.clusterId === clusterId }
        .filter { _.imageType === imageType }
        .result

    def getAllForCluster(clusterId: Long): DBIO[Seq[ClusterImage]] =
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
      ClusterImage(
        clusterImageRecord.imageType,
        clusterImageRecord.imageUrl,
        clusterImageRecord.timestamp
      )
  }

}
