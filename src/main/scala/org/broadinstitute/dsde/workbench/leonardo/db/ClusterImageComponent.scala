package org.broadinstitute.dsde.workbench.leonardo.db

import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterImage, ClusterTool}

case class ClusterImageRecord(clusterId: Long, tool: String, dockerImage: String)

trait ClusterImageComponent extends LeoComponent {
  this: ClusterComponent =>

  import profile.api._

  class ClusterImageTable(tag: Tag) extends Table[ClusterImageRecord](tag, "CLUSTER_IMAGE") {
    def clusterId = column[Long]("clusterId")

    def tool = column[String]("tool", O.Length(254))

    def dockerImage = column[String]("dockerImage", O.Length(1024))

    def cluster = foreignKey("FK_CLUSTER_ID", clusterId, clusterQuery)(_.id)

    def uniqueKey = index("IDX_CLUSTER_IMAGE_UNIQUE", (clusterId, tool), unique = true)

    def * = (clusterId, tool, dockerImage) <> (ClusterImageRecord.tupled, ClusterImageRecord.unapply)
  }

  object clusterImageQuery extends TableQuery(new ClusterImageTable(_)) {

    def save(clusterId: Long, clusterImage: ClusterImage): DBIO[Int] = {
      clusterImageQuery += marshallClusterImage(clusterId, clusterImage)
    }

    def saveAllForCluster(clusterId: Long, clusterImages: Seq[ClusterImage]): DBIO[Option[Int]] = {
      clusterImageQuery ++= clusterImages.map { c =>
        marshallClusterImage(clusterId, c)
      }
    }

    def get(clusterId: Long, tool: ClusterTool): DBIO[Option[ClusterImage]] = {
      clusterImageQuery
        .filter { _.clusterId === clusterId }
        .filter { _.tool === tool.toString }
        .result
        .headOption
        .map(_.map(unmarshalClusterImage))
    }

    def getAllForCluster(clusterId: Long): DBIO[Seq[ClusterImage]] = {
      clusterImageQuery
        .filter { _.clusterId === clusterId }
        .result
        .map(_.map(unmarshalClusterImage))
    }

    def marshallClusterImage(clusterId: Long, clusterImage: ClusterImage): ClusterImageRecord = {
      ClusterImageRecord(clusterId, clusterImage.tool.toString, clusterImage.dockerImage)
    }

    def unmarshalClusterImage(clusterImageRecord: ClusterImageRecord): ClusterImage = {
      ClusterImage(ClusterTool.withName(clusterImageRecord.tool), clusterImageRecord.dockerImage)
    }

  }

}
