package org.broadinstitute.dsde.workbench.leonardo.db

import org.broadinstitute.dsde.workbench.leonardo.model.{ExtensionType, UserJupyterExtensionConfig}

case class ExtensionRecord(clusterId: Long, extensionType: String, name: String, value: String)

trait ExtensionComponent extends LeoComponent {
  this: ClusterComponent =>

  import profile.api._

  class ExtensionTable(tag: Tag) extends Table[ExtensionRecord](tag, "NOTEBOOK_EXTENSIONS") {
    def clusterId = column[Long]("clusterId")

    def extensionType = column[String]("extensionType", O.Length(254))

    def name = column[String]("name", O.Length(254))

    def path = column[String]("path", O.Length(1024))

    def cluster = foreignKey("FK_CLUSTER_ID", clusterId, clusterQuery)(_.id)

    def uniqueKey = index("IDX_LABEL_UNIQUE", (clusterId, extensionType, name), unique = true)

    def * = (clusterId, extensionType, name, path) <> (ExtensionRecord.tupled, ExtensionRecord.unapply)
  }

  object extensionQuery extends TableQuery(new ExtensionTable(_)) {

    def save(clusterId: Long, extensionType: String, name: String, value: String): DBIO[Int] = {
      extensionQuery += ExtensionRecord(clusterId, extensionType, name, value)
    }

    def saveAllForCluster(clusterId: Long, userJupyterExtensionConfig: Option[UserJupyterExtensionConfig]) = {
      userJupyterExtensionConfig match {
        case Some(ext) => extensionQuery ++= marshallExtensions(clusterId, ext)
        case None => DBIO.successful(0)
      }
    }

    def getAllForCluster(clusterId: Long): DBIO[UserJupyterExtensionConfig] = {
      extensionQuery.filter {
        _.clusterId === clusterId
      }.result map { recs =>
        val nbExtensions = (recs.filter(_.extensionType == ExtensionType.NBExtension.toString) map { rec => rec.name -> rec.value }).toMap
        val serverExtensions = (recs.filter(_.extensionType == ExtensionType.ServerExtension.toString) map { rec => rec.name -> rec.value }).toMap
        val combinedExtensions = (recs.filter(_.extensionType == ExtensionType.CombinedExtension.toString) map { rec => rec.name -> rec.value }).toMap
        val labExtensions = (recs.filter(_.extensionType == ExtensionType.LabExtension.toString) map {rec => rec.name -> rec.value}).toMap
        UserJupyterExtensionConfig(nbExtensions, serverExtensions, combinedExtensions, labExtensions)
      }
    }

    def marshallExtensions(clusterId:Long, userJupyterExtensionConfig: UserJupyterExtensionConfig): List[ExtensionRecord] = {
      ((userJupyterExtensionConfig.nbExtensions map { case(key, value) => ExtensionRecord(clusterId, ExtensionType.NBExtension.toString, key, value)}) ++
      (userJupyterExtensionConfig.serverExtensions map { case(key, value) => ExtensionRecord(clusterId, ExtensionType.ServerExtension.toString, key, value)}) ++
      (userJupyterExtensionConfig.combinedExtensions map { case(key, value) => ExtensionRecord(clusterId, ExtensionType.CombinedExtension.toString, key, value)}) ++
      (userJupyterExtensionConfig.labExtensions map {case(key, value) => ExtensionRecord(clusterId, ExtensionType.LabExtension.toString, key, value)})).toList
    }

    def unmarshallExtensions(extList: List[ExtensionRecord]): Option[UserJupyterExtensionConfig] = {
      if (extList.isEmpty) {
        None
      } else {
        val nbExtension = extList.filter(_.extensionType == ExtensionType.NBExtension.toString).map(x => x.name -> x.value).toMap
        val serverExtension = extList.filter(_.extensionType == ExtensionType.ServerExtension.toString).map(x => x.name -> x.value).toMap
        val combinedExtension = extList.filter(_.extensionType == ExtensionType.CombinedExtension.toString).map(x => x.name -> x.value).toMap
        val labExtension = extList.filter(_.extensionType == ExtensionType.LabExtension.toString).map(x => x.name -> x.value).toMap
        Some(UserJupyterExtensionConfig(nbExtension, serverExtension, combinedExtension, labExtension))
      }
    }
  }
}