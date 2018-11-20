package org.broadinstitute.dsde.workbench.leonardo.db

case class ScopeRecord(clusterId: Long, scope: String)
trait ScopeComponent extends LeoComponent {
  this: ClusterComponent =>

  import profile.api._

  class ScopeTable(tag: Tag) extends Table[ScopeRecord](tag, "CLUSTER_SCOPES") {
    def clusterId = column[Long]("clusterId")

    def scope = column[String]("scope", O.Length(254))

    def cluster = foreignKey("FK_CLUSTER_ID", clusterId, clusterQuery)(_.id)

    def uniqueKey = index("IDX_LABEL_UNIQUE", (clusterId, scope), unique = true)

    def * = (clusterId, scope) <> (ScopeRecord.tupled, ScopeRecord.unapply)
  }

  object scopeQuery extends TableQuery(new ScopeTable(_)) {

    def save(clusterId: Long, scope: String): DBIO[Int] = {
      scopeQuery += ScopeRecord(clusterId, scope)
    }


    def saveAllForCluster(clusterId: Long, scopes: List[String]) = {
      scopeQuery ++= scopes.map(scope => marshallScope(clusterId, scope))
    }

    def getAllForCluster(clusterId: Long): DBIO[List[String]] = {
      scopeQuery.filter {
        _.clusterId === clusterId
      }.result map { recs =>
        val clusterScopes = recs.map { rec =>
          rec.scope
          }
        clusterScopes.toList
        }
      }
    }

    def marshallScope(clusterId:Long, scope: String): ScopeRecord = {
      ScopeRecord(clusterId, scope)
    }

    def unmarshallScopes(scopeList: List[ScopeRecord]): Option[List[String]] = {
      if (scopeList.isEmpty) {
        None
      } else {
        Some(scopeList.map(rec => rec.scope))
      }
    }
}
