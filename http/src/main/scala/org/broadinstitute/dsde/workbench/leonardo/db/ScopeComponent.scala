package org.broadinstitute.dsde.workbench.leonardo.db

case class ScopeRecord(clusterId: Long, scope: String)

trait ScopeComponent extends LeoComponent {
  this: ClusterComponent =>

  import profile.api._

  class ScopeTable(tag: Tag) extends Table[ScopeRecord](tag, "CLUSTER_SCOPES") {
    def clusterId = column[Long]("clusterId")

    def scope = column[String]("scope", O.Length(254))

    def cluster = foreignKey("FK_CLUSTER_ID", clusterId, clusterQuery)(_.id)
    def uniqueKey = index("IDX_SCOPE_UNIQUE", (clusterId, scope), unique = true)

    def * = (clusterId, scope) <> (ScopeRecord.tupled, ScopeRecord.unapply)
  }

  object scopeQuery extends TableQuery(new ScopeTable(_)) {

    def save(clusterId: Long, scope: String): DBIO[Int] = {
      scopeQuery += ScopeRecord(clusterId, scope)
    }


    def saveAllForCluster(clusterId: Long, scopes: Set[String]) = {
      scopeQuery ++= scopes.map(scope => marshallScope(clusterId, scope))
    }

    def getAllForCluster(clusterId: Long): DBIO[Set[String]] = {
      scopeQuery.filter {
        _.clusterId === clusterId
      }.result map { recs =>
        val clusterScopes = recs.map { rec =>
          rec.scope
        }
        clusterScopes.toSet
      }
    }

    def marshallScope(clusterId: Long, scope: String): ScopeRecord = {
      ScopeRecord(clusterId, scope)
    }

    def unmarshallScopes(scopeList: List[ScopeRecord]): Set[String] = {
      scopeList.map(rec => rec.scope).toSet
    }

    private def clusterScopeFilter(clusterId: Long, scope: String): Query[ScopeTable, ScopeRecord, Seq] = {
      scopeQuery.filter {
        _.clusterId === clusterId
      }.filter {
        _.scope === scope
      }
    }

    def delete(clusterId: Long, scope: String): DBIO[Int] = {
      clusterScopeFilter(clusterId, scope).delete
    }

    def deleteAllForCluster(clusterId: Long): DBIO[Int] = {
      scopeQuery.filter {
        _.clusterId === clusterId
      }.delete
    }
  }
}
