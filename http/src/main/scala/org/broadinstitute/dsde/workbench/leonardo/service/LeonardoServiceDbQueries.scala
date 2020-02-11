package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
import org.broadinstitute.dsde.workbench.leonardo.db.RuntimeConfigQueries._
import org.broadinstitute.dsde.workbench.leonardo.db.clusterQuery.{fullClusterQueryByUniqueKey, unmarshalFullCluster}
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.ExecutionContext

object LeonardoServiceDbQueries {
  def getGetClusterResponse(googleProject: GoogleProject,
                            clusterName: ClusterName)(implicit executionContext: ExecutionContext): DBIO[GetClusterResponse] = {
    val activeCluster = fullClusterQueryByUniqueKey(googleProject, clusterName, None).joinLeft(runtimeConfigs)
    activeCluster.result.flatMap {
      recs =>
        val clusterRecs = recs.map(_._1)
        val res = for {
          cluster <- unmarshalFullCluster(clusterRecs).headOption
          runtimeConfig <- recs.headOption.flatMap(_._2)
        } yield GetClusterResponse.fromCluster(cluster, runtimeConfig.runtimeConfig)
        res.fold[DBIO[GetClusterResponse]](DBIO.failed(ClusterNotFoundException(googleProject, clusterName)))(r => DBIO.successful(r))
    }
  }
}
