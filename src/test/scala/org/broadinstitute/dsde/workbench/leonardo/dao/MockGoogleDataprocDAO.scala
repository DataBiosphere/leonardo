package org.broadinstitute.dsde.workbench.leonardo.dao


import com.google.api.services.dataproc.model.Operation
import scala.collection.JavaConverters._
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterRequest

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.Future

class MockGoogleDataprocDAO extends DataprocDAO {
  import scala.concurrent.ExecutionContext.Implicits.global

  private val clusters: mutable.Map[String, ClusterRequest] = new TrieMap()

  override def createCluster(googleProject: String, clusterName: String, clusterRequest: ClusterRequest): Future[Operation] = {
    clusters += clusterName -> clusterRequest
    Future {
      new Operation().setName(s"${googleProject}.${clusterName}")
        .setMetadata(Map[String, AnyRef](("", "")).asJava)
        .setDone(true)
        .setResponse(Map[String, AnyRef](("", "")).asJava)
    }
  }

}
