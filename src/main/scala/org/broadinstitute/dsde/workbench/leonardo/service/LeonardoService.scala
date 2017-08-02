package org.broadinstitute.dsde.workbench.leonardo.service

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.google.api.services.dataproc.model._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.dao.{DataprocDAO, GoogleDataprocDAO}
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterRequest

import scala.concurrent.{ExecutionContext, Future}

class LeonardoService(gdDAO: DataprocDAO)(implicit val system: ActorSystem, val materializer: Materializer, val executionContext: ExecutionContext) extends LazyLogging{

  def createCluster(googleProject: String, clusterName: String, clusterRequest: ClusterRequest): Future[Operation] = {
    gdDAO.createCluster(googleProject, clusterName, clusterRequest)
  }

}
