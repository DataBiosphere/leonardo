package org.broadinstitute.dsde.workbench.leonardo.service

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri.Host
import akka.stream.ActorMaterializer
import org.broadinstitute.dsde.workbench.leonardo.config.ProxyConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache.{ClusterReady, GetClusterResponse}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by rtitle on 8/25/17.
  */
class MockProxyService(proxyConfig: ProxyConfig, gdDAO: GoogleDataprocDAO, dbRef: DbReference, authProvider: LeoAuthProvider)
                      (implicit system: ActorSystem, materializer: ActorMaterializer, executionContext: ExecutionContext)
  extends ProxyService(proxyConfig: ProxyConfig, gdDAO: GoogleDataprocDAO,  dbRef: DbReference, system.deadLetters, authProvider, system.deadLetters) {

  override def getTargetHost(googleProject: GoogleProject, clusterName: ClusterName): Future[GetClusterResponse] =
    Future.successful(ClusterReady(Host("localhost")))

}
