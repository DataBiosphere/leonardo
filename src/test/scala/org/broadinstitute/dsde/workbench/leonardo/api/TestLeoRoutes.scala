package org.broadinstitute.dsde.workbench.leonardo.api

import akka.actor.{Actor, Props}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.config.{DataprocConfig, ProxyConfig, SwaggerConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.MockGoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.DbSingleton
import org.broadinstitute.dsde.workbench.leonardo.monitor.NoopActor
import org.broadinstitute.dsde.workbench.leonardo.service.{LeonardoService, MockProxyService}

import scala.concurrent.duration._

/**
  * Created by rtitle on 8/15/17.
  */
trait TestLeoRoutes { this: ScalatestRouteTest =>
  val config = ConfigFactory.parseResources("reference.conf").withFallback(ConfigFactory.load())
  val dataprocConfig = config.as[DataprocConfig]("dataproc")
  val mockGoogleDataprocDAO = new MockGoogleDataprocDAO(dataprocConfig)
  val proxyConfig = ProxyConfig(jupyterPort = 8001, jupyterProtocol = "tcp", jupyterDomain = "", dnsPollPeriod = 1 day)
  // Route tests don't currently do cluster monitoring, so use NoopActor
  val clusterMonitorSupervisor = system.actorOf(NoopActor.props)
  val leonardoService = new LeonardoService(dataprocConfig, mockGoogleDataprocDAO, DbSingleton.ref, clusterMonitorSupervisor)
  val proxyService = new MockProxyService(proxyConfig, DbSingleton.ref)
  val swaggerConfig = SwaggerConfig("", "")
  val leoRoutes = new LeoRoutes(leonardoService, proxyService, swaggerConfig)
}
