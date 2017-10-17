package org.broadinstitute.dsde.workbench.leonardo.api

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.config.{ClusterResourcesConfig, DataprocConfig, ProxyConfig, SwaggerConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.MockGoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.DbSingleton
import org.broadinstitute.dsde.workbench.leonardo.model.UserInfo
import org.broadinstitute.dsde.workbench.leonardo.monitor.NoopActor
import org.broadinstitute.dsde.workbench.leonardo.service.{LeonardoService, MockProxyService}
import org.broadinstitute.dsde.workbench.model.{WorkbenchUserEmail, WorkbenchUserId}

/**
  * Created by rtitle on 8/15/17.
  */
trait TestLeoRoutes { this: ScalatestRouteTest =>
  val config = ConfigFactory.parseResources("reference.conf").withFallback(ConfigFactory.load())
  val dataprocConfig = config.as[DataprocConfig]("dataproc")
  val proxyConfig = config.as[ProxyConfig]("proxy")
  val clusterResourcesConfig = config.as[ClusterResourcesConfig]("clusterResources")
  val whitelistConfig = config.as[(Set[String])]("whitelist").map(WorkbenchUserEmail(_))
  val mockGoogleDataprocDAO = new MockGoogleDataprocDAO(dataprocConfig, proxyConfig)
  val mockSamDAO = new MockSamDAO

  // Route tests don't currently do cluster monitoring, so use NoopActor
  val clusterMonitorSupervisor = system.actorOf(NoopActor.props)
  val leonardoService = new LeonardoService(dataprocConfig, clusterResourcesConfig, proxyConfig, mockGoogleDataprocDAO, DbSingleton.ref, clusterMonitorSupervisor, mockSamDAO)
  val proxyService = new MockProxyService(proxyConfig, mockGoogleDataprocDAO, DbSingleton.ref)
  val swaggerConfig = SwaggerConfig("", "")
  val defaultUserInfo = UserInfo(OAuth2BearerToken("accessToken"), WorkbenchUserId("user1"), WorkbenchUserEmail("user1@example.com"), 0)
  val leoRoutes = new LeoRoutes(leonardoService, proxyService, swaggerConfig, whitelistConfig) with MockUserInfoDirectives {
    override val userInfo: UserInfo = defaultUserInfo
  }
}
