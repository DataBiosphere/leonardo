package org.broadinstitute.dsde.workbench.leonardo.api

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleIamDAO
import org.broadinstitute.dsde.workbench.leonardo.auth.WhitelistAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.config.{ClusterDefaultsConfig, ClusterFilesConfig, ClusterResourcesConfig, DataprocConfig, ProxyConfig, SwaggerConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.{MockGoogleDataprocDAO, MockSamDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.DbSingleton
import org.broadinstitute.dsde.workbench.leonardo.model.UserInfo
import org.broadinstitute.dsde.workbench.leonardo.monitor.NoopActor
import org.broadinstitute.dsde.workbench.leonardo.service.{LeonardoService, MockProxyService, StatusService}
import org.broadinstitute.dsde.workbench.model.{WorkbenchEmail, WorkbenchUserId}

import scala.concurrent.duration._

/**
  * Created by rtitle on 8/15/17.
  */
trait TestLeoRoutes { this: ScalatestRouteTest =>
  val config = ConfigFactory.parseResources("reference.conf").withFallback(ConfigFactory.load())
  val dataprocConfig = config.as[DataprocConfig]("dataproc")
  val proxyConfig = config.as[ProxyConfig]("proxy")
  val clusterFilesConfig = config.as[ClusterFilesConfig]("clusterFiles")
  val clusterResourcesConfig = config.as[ClusterResourcesConfig]("clusterResources")
  val swaggerConfig = config.as[SwaggerConfig]("swagger")
  val mockGoogleIamDAO = new MockGoogleIamDAO
  val mockSamDAO = new MockSamDAO
  val clusterDefaultsConfig = config.as[ClusterDefaultsConfig]("clusterDefaults")
  val mockGoogleDataprocDAO = new MockGoogleDataprocDAO(dataprocConfig, proxyConfig, clusterDefaultsConfig)
  val whitelistAuthProvider = new WhitelistAuthProvider(config.atPath("auth.providerConfig"))
  // Route tests don't currently do cluster monitoring, so use NoopActor
  val clusterMonitorSupervisor = system.actorOf(NoopActor.props)
  val leonardoService = new LeonardoService(dataprocConfig, clusterFilesConfig, clusterResourcesConfig, proxyConfig, swaggerConfig, mockGoogleDataprocDAO, mockGoogleIamDAO, DbSingleton.ref, clusterMonitorSupervisor, mockSamDAO, whitelistAuthProvider)
  val proxyService = new MockProxyService(proxyConfig, mockGoogleDataprocDAO, DbSingleton.ref, whitelistAuthProvider)
  val statusService = new StatusService(mockGoogleDataprocDAO, mockSamDAO, DbSingleton.ref, dataprocConfig, pollInterval = 1.second)
  val defaultUserInfo = UserInfo(OAuth2BearerToken("accessToken"), WorkbenchUserId("user1"), WorkbenchEmail("user1@example.com"), 0)
  val leoRoutes = new LeoRoutes(leonardoService, proxyService, statusService, swaggerConfig) with MockUserInfoDirectives {
    override val userInfo: UserInfo = defaultUserInfo
  }
}
