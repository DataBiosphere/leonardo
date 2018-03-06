package org.broadinstitute.dsde.workbench.leonardo.api

import java.io.ByteArrayInputStream

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.google.mock.{MockGoogleDataprocDAO, MockGoogleIamDAO, MockGoogleStorageDAO}
import org.broadinstitute.dsde.workbench.leonardo.auth.WhitelistAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.auth.sam.MockPetClusterServiceAccountProvider
import org.broadinstitute.dsde.workbench.leonardo.config.{ClusterDefaultsConfig, ClusterFilesConfig, ClusterResourcesConfig, DataprocConfig, ProxyConfig, SwaggerConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.MockSamDAO
import org.broadinstitute.dsde.workbench.leonardo.db.DbSingleton
import org.broadinstitute.dsde.workbench.leonardo.monitor.NoopActor
import org.broadinstitute.dsde.workbench.leonardo.service.{BucketHelper, LeonardoService, MockProxyService, StatusService}
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail, WorkbenchUserId}
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._

/**
  * Created by rtitle on 8/15/17.
  */
trait TestLeoRoutes { this: ScalatestRouteTest with ScalaFutures =>
  val config = ConfigFactory.parseResources("reference.conf").withFallback(ConfigFactory.load())
  val whitelist = config.as[Set[String]]("auth.whitelistProviderConfig.whitelist").map(_.toLowerCase)
  val dataprocConfig = config.as[DataprocConfig]("dataproc")
  val proxyConfig = config.as[ProxyConfig]("proxy")
  val clusterFilesConfig = config.as[ClusterFilesConfig]("clusterFiles")
  val clusterResourcesConfig = config.as[ClusterResourcesConfig]("clusterResources")
  val swaggerConfig = config.as[SwaggerConfig]("swagger")
  val mockGoogleIamDAO = new MockGoogleIamDAO
  val mockGoogleStorageDAO = new MockGoogleStorageDAO
  val mockPetGoogleDAO:(WorkbenchEmail, GoogleProject) => MockGoogleStorageDAO = (email, project) => {
    val petMock = new MockGoogleStorageDAO
    val extensionPath = GcsPath(GcsBucketName("bucket"), GcsObjectName("my_extension.tar.gz"))
    petMock.buckets += extensionPath.bucketName -> Set((extensionPath.objectName, new ByteArrayInputStream("foo".getBytes())))
    petMock
  }
  val mockSamDAO = new MockSamDAO
  val clusterDefaultsConfig = config.as[ClusterDefaultsConfig]("clusterDefaults")
  val mockGoogleDataprocDAO = new MockGoogleDataprocDAO
  val extensionPath = GcsPath(GcsBucketName("bucket"), GcsObjectName("my_extension.tar.gz"))
  mockGoogleStorageDAO.buckets += extensionPath.bucketName -> Set((extensionPath.objectName, new ByteArrayInputStream("foo".getBytes())))

  // TODO look into parameterized tests so both provider impls can both be tested
  //val serviceAccountProvider = new MockPetServiceAccountProvider(config.getConfig("serviceAccounts.config"))
  val serviceAccountProvider = new MockPetClusterServiceAccountProvider(config.getConfig("serviceAccounts.config"))

  val whitelistAuthProvider = new WhitelistAuthProvider(config.getConfig("auth.whitelistProviderConfig"), serviceAccountProvider)

  // Route tests don't currently do cluster monitoring, so use NoopActor
  val clusterMonitorSupervisor = system.actorOf(NoopActor.props)
  val bucketHelper = new BucketHelper(dataprocConfig, mockGoogleDataprocDAO, mockGoogleStorageDAO, serviceAccountProvider)
  val leonardoService = new LeonardoService(dataprocConfig, clusterFilesConfig, clusterResourcesConfig, clusterDefaultsConfig, proxyConfig, swaggerConfig, mockGoogleDataprocDAO, mockGoogleIamDAO, mockGoogleStorageDAO,mockPetGoogleDAO, DbSingleton.ref, clusterMonitorSupervisor, whitelistAuthProvider, serviceAccountProvider, whitelist, bucketHelper)
  val proxyService = new MockProxyService(proxyConfig, mockGoogleDataprocDAO, DbSingleton.ref, whitelistAuthProvider)
  val statusService = new StatusService(mockGoogleDataprocDAO, mockSamDAO, DbSingleton.ref, dataprocConfig, pollInterval = 1.second)
  val defaultUserInfo = UserInfo(OAuth2BearerToken("accessToken"), WorkbenchUserId("user1"), WorkbenchEmail("user1@example.com"), 0)
  val leoRoutes = new LeoRoutes(leonardoService, proxyService, statusService, swaggerConfig) with MockUserInfoDirectives {
    override val userInfo: UserInfo = defaultUserInfo
  }

  def clusterServiceAccount(googleProject: GoogleProject): Option[WorkbenchEmail] = {
    serviceAccountProvider.getClusterServiceAccount(defaultUserInfo.userEmail, googleProject).futureValue
  }

  def notebookServiceAccount(googleProject: GoogleProject): Option[WorkbenchEmail] = {
    serviceAccountProvider.getNotebookServiceAccount(defaultUserInfo.userEmail, googleProject).futureValue
  }
}
