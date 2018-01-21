package org.broadinstitute.dsde.workbench.leonardo

import java.time.Instant

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.google.gcs.{GcsBucketName, GcsPath, GcsRelativePath}
import org.broadinstitute.dsde.workbench.leonardo.auth.{MockPetsPerProjectServiceAccountProvider, WhitelistAuthProvider}
import org.broadinstitute.dsde.workbench.leonardo.config.{ClusterDefaultsConfig, ClusterFilesConfig, ClusterResourcesConfig, DataprocConfig, ProxyConfig, SwaggerConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.MockSamDAO
import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterName, ClusterRequest}
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.model.{WorkbenchEmail, WorkbenchUserId}
import org.broadinstitute.dsde.workbench.model.google.{GoogleProject, ServiceAccountKey, ServiceAccountKeyId, ServiceAccountPrivateKeyData}
import org.scalatest.concurrent.ScalaFutures

// values common to multiple tests, to reduce boilerplate

trait CommonTestData { this: ScalaFutures =>
  val name1 = ClusterName("name1")
  val name2 = ClusterName("name2")
  val name3 = ClusterName("name3")
  val project = GoogleProject("dsp-leo-test")
  val userInfo = UserInfo(OAuth2BearerToken("accessToken"), WorkbenchUserId("user1"), WorkbenchEmail("user1@example.com"), 0)
  val userEmail = WorkbenchEmail("user1@example.com")
  val testClusterRequest = ClusterRequest(Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"), None)
  val serviceAccountEmail = WorkbenchEmail("pet-1234567890@test-project.iam.gserviceaccount.com")
  val jupyterExtensionUri = Some(GcsPath(GcsBucketName("extension_bucket"), GcsRelativePath("extension_path")))
  val serviceAccountKey = ServiceAccountKey(ServiceAccountKeyId("123"), ServiceAccountPrivateKeyData("abcdefg"), Some(Instant.now), Some(Instant.now.plusSeconds(300)))

  val config = ConfigFactory.parseResources("reference.conf").withFallback(ConfigFactory.load())
  val whitelist = config.as[Set[String]]("auth.whitelistProviderConfig.whitelist").map(_.toLowerCase)
  val dataprocConfig = config.as[DataprocConfig]("dataproc")
  val clusterFilesConfig = config.as[ClusterFilesConfig]("clusterFiles")
  val clusterResourcesConfig = config.as[ClusterResourcesConfig]("clusterResources")
  val clusterDefaultsConfig = config.as[ClusterDefaultsConfig]("clusterDefaults")
  val proxyConfig = config.as[ProxyConfig]("proxy")
  val swaggerConfig = config.as[SwaggerConfig]("swagger")

  val defaultUserInfo = UserInfo(OAuth2BearerToken("accessToken"), WorkbenchUserId("user1"), WorkbenchEmail("user1@example.com"), 0)

  // TODO look into parameterized tests so both provider impls can both be tested
  // Also remove code duplication with LeonardoServiceSpec, TestLeoRoutes, and CommonTestData
  val serviceAccountProvider = new MockPetsPerProjectServiceAccountProvider(config.getConfig("serviceAccounts.config"))
  val whitelistAuthProvider = new WhitelistAuthProvider(config.getConfig("auth.whitelistProviderConfig"), serviceAccountProvider)

  val samDAO = new MockSamDAO

  protected def clusterServiceAccount(googleProject: GoogleProject): Option[WorkbenchEmail] = {
    serviceAccountProvider.getClusterServiceAccount(defaultUserInfo, googleProject).futureValue
  }

  protected def notebookServiceAccount(googleProject: GoogleProject): Option[WorkbenchEmail] = {
    serviceAccountProvider.getNotebookServiceAccount(defaultUserInfo, googleProject).futureValue
  }
}

trait GcsPathUtils {
  def gcsPath(str: String): GcsPath = {
    GcsPath.parse(str).right.get
  }
}
