package org.broadinstitute.dsde.workbench.leonardo

import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.auth.WhitelistAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.auth.sam.MockPetClusterServiceAccountProvider
import org.broadinstitute.dsde.workbench.leonardo.config.{ClusterDefaultsConfig, ClusterFilesConfig, ClusterResourcesConfig, DataprocConfig, ProxyConfig, SwaggerConfig}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.model.google.{GoogleProject, ServiceAccountKey, ServiceAccountKeyId, ServiceAccountPrivateKeyData, _}
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail, WorkbenchUserId}
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.ExecutionContext

// values common to multiple tests, to reduce boilerplate

trait CommonTestData { this: ScalaFutures =>
  val name1 = ClusterName("name1")
  val name2 = ClusterName("name2")
  val name3 = ClusterName("name3")
  val project = GoogleProject("dsp-leo-test")
  val userEmail = WorkbenchEmail("user1@example.com")
  val userInfo = UserInfo(OAuth2BearerToken("accessToken"), WorkbenchUserId("user1"), userEmail, 0)
  val serviceAccountEmail = WorkbenchEmail("pet-1234567890@test-project.iam.gserviceaccount.com")
  val unauthorizedEmail = WorkbenchEmail("somecreep@example.com")
  val jupyterExtensionUri = GcsPath(GcsBucketName("extension_bucket"), GcsObjectName("extension_path"))
  val jupyterUserScriptUri = GcsPath(GcsBucketName("userscript_bucket"), GcsObjectName("userscript.sh"))
  val serviceAccountKey = ServiceAccountKey(ServiceAccountKeyId("123"), ServiceAccountPrivateKeyData("abcdefg"), Some(Instant.now), Some(Instant.now.plusSeconds(300)))
  val initBucketPath = GcsBucketName("bucket-path")

  val config = ConfigFactory.parseResources("reference.conf").withFallback(ConfigFactory.load())
  val whitelistAuthConfig = config.getConfig("auth.whitelistProviderConfig")
  val whitelist = config.as[Set[String]]("auth.whitelistProviderConfig.whitelist").map(_.toLowerCase)
  val dataprocConfig = config.as[DataprocConfig]("dataproc")
  val clusterFilesConfig = config.as[ClusterFilesConfig]("clusterFiles")
  val clusterResourcesConfig = config.as[ClusterResourcesConfig]("clusterResources")
  val clusterDefaultsConfig = config.as[ClusterDefaultsConfig]("clusterDefaults")
  val proxyConfig = config.as[ProxyConfig]("proxy")
  val swaggerConfig = config.as[SwaggerConfig]("swagger")
  val clusterUrlBase = dataprocConfig.clusterUrlBase
  val serviceAccountsConfig = config.getConfig("serviceAccounts.config")

  val singleNodeDefaultMachineConfig = MachineConfig(Some(clusterDefaultsConfig.numberOfWorkers), Some(clusterDefaultsConfig.masterMachineType), Some(clusterDefaultsConfig.masterDiskSize))
  val testClusterRequest = ClusterRequest(Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"), None)
  val testClusterRequestWithExtensionAndScript = ClusterRequest(Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"), Some(jupyterExtensionUri), Some(jupyterUserScriptUri))


  val serviceAccountInfo = new ServiceAccountInfo(Option(WorkbenchEmail("testServiceAccount1@example.com")), Option(WorkbenchEmail("testServiceAccount2@example.com")))
  val testCluster = new Cluster(name1, new UUID(1, 1), project, serviceAccountInfo, MachineConfig(), Cluster.getClusterUrl(project, name1, clusterUrlBase), OperationName("op"), ClusterStatus.Running, None, userEmail, Instant.now(), None, Map(), Option(GcsPath(GcsBucketName("bucketName"), GcsObjectName("extension"))),Option(GcsPath(GcsBucketName("bucketName"), GcsObjectName("userScript"))), Some(GcsBucketName("testStagingBucket1")), Set.empty)


  // TODO look into parameterized tests so both provider impls can both be tested
  // Also remove code duplication with LeonardoServiceSpec, TestLeoRoutes, and CommonTestData
  val serviceAccountProvider = new MockPetClusterServiceAccountProvider(serviceAccountsConfig)
  val whitelistAuthProvider = new WhitelistAuthProvider(whitelistAuthConfig, serviceAccountProvider)


  protected def clusterServiceAccount(googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Option[WorkbenchEmail] = {
    serviceAccountProvider.getClusterServiceAccount(userInfo.userEmail, googleProject).futureValue
  }

  protected def notebookServiceAccount(googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Option[WorkbenchEmail] = {
    serviceAccountProvider.getNotebookServiceAccount(userInfo.userEmail, googleProject).futureValue
  }
}

trait GcsPathUtils {
  def gcsPath(str: String): GcsPath = {
    parseGcsPath(str).right.get
  }
}
