package org.broadinstitute.dsde.workbench.leonardo.service

import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.model.{HttpRequest, StatusCodes, Uri}
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.headers.{HttpCookiePair, OAuth2BearerToken}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.workbench.google.gcs.GcsBucketName
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleIamDAO
import org.broadinstitute.dsde.workbench.leonardo.GcsPathUtils
import org.broadinstitute.dsde.workbench.leonardo.auth.MockLeoAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.config.{ClusterDefaultsConfig, ClusterFilesConfig, ClusterResourcesConfig, DataprocConfig, ProxyConfig, SwaggerConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.{MockGoogleDataprocDAO, MockSamDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.NoopActor
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail, WorkbenchUserId}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FreeSpec, Matchers}
import net.ceedubs.ficus.Ficus._

class AuthProviderSpec extends FreeSpec with ScalatestRouteTest with Matchers with TestComponent with ScalaFutures with GcsPathUtils with TestProxy {
  val name1 = ClusterName("name1")
  val project = GoogleProject("dsp-leo-test")
  val userInfo = UserInfo(OAuth2BearerToken("accessToken"), WorkbenchUserId("user1"), WorkbenchEmail("user1@example.com"), 0)
  val userEmail = WorkbenchEmail("user1@example.com")
  val serviceAccountEmail = WorkbenchEmail("pet-1234567890@test-project.iam.gserviceaccount.com")
  val clusterName = name1.string
  val googleProject = project.value

  val config = ConfigFactory.parseResources("reference.conf").withFallback(ConfigFactory.load())
  val dataprocConfig = config.as[DataprocConfig]("dataproc")
  val proxyConfig = config.as[ProxyConfig]("proxy")
  val clusterFilesConfig = config.as[ClusterFilesConfig]("clusterFiles")
  val clusterResourcesConfig = config.as[ClusterResourcesConfig]("clusterResources")
  val swaggerConfig = config.as[SwaggerConfig]("swagger")
  val mockGoogleIamDAO = new MockGoogleIamDAO
  val mockSamDAO = new MockSamDAO
  val clusterDefaultsConfig = config.as[ClusterDefaultsConfig]("clusterDefaults")

  val routeTest = this

  private val bucketPath = GcsBucketName("bucket-path")
  private val testClusterRequest = ClusterRequest(bucketPath, Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"), None)
  private val alwaysYesProvider = new MockLeoAuthProvider(config.getConfig("auth.alwaysYesProviderConfig"))
  private val alwaysNoProvider = new MockLeoAuthProvider(config.getConfig("auth.alwaysNoProviderConfig"))

  val c1 = Cluster(
    clusterName = name1,
    googleId = UUID.randomUUID(),
    googleProject = project,
    googleServiceAccount = serviceAccountEmail,
    googleBucket = GcsBucketName("bucket1"),
    machineConfig = MachineConfig(Some(0),Some(""), Some(500)),
    clusterUrl = Cluster.getClusterUrl(project, name1),
    operationName = OperationName("op1"),
    status = ClusterStatus.Unknown,
    hostIp = Some(IP("numbers.and.dots")),
    creator = userEmail,
    createdDate = Instant.now(),
    destroyedDate = None,
    labels = Map("bam" -> "yes", "vcf" -> "no"),
    jupyterExtensionUri = None)

  val gdDAO = new MockGoogleDataprocDAO(dataprocConfig, proxyConfig, clusterDefaultsConfig)
  val iamDAO = new MockGoogleIamDAO
  val samDAO = new MockSamDAO
  val tokenCookie = HttpCookiePair("FCtoken", "me")

  def leoWithAuthProvider(authProvider: LeoAuthProvider): LeonardoService = {
    new LeonardoService(dataprocConfig, clusterFilesConfig, clusterResourcesConfig, proxyConfig, swaggerConfig, gdDAO, iamDAO, DbSingleton.ref, system.actorOf(NoopActor.props), samDAO, authProvider)
  }

  def proxyWithAuthProvider(authProvider: LeoAuthProvider): ProxyService = {
    new ProxyService(proxyConfig, gdDAO, DbSingleton.ref, system.deadLetters, authProvider)
  }

  "Leo with an AuthProvider" - {
    "should let you do things if the auth provider says yes" in isolatedDbTest {
      val leo = leoWithAuthProvider(alwaysYesProvider)
      val proxy = proxyWithAuthProvider(alwaysYesProvider)

      // create
      leo.createCluster(userInfo, project, name1, testClusterRequest).futureValue

      // get status
      leo.getActiveClusterDetails(userInfo, project, name1).futureValue

      // list
      leo.listClusters(userInfo, Map()).futureValue

      //connect
      val httpRequest = HttpRequest(GET, Uri(s"/notebooks/$googleProject/$clusterName"))
      proxy.proxy(userInfo, GoogleProject(googleProject), ClusterName(clusterName), httpRequest, tokenCookie)

      //todo: sync

      //delete
      leo.deleteCluster(userInfo, project, name1)
    }

    "should not let you do things if the auth provider says no" in isolatedDbTest {
      val leo = leoWithAuthProvider(alwaysNoProvider)

      //can't make a cluster
      val clusterCreateException = leo.createCluster(userInfo, project, name1, testClusterRequest).failed.futureValue
      clusterCreateException shouldBe a [AuthorizationError]
      clusterCreateException.asInstanceOf[AuthorizationError].statusCode shouldBe StatusCodes.Unauthorized

      //can't get details on an existing cluster
      //poke a cluster into the database so we actually have something to look for
      dbFutureValue { _.clusterQuery.save(c1, gcsPath("gs://bucket1"), None) }

      val clusterGetResponseException = leo.getActiveClusterDetails(userInfo, c1.googleProject, c1.clusterName).failed.futureValue
      clusterGetResponseException shouldBe a [ClusterNotFoundException]
      clusterGetResponseException.asInstanceOf[ClusterNotFoundException].statusCode shouldBe StatusCodes.NotFound

      // TODO:
      //list all clusters
      //connect to cluster
      //destroy cluster
    }

    "should give you a 401 if you can see a cluster's details but can't do the more specific action" in isolatedDbTest {
      val readOnlyProvider = new MockLeoAuthProvider(config.getConfig("auth.readOnlyProviderConfig"))
      //test connect and destroy
      val leo = leoWithAuthProvider(readOnlyProvider)
    }
  }
}
