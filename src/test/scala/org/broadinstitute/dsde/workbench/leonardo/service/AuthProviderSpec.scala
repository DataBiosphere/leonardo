package org.broadinstitute.dsde.workbench.leonardo.service

import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.testkit.TestKit
import org.broadinstitute.dsde.workbench.google.gcs.GcsBucketName
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleIamDAO
import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, GcsPathUtils}
import org.broadinstitute.dsde.workbench.leonardo.auth.MockLeoAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.dao.{MockGoogleDataprocDAO, MockSamDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.NoopActor
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FreeSpecLike, Matchers}

class AuthProviderSpec extends TestKit(ActorSystem("leonardotest")) with FreeSpecLike with Matchers with TestComponent with ScalaFutures with CommonTestData with GcsPathUtils {

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

  def leoWithAuthProvider(authProvider: LeoAuthProvider): LeonardoService = {
    val gdDAO = new MockGoogleDataprocDAO(dataprocConfig, proxyConfig, clusterDefaultsConfig)
    val iamDAO = new MockGoogleIamDAO
    val samDAO = new MockSamDAO
    val leo = new LeonardoService(dataprocConfig, clusterFilesConfig, clusterResourcesConfig, proxyConfig, swaggerConfig, gdDAO, iamDAO, DbSingleton.ref, system.actorOf(NoopActor.props), samDAO, authProvider)
    leo
  }

  "Leo with an AuthProvider" - {
    "should let you do things if the auth provider says yes" in isolatedDbTest {
      val leo = leoWithAuthProvider(alwaysYesProvider)

      // create the cluster
      val clusterCreateResponse = leo.createCluster(userInfo, project, name1, testClusterRequest).futureValue

      // get the cluster detail
      val clusterGetResponse = leo.getActiveClusterDetails(userInfo, project, name1).futureValue

      // TODO:
      //list all clusters
      //connect to cluster
      //destroy cluster
    }

    "should not let you do things if the auth provider says no" in isolatedDbTest {
      val leo = leoWithAuthProvider(alwaysNoProvider)

      //can't make a cluster
      val clusterCreateException = leo.createCluster(userInfo, project, name1, testClusterRequest).failed.futureValue
      clusterCreateException shouldBe a [AuthorizationError]
      clusterCreateException.asInstanceOf[AuthorizationError].statusCode shouldBe StatusCodes.Forbidden

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
      val readOnlyProvider = new MockLeoAuthProvider(config.getConfig("auth.readOnlyProvider"))
      //test connect and destroy
      val leo = leoWithAuthProvider(readOnlyProvider)
    }
  }
}
