package org.broadinstitute.dsde.workbench.leonardo.auth

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData
import org.broadinstitute.dsde.workbench.leonardo.auth.sam.MockPetClusterServiceAccountProvider
import org.broadinstitute.dsde.workbench.leonardo.service.ClusterNotFoundException
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

class ServiceAccountProviderHelperSpec extends TestKit(ActorSystem("leonardotest")) with FlatSpecLike with Matchers with CommonTestData with ScalaFutures with BeforeAndAfterAll {
  import system.dispatcher

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  "ServiceAccountProviderHelper" should "delegate provider calls" in {
    val helper = ServiceAccountProviderHelper(serviceAccountProvider, config.getConfig("serviceAccounts.config"))

    helper.getLeoServiceAccountAndKey shouldBe serviceAccountProvider.getLeoServiceAccountAndKey
    helper.getClusterServiceAccount(userInfo.userEmail, project).futureValue shouldBe serviceAccountProvider.getClusterServiceAccount(userInfo.userEmail, project).futureValue
    helper.getNotebookServiceAccount(userInfo.userEmail, project).futureValue shouldBe serviceAccountProvider.getNotebookServiceAccount(userInfo.userEmail, project).futureValue
    helper.listGroupsStagingBucketReaders(userEmail).futureValue shouldBe serviceAccountProvider.listGroupsStagingBucketReaders(userEmail).futureValue
  }

  // The next 3 tests verify that an exception thrown in ServiceAccountProvider gets translated to LeoException in ServiceAccountProviderHelper

  it should "pass through LeoExceptions" in {
    val mockProvider = new MockPetClusterServiceAccountProvider(config.getConfig("serviceAccounts.config")) {
      override def getNotebookServiceAccount(userEmail: WorkbenchEmail, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Option[WorkbenchEmail]] = {
        Future.failed(ClusterNotFoundException(googleProject, name1))
      }
    }

    val helper = ServiceAccountProviderHelper(mockProvider, config.getConfig("serviceAccounts.config"))
    helper.getNotebookServiceAccount(userInfo.userEmail, project).failed.futureValue shouldBe a [ClusterNotFoundException]
  }

  it should "map non-LeoExceptions to LeoExceptions" in {
    val mockProvider = new MockPetClusterServiceAccountProvider(config.getConfig("serviceAccounts.config")) {
      override def getNotebookServiceAccount(userEmail: WorkbenchEmail, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Option[WorkbenchEmail]] = {
        Future.failed(new RuntimeException)
      }
    }

    val helper = ServiceAccountProviderHelper(mockProvider, config.getConfig("serviceAccounts.config"))
    helper.getNotebookServiceAccount(userInfo.userEmail, project).failed.futureValue shouldBe a [ServiceAccountProviderException]
  }

  it should "handle thrown exceptions" in {
    val mockProvider = new MockPetClusterServiceAccountProvider(config.getConfig("serviceAccounts.config")) {
      override def getNotebookServiceAccount(userEmail: WorkbenchEmail, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Option[WorkbenchEmail]] = {
        throw new RuntimeException
      }
    }

    val helper = ServiceAccountProviderHelper(mockProvider, config.getConfig("serviceAccounts.config"))
    helper.getNotebookServiceAccount(userInfo.userEmail, project).failed.futureValue shouldBe a [ServiceAccountProviderException]
  }

  it should "timeout long provider responses" in {
    implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(10, Seconds)))
    val mockProvider = new MockPetClusterServiceAccountProvider(config.getConfig("serviceAccounts.config")) {
      override def getNotebookServiceAccount(workbenchEmail: WorkbenchEmail, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Option[WorkbenchEmail]] = {
        Future {
          Thread.sleep((1 minute).toMillis)
          Some(userEmail)
        }
      }
    }

    val helper = ServiceAccountProviderHelper(mockProvider, config.getConfig("serviceAccounts.config"))
    // should timeout after 1 second
    val response = helper.getNotebookServiceAccount(userInfo.userEmail, project).failed.futureValue
    response shouldBe a [ServiceAccountProviderException]
    response.asInstanceOf[ServiceAccountProviderException].isTimeout shouldBe true
  }

}
