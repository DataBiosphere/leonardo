package org.broadinstitute.dsde.workbench.leonardo.auth

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData
import org.broadinstitute.dsde.workbench.leonardo.model.ProjectActions
import org.broadinstitute.dsde.workbench.leonardo.service.ClusterNotFoundException
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.{ExecutionContext, Future}

class ServiceAccountProviderHelperSpec extends TestKit(ActorSystem("leonardotest")) with FlatSpecLike with Matchers with CommonTestData with ScalaFutures with BeforeAndAfterAll {
  import system.dispatcher

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  "ServiceAccountProviderHelper" should "delegate provider calls" in {
    val helper = ServiceAccountProviderHelper(serviceAccountProvider, config.getConfig("serviceAccounts.config"))

    helper.getLeoServiceAccountAndKey shouldBe serviceAccountProvider.getLeoServiceAccountAndKey
    helper.getClusterServiceAccount(userInfo, project).futureValue shouldBe serviceAccountProvider.getClusterServiceAccount(userInfo, project).futureValue
    helper.getNotebookServiceAccount(userInfo, project).futureValue shouldBe serviceAccountProvider.getNotebookServiceAccount(userInfo, project).futureValue
    helper.listGroupsStagingBucketReaders(userEmail).futureValue shouldBe serviceAccountProvider.listGroupsStagingBucketReaders(userEmail).futureValue
  }

  // The next 3 tests verify that an exception thrown in ServiceAccountProvider gets translated to LeoException in ServiceAccountProviderHelper

  it should "pass through LeoExceptions" in {
    val mockProvider = new MockPetsPerProjectServiceAccountProvider(config.getConfig("serviceAccounts.config")) {
      override def getNotebookServiceAccount(userInfo: UserInfo, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Option[WorkbenchEmail]] = {
        Future.failed(ClusterNotFoundException(googleProject, name1))
      }
    }

    val helper = ServiceAccountProviderHelper(mockProvider, config.getConfig("serviceAccounts.config"))
    helper.getNotebookServiceAccount(userInfo, project).failed.futureValue shouldBe a [ClusterNotFoundException]
  }

  it should "map non-LeoExceptions to LeoExceptions" in {
    val mockProvider = new MockPetsPerProjectServiceAccountProvider(config.getConfig("serviceAccounts.config")) {
      override def getNotebookServiceAccount(userInfo: UserInfo, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Option[WorkbenchEmail]] = {
        Future.failed(new RuntimeException)
      }
    }

    val helper = ServiceAccountProviderHelper(mockProvider, config.getConfig("serviceAccounts.config"))
    helper.getNotebookServiceAccount(userInfo, project).failed.futureValue shouldBe a [ServiceAccountProviderException]
  }

  it should "handle thrown exceptions" in {
    val mockProvider = new MockPetsPerProjectServiceAccountProvider(config.getConfig("serviceAccounts.config")) {
      override def getNotebookServiceAccount(userInfo: UserInfo, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Option[WorkbenchEmail]] = {
        throw new RuntimeException
      }
    }

    val helper = ServiceAccountProviderHelper(mockProvider, config.getConfig("serviceAccounts.config"))
    helper.getNotebookServiceAccount(userInfo, project).failed.futureValue shouldBe a [ServiceAccountProviderException]
  }

}
