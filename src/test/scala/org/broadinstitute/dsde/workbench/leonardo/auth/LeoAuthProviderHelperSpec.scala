package org.broadinstitute.dsde.workbench.leonardo.auth

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData
import org.broadinstitute.dsde.workbench.leonardo.model.{NotebookClusterActions, ProjectActions}
import org.broadinstitute.dsde.workbench.leonardo.service.ClusterNotFoundException
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by rtitle on 1/25/18.
  */
class LeoAuthProviderHelperSpec extends TestKit(ActorSystem("leonardotest")) with FlatSpecLike with Matchers with CommonTestData with ScalaFutures with BeforeAndAfterAll {
  import system.dispatcher

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  "LeoAuthProviderHelper" should "delegate provider calls" in {
    val mockProvider = new MockLeoAuthProvider(config.getConfig("auth.alwaysYesProviderConfig"), serviceAccountProvider)
    val helper = LeoAuthProviderHelper(mockProvider, config.getConfig("auth.samAuthProviderConfig"), serviceAccountProvider)

    helper.hasProjectPermission(userEmail, ProjectActions.CreateClusters, project).futureValue shouldBe true
    helper.hasNotebookClusterPermission(userEmail, NotebookClusterActions.ConnectToCluster, project, name1).futureValue shouldBe true
  }

  it should "pass through LeoExceptions" in {
    val mockProvider = new MockLeoAuthProvider(config.getConfig("auth.alwaysYesProviderConfig"), serviceAccountProvider) {
      override def hasProjectPermission(userEmail: WorkbenchEmail, action: ProjectActions.ProjectAction, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Boolean] = {
        Future.failed(ClusterNotFoundException(googleProject, name1))
      }
    }

    val helper = LeoAuthProviderHelper(mockProvider, config.getConfig("auth.samAuthProviderConfig"), serviceAccountProvider)
    helper.hasProjectPermission(userEmail, ProjectActions.CreateClusters, project).failed.futureValue shouldBe a [ClusterNotFoundException]
  }

  it should "map non-LeoExceptions to LeoExceptions" in {
    val mockProvider = new MockLeoAuthProvider(config.getConfig("auth.alwaysYesProviderConfig"), serviceAccountProvider, false)
    val helper = LeoAuthProviderHelper(mockProvider, config.getConfig("auth.samAuthProviderConfig"), serviceAccountProvider)

    helper.notifyClusterCreated(userEmail, project, name1).failed.futureValue shouldBe a [AuthProviderException]
  }

  it should "handle thrown exceptions" in {
    val mockProvider = new MockLeoAuthProvider(config.getConfig("auth.alwaysYesProviderConfig"), serviceAccountProvider) {
      override def hasProjectPermission(userEmail: WorkbenchEmail, action: ProjectActions.ProjectAction, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Boolean] = {
        throw new RuntimeException
      }
    }

    val helper = LeoAuthProviderHelper(mockProvider, config.getConfig("auth.samAuthProviderConfig"), serviceAccountProvider)
    helper.hasProjectPermission(userEmail, ProjectActions.CreateClusters, project).failed.futureValue shouldBe a [AuthProviderException]
  }

}
