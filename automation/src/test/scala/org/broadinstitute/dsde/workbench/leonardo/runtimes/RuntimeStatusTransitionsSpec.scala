package org.broadinstitute.dsde.workbench.leonardo.runtimes

import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.leonardo.TestUser.{getAuthTokenAndAuthorization, Ron}
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.leonardo.notebooks.JupyterServerClient
import org.broadinstitute.dsde.workbench.service.RestException
import org.scalatest.tagobjects.Retryable
import org.scalatest.{DoNotDiscover, ParallelTestExecution}

import scala.util.Try

/**
 * This spec is for validating how Leonardo/Google handles cluster status transitions.
 *
 * Note these tests can take a long time so we don't test all edge cases, but these cases
 * should exercise the most commonly used paths through the system.
 */
@DoNotDiscover
class RuntimeStatusTransitionsSpec
    extends BillingProjectFixtureSpec
    with ParallelTestExecution
    with LeonardoTestUtils
    with NewBillingProjectAndWorkspaceBeforeAndAfterAll {

  // these tests just hit the Leo APIs; they don't interact with notebooks via selenium
  "RuntimeStatusTransitionsSpec" - {

    implicit val (ronAuthToken, ronAuthorization) = getAuthTokenAndAuthorization(Ron)
    implicit val rat = ronAuthToken.unsafeRunSync()
    implicit val ra = ronAuthorization.unsafeRunSync()

    "create, monitor, delete should transition correctly" taggedAs Retryable in { billingProject =>
      logger.info("Starting RuntimeStatusTransitionsSpec: create, monitor, delete should transition correctly")

      val runtimeName = randomClusterName
      val runtimeRequest = LeonardoApiClient.defaultCreateRuntime2Request

      // create a runtime, but don't wait
      createNewRuntime(billingProject, runtimeName, runtimeRequest, monitor = false)
      // runtime status should be Creating
      val creatingRuntime = Leonardo.cluster.getRuntime(billingProject, runtimeName)
      creatingRuntime.status shouldBe ClusterStatus.Creating

      // can't create another runtime with the same name
      val caught = the[RestError] thrownBy createNewRuntime(billingProject, runtimeName, monitor = false)
      caught.statusCode shouldBe (org.http4s.Status.Conflict)

      // can't stop a Creating runtime
      val caught2 = the[RestException] thrownBy stopRuntime(billingProject, runtimeName, monitor = false)
      caught2.message should include(""""statusCode":409""")

      // wait for runtime to be running
      monitorCreateRuntime(billingProject, runtimeName, runtimeRequest)
      Leonardo.cluster.getRuntime(billingProject, runtimeName).status shouldBe ClusterStatus.Running

      // verify we can hit the proxy when the runtime is running
      val getResult = Try(JupyterServerClient.getApi(billingProject, runtimeName))
      getResult.isSuccess shouldBe true
      getResult.get should not include "ProxyException"

      // delete the runtime, but don't wait
      deleteRuntime(billingProject, runtimeName, monitor = false)

      // runtime status should be Deleting
      Leonardo.cluster.getRuntime(billingProject, runtimeName).status shouldBe ClusterStatus.Deleting

      // Call delete again. This should succeed, and not change the status.
      deleteRuntime(billingProject, runtimeName, monitor = false)
      Leonardo.cluster.getRuntime(billingProject, runtimeName).status shouldBe ClusterStatus.Deleting

      // Can't recreate while runtime is deleting
      val caught3 =
        the[RestError] thrownBy createNewRuntime(billingProject, runtimeName, runtimeRequest, monitor = false)
      caught3.statusCode shouldBe (org.http4s.Status.Conflict)

      // Wait for the runtime to be deleted
      monitorDeleteRuntime(billingProject, runtimeName)

      // New runtime can now be recreated with the same name
      // We monitor creation to make sure it gets successfully created in Google.
      withNewRuntime(billingProject, runtimeName, runtimeRequest, monitorCreate = true, monitorDelete = false)(noop)
    }

    "error'd runtimes with bad user script should transition correctly" in { billingProject =>
      // make an Error'd runtime
      withNewErroredRuntime(billingProject, false) { runtime =>
        // runtime should be in Error status
        runtime.status shouldBe ClusterStatus.Error

        // can't stop an Error'd runtime
        val caught =
          the[RestException] thrownBy stopRuntime(runtime.googleProject, runtime.runtimeName, monitor = false)
        caught.message should include(""""statusCode":409""")

        // can't recreate an Error'd runtime
        val caught2 =
          the[RestError] thrownBy createNewRuntime(runtime.googleProject, runtime.runtimeName, monitor = false)
        caught2.statusCode shouldBe (org.http4s.Status.Conflict)

        // can delete an Error'd runtime
      }
    }

    "Runtime with bad user startup script should transition correctly" in { billingProject =>
      withNewErroredRuntime(billingProject, true) { runtime =>
        runtime.status shouldBe ClusterStatus.Error

        // can't stop an Error'd runtime
        val caught =
          the[RestException] thrownBy stopRuntime(runtime.googleProject, runtime.runtimeName, monitor = false)
        caught.message should include(""""statusCode":409""")

        // can't recreate an Error'd runtime
        val caught2 =
          the[RestError] thrownBy createNewRuntime(runtime.googleProject, runtime.runtimeName, monitor = false)
        caught2.statusCode shouldBe (org.http4s.Status.Conflict)
      }
    }
    // Note: omitting stop/start and patch/update tests here because those are covered in more depth in NotebookClusterMonitoringSpec
  }

}
