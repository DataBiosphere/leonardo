package org.broadinstitute.dsde.workbench.leonardo
package runtimes

import cats.implicits._
import org.broadinstitute.dsde.workbench.DoneCheckable
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.google2.{streamFUntilDone, MachineTypeName}
import org.broadinstitute.dsde.workbench.leonardo.GPAllocFixtureSpec.gpallocProjectKey
import org.broadinstitute.dsde.workbench.leonardo.RuntimeFixtureSpec._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.service.util.Tags
import org.http4s.headers.Authorization
import org.http4s.{AuthScheme, Credentials}
import org.scalatest.time.{Minutes, Seconds, Span}
import org.scalatest.{fixture, DoNotDiscover, Outcome}

import scala.concurrent.duration._

// extending fixture.Freespec just so we can use `taggedAs`. Not really need fixture for anything
@DoNotDiscover
class RuntimePatchSpec extends fixture.FreeSpec with LeonardoTestUtils with LeonardoTestSuite {
  implicit val ronToken: AuthToken = ronAuthToken
  implicit val auth: Authorization = Authorization(Credentials.Token(AuthScheme.Bearer, ronCreds.makeAuthToken().value))

  //this is an end to end test of the pub/sub infrastructure
  "Patch endpoint should perform a stop/start tranition for GCE VM" taggedAs Tags.SmokeTest in { _ =>
    val billingProjectString =
      sys.props.get(gpallocProjectKey).getOrElse(throw new Exception("Billing project is not set"))
    val googleProject = GoogleProject(billingProjectString)
    // create a new GCE runtime
    val runtime = createNewRuntime(googleProject, request = getRuntimeRequest(CloudService.GCE, None))(ronAuthToken)

    val newMasterMachineType = "n1-standard-2"
    val runtimeConfig = UpdateRuntimeConfigRequestCopy.GceConfig(
      Some(newMasterMachineType),
      None
    )

    val originalCluster =
      Leonardo.cluster.getRuntime(googleProject, runtime.clusterName)
    originalCluster.status shouldBe ClusterStatus.Running

    val originalMachineConfig = originalCluster.runtimeConfig

    Leonardo.cluster.updateRuntime(
      runtime.googleProject,
      runtime.clusterName,
      request = UpdateRuntimeRequestCopy(Some(runtimeConfig), true, None, None)
    )

    eventually(timeout(Span(10, Minutes)), interval(Span(10, Seconds))) {
      val getRuntime =
        Leonardo.cluster.getRuntime(runtime.googleProject, runtime.clusterName)
      getRuntime.status shouldBe ClusterStatus.Stopping
    }

    eventually(timeout(Span(10, Minutes)), interval(Span(30, Seconds))) {
      val getRuntime =
        Leonardo.cluster.getRuntime(runtime.googleProject, runtime.clusterName)
      getRuntime.status shouldBe ClusterStatus.Running
      getRuntime.runtimeConfig shouldBe originalMachineConfig
        .asInstanceOf[RuntimeConfig.GceConfig]
        .copy(machineType = MachineTypeName(newMasterMachineType))
    }
  }

  "Patch endpoint should perform a stop/start tranition for Dataproc cluster" taggedAs Tags.SmokeTest in { _ =>
    val billingProjectString =
      sys.props.get(gpallocProjectKey).getOrElse(throw new Exception("Billing project is not set"))
    val googleProject = GoogleProject(billingProjectString)
    // create a new GCE runtime
    val runtime =
      createNewRuntime(googleProject, request = getRuntimeRequest(CloudService.Dataproc, None))(ronAuthToken)

    val newMasterMachineType = "n1-standard-2"
    val runtimeConfig = UpdateRuntimeConfigRequestCopy.DataprocConfig(
      Some(newMasterMachineType),
      None,
      None,
      None
    )

    val originalCluster =
      Leonardo.cluster.getRuntime(googleProject, runtime.clusterName)
    originalCluster.status shouldBe ClusterStatus.Running

    val originalMachineConfig = originalCluster.runtimeConfig

    Leonardo.cluster.updateRuntime(
      runtime.googleProject,
      runtime.clusterName,
      request = UpdateRuntimeRequestCopy(Some(runtimeConfig), true, None, None)
    )

    val res = LeonardoApiClient.client.use { c =>
      implicit val httpClient = c
      val stoppingDoneCheckable: DoneCheckable[GetRuntimeResponseCopy] =
        x => x.status == ClusterStatus.Starting
      val startingDoneCheckable: DoneCheckable[GetRuntimeResponseCopy] =
        x => x.status == ClusterStatus.Running

      val ioa = LeonardoApiClient.getRuntime(runtime.googleProject, runtime.clusterName)
      for {
        getRuntimeResult <- ioa
        _ = getRuntimeResult.status shouldBe ClusterStatus.Stopping
        monitorStoppingResult <- testTimer.sleep(30 seconds) >> streamFUntilDone(ioa, 20, 10 seconds)(
          testTimer,
          stoppingDoneCheckable
        ).compile.lastOrError
        _ = monitorStoppingResult.status shouldBe ClusterStatus.Starting
        monitringStartingResult <- testTimer.sleep(30 seconds) >> streamFUntilDone(ioa, 20, 10 seconds)(
          testTimer,
          startingDoneCheckable
        ).compile.lastOrError
      } yield {
        monitringStartingResult.status shouldBe ClusterStatus.Running
        monitringStartingResult.runtimeConfig shouldBe originalMachineConfig
          .asInstanceOf[RuntimeConfig.DataprocConfig]
          .copy(masterMachineType = MachineTypeName(newMasterMachineType))
      }
    }
    res.unsafeRunSync
  }

  override protected def withFixture(test: OneArgTest): Outcome = super.withFixture(test.toNoArgTest(()))

  override type FixtureParam = Unit
}
