package org.broadinstitute.dsde.workbench.leonardo
package runtimes

import cats.effect.IO
import cats.implicits._
import org.broadinstitute.dsde.workbench.DoneCheckable
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.google2.{streamFUntilDone, MachineTypeName}
import org.broadinstitute.dsde.workbench.leonardo.GPAllocFixtureSpec.gpallocProjectKey
import org.broadinstitute.dsde.workbench.leonardo.LeonardoApiClient._
import org.broadinstitute.dsde.workbench.leonardo.http.RuntimeConfigRequest
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.service.util.Tags
import org.http4s.headers.Authorization
import org.http4s.{AuthScheme, Credentials}
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
    val runtimeName = randomClusterName

    val newMasterMachineType = "n1-standard-2"
    val newRuntimeConfig = UpdateRuntimeConfigRequestCopy.GceConfig(
      Some(newMasterMachineType),
      None
    )
    val createRuntimeRequest = defaultCreateRuntime2Request.copy(
      runtimeConfig = Some(
        RuntimeConfigRequest.GceConfig(
          Some(MachineTypeName("n1-standard-4")),
          None
        )
      )
    )

    val res = LeonardoApiClient.client.use { c =>
      implicit val httpClient = c
      val stoppingDoneCheckable: DoneCheckable[GetRuntimeResponseCopy] =
        x => x.status == ClusterStatus.Starting
      val startingDoneCheckable: DoneCheckable[GetRuntimeResponseCopy] =
        x => x.status == ClusterStatus.Running

      for {
        _ <- createRuntimeWithWait(googleProject, runtimeName, createRuntimeRequest)
        _ <- IO {
          Leonardo.cluster.updateRuntime(
            googleProject,
            runtimeName,
            request = UpdateRuntimeRequestCopy(Some(newRuntimeConfig), true, None, None)
          )
        }
        _ <- testTimer.sleep(30 seconds) //We need this because DB update happens in subscriber for update API.
        ioa = LeonardoApiClient.getRuntime(googleProject, runtimeName)
        getRuntimeResult <- ioa
        _ = getRuntimeResult.status shouldBe ClusterStatus.Stopping
        monitorStoppingResult <- testTimer.sleep(30 seconds) >> streamFUntilDone(ioa, 20, 10 seconds)(
          testTimer,
          stoppingDoneCheckable
        ).compile.lastOrError
        _ = monitorStoppingResult.status shouldBe ClusterStatus.Starting
        monitringStartingResult <- testTimer.sleep(50 seconds) >> streamFUntilDone(ioa, 30, 10 seconds)(
          testTimer,
          startingDoneCheckable
        ).compile.lastOrError
      } yield {
        monitringStartingResult.status shouldBe ClusterStatus.Running
        monitringStartingResult.runtimeConfig
          .asInstanceOf[RuntimeConfig.DataprocConfig]
          .masterMachineType shouldBe MachineTypeName(newMasterMachineType)
      }
    }
    res.unsafeRunSync
  }

  "Patch endpoint should perform a stop/start tranition for Dataproc cluster" taggedAs Tags.SmokeTest in { _ =>
    val billingProjectString =
      sys.props.get(gpallocProjectKey).getOrElse(throw new Exception("Billing project is not set"))
    val googleProject = GoogleProject(billingProjectString)
    val newMasterMachineType = "n1-standard-2"
    val newRuntimeConfig = UpdateRuntimeConfigRequestCopy.DataprocConfig(
      Some(newMasterMachineType),
      None,
      None,
      None
    )

    val runtimeName = randomClusterName
    val createRuntimeRequest = defaultCreateRuntime2Request.copy(
      runtimeConfig = Some(
        RuntimeConfigRequest.DataprocConfig(
          None,
          Some(MachineTypeName("n1-standard-4")),
          None,
          None,
          None,
          None,
          None,
          Map.empty
        )
      )
    )
    val res = LeonardoApiClient.client.use { c =>
      implicit val httpClient = c
      val stoppingDoneCheckable: DoneCheckable[GetRuntimeResponseCopy] =
        x => x.status == ClusterStatus.Starting
      val startingDoneCheckable: DoneCheckable[GetRuntimeResponseCopy] =
        x => x.status == ClusterStatus.Running

      for {
        _ <- createRuntimeWithWait(googleProject, runtimeName, createRuntimeRequest)
        _ <- IO {
          Leonardo.cluster.updateRuntime(
            googleProject,
            runtimeName,
            request = UpdateRuntimeRequestCopy(Some(newRuntimeConfig), true, None, None)
          )
        }
        _ <- testTimer.sleep(30 seconds) //We need this because DB update happens in subscriber for update API.
        ioa = LeonardoApiClient.getRuntime(googleProject, runtimeName)
        getRuntimeResult <- ioa
        _ = getRuntimeResult.status shouldBe ClusterStatus.Stopping
        monitorStoppingResult <- testTimer.sleep(30 seconds) >> streamFUntilDone(ioa, 20, 10 seconds)(
          testTimer,
          stoppingDoneCheckable
        ).compile.lastOrError
        _ = monitorStoppingResult.status shouldBe ClusterStatus.Starting
        monitringStartingResult <- testTimer.sleep(50 seconds) >> streamFUntilDone(ioa, 30, 10 seconds)(
          testTimer,
          startingDoneCheckable
        ).compile.lastOrError
      } yield {
        monitringStartingResult.status shouldBe ClusterStatus.Running
        monitringStartingResult.runtimeConfig
          .asInstanceOf[RuntimeConfig.DataprocConfig]
          .masterMachineType shouldBe MachineTypeName(newMasterMachineType)
      }
    }
    res.unsafeRunSync
  }

  override protected def withFixture(test: OneArgTest): Outcome = super.withFixture(test.toNoArgTest(()))

  override type FixtureParam = Unit
}
