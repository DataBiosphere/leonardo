package org.broadinstitute.dsde.workbench.leonardo
package runtimes

import cats.effect.{IO, Sync}
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.DoneCheckable
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.google2.{streamFUntilDone, streamUntilDoneOrTimeout, DiskName, MachineTypeName}
import org.broadinstitute.dsde.workbench.leonardo.GPAllocFixtureSpec.gpallocProjectKey
import org.broadinstitute.dsde.workbench.leonardo.LeonardoApiClient._
import org.broadinstitute.dsde.workbench.leonardo.http.{
  PersistentDiskRequest,
  RuntimeConfigRequest,
  UpdateRuntimeConfigRequest,
  UpdateRuntimeRequest
}
import org.broadinstitute.dsde.workbench.leonardo.notebooks.{NotebookTestUtils, Python3}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.service.util.Tags
import org.http4s.headers.Authorization
import org.http4s.{AuthScheme, Credentials}
import org.scalatest.{DoNotDiscover, Outcome}
import org.scalatest.freespec.FixtureAnyFreeSpec

import scala.concurrent.duration._

// extending fixture.Freespec just so we can use `taggedAs`. Not really need fixture for anything
@DoNotDiscover
class RuntimePatchSpec extends FixtureAnyFreeSpec with LeonardoTestUtils with LeonardoTestSuite with NotebookTestUtils {
  implicit val ronToken: AuthToken = ronAuthToken
  implicit val auth: Authorization = Authorization(Credentials.Token(AuthScheme.Bearer, ronCreds.makeAuthToken().value))

  //this is an end to end test of the pub/sub infrastructure
  "Patch endpoint should perform a stop/start tranition for GCE VM" taggedAs Tags.SmokeTest in { _ =>
    val billingProjectString =
      sys.props.get(gpallocProjectKey).getOrElse(throw new Exception("Billing project is not set"))
    val googleProject = GoogleProject(billingProjectString)
    // create a new GCE runtime
    val runtimeName = randomClusterName

    val newMasterMachineType = MachineTypeName("n1-standard-2")
    val newDiskSize = DiskSize(20)
    val updateRuntimeRequest = UpdateRuntimeRequest(Some(
                                                      UpdateRuntimeConfigRequest.GceConfig(
                                                        Some(newMasterMachineType),
                                                        Some(newDiskSize)
                                                      )
                                                    ),
                                                    true,
                                                    None,
                                                    None,
                                                    Map.empty,
                                                    Set.empty)
    val createRuntimeRequest = defaultCreateRuntime2Request.copy(
      runtimeConfig = Some(
        RuntimeConfigRequest.GceConfig(
          Some(MachineTypeName("n1-standard-4")),
          Some(DiskSize(10))
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
        _ <- updateRuntime(googleProject, runtimeName, updateRuntimeRequest)
        _ <- testTimer.sleep(30 seconds) //We need this because DB update happens in subscriber for update API.
        ioa = LeonardoApiClient.getRuntime(googleProject, runtimeName)
        getRuntimeResult <- ioa
        _ = getRuntimeResult.status shouldBe ClusterStatus.Stopping
        monitorStoppingResult <- testTimer.sleep(30 seconds) >> streamFUntilDone(ioa, 20, 10 seconds)(
          testTimer,
          stoppingDoneCheckable
        ).compile.lastOrError
        _ = monitorStoppingResult.status shouldBe ClusterStatus.Starting
        monitoringStartingResult <- testTimer.sleep(50 seconds) >> streamFUntilDone(ioa, 30, 10 seconds)(
          testTimer,
          startingDoneCheckable
        ).compile.lastOrError
        clusterCopy = ClusterCopy.fromGetRuntimeResponseCopy(getRuntimeResult)
        _ <- IO(
          withWebDriver { implicit driver =>
            withNewNotebook(clusterCopy, Python3) { notebookPage =>
              //all other packages cannot be tested for their versions in this manner
              //warnings are ignored because they are benign warnings that show up for python2 because of compilation against an older numpy
              val res = notebookPage
                .executeCell(
                  "! df -H |grep sdb"
                )
                .get
              res should include("22G") //disk output is always a few more gb than what's specified
            }
          }
        )
      } yield {
        monitoringStartingResult.status shouldBe ClusterStatus.Running
        val res = monitoringStartingResult.runtimeConfig
          .asInstanceOf[RuntimeConfig.GceConfig]
        res.machineType shouldBe newMasterMachineType
        res.diskSize shouldBe newDiskSize
      }
    }
    res.unsafeRunSync
  }

  //this is an end to end test of the pub/sub infrastructure
  "Patch endpoint should perform a stop/start tranition for GCE VM with PD" taggedAs Tags.SmokeTest in { _ =>
    val billingProjectString =
      sys.props.get(gpallocProjectKey).getOrElse(throw new Exception("Billing project is not set"))
    val googleProject = GoogleProject(billingProjectString)
    // create a new GCE runtime
    val runtimeName = randomClusterName

    val newMasterMachineType = MachineTypeName("n1-standard-2")
    val newDiskSize = DiskSize(20)
    val updateRuntimeRequest = UpdateRuntimeRequest(Some(
                                                      UpdateRuntimeConfigRequest.GceConfig(
                                                        Some(newMasterMachineType),
                                                        Some(newDiskSize)
                                                      )
                                                    ),
                                                    true,
                                                    None,
                                                    None,
                                                    Map.empty,
                                                    Set.empty)
    val createRuntimeRequest = defaultCreateRuntime2Request.copy(
      runtimeConfig = Some(
        RuntimeConfigRequest.GceWithPdConfig(
          Some(MachineTypeName("n1-standard-4")),
          PersistentDiskRequest(
            DiskName("pd-test"),
            Some(DiskSize(10)),
            None,
            Map.empty
          )
        )
      )
    )

    val res = LeonardoApiClient.client.use { implicit c =>
      val startingDoneCheckable: DoneCheckable[GetRuntimeResponseCopy] =
        x => x.status == ClusterStatus.Running

      for {
        _ <- createRuntimeWithWait(googleProject, runtimeName, createRuntimeRequest)
        _ <- updateRuntime(googleProject, runtimeName, updateRuntimeRequest)
        _ <- testTimer.sleep(70 seconds) //We need this because DB update happens in subscriber for update API.
        ioa = LeonardoApiClient.getRuntime(googleProject, runtimeName)
        getRuntimeResult <- ioa
        monitoringStartingResult <- testTimer.sleep(50 seconds) >> streamFUntilDone(ioa, 30, 10 seconds)(
          testTimer,
          startingDoneCheckable
        ).compile.lastOrError
        clusterCopy = ClusterCopy.fromGetRuntimeResponseCopy(getRuntimeResult)
        _ <- IO(
          withWebDriver { implicit driver =>
            withNewNotebook(clusterCopy, Python3) { notebookPage =>
              //all other packages cannot be tested for their versions in this manner
              //warnings are ignored because they are benign warnings that show up for python2 because of compilation against an older numpy
              val res = notebookPage
                .executeCell(
                  "! df -H |grep sdb"
                )
                .get
              res should include("22G") //disk output is always a few more gb than what's specified
            }
          }
        )
      } yield {
        monitoringStartingResult.status shouldBe ClusterStatus.Running
        val res = monitoringStartingResult.runtimeConfig
          .asInstanceOf[RuntimeConfig.GceWithPdConfig]
        res.machineType shouldBe newMasterMachineType
      }
    }
    res.unsafeRunSync
  }

  "Patch endpoint should perform a stop/start tranition for Dataproc cluster" taggedAs Tags.SmokeTest in { _ =>
    val billingProjectString =
      sys.props.get(gpallocProjectKey).getOrElse(throw new Exception("Billing project is not set"))
    val googleProject = GoogleProject(billingProjectString)
    val newMasterMachineType = MachineTypeName("n1-standard-2")
    val newDiskSize = DiskSize(60)
    val updateRuntimeRequest = UpdateRuntimeRequest(Some(
                                                      UpdateRuntimeConfigRequest.DataprocConfig(
                                                        Some(newMasterMachineType),
                                                        Some(newDiskSize),
                                                        None,
                                                        None
                                                      )
                                                    ),
                                                    true,
                                                    None,
                                                    None,
                                                    Map.empty,
                                                    Set.empty)
    val runtimeName = randomClusterName
    val createRuntimeRequest = defaultCreateRuntime2Request.copy(
      runtimeConfig = Some(
        RuntimeConfigRequest.DataprocConfig(
          None,
          Some(MachineTypeName("n1-standard-4")),
          Some(DiskSize(50)),
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
        _ <- updateRuntime(googleProject, runtimeName, updateRuntimeRequest)
        _ <- testTimer.sleep(30 seconds) //We need this because DB update happens in subscriber for update API.
        ioa = LeonardoApiClient.getRuntime(googleProject, runtimeName)
        getRuntimeResult <- ioa
        _ = getRuntimeResult.status shouldBe ClusterStatus.Stopping
        monitorStoppingResult <- testTimer.sleep(30 seconds) >> streamUntilDoneOrTimeout(
          ioa,
          30,
          10 seconds,
          s"Stopping ${googleProject}/${runtimeName} timed out"
        )(
          implicitly[Sync[IO]],
          testTimer,
          stoppingDoneCheckable
        )
        _ = monitorStoppingResult.status shouldBe ClusterStatus.Starting
        monitringStartingResult <- testTimer.sleep(50 seconds) >> streamUntilDoneOrTimeout(
          ioa,
          30,
          10 seconds,
          s"starting ${googleProject}/${runtimeName} timed out"
        )(
          implicitly[Sync[IO]],
          testTimer,
          startingDoneCheckable
        )
        clusterCopy = ClusterCopy.fromGetRuntimeResponseCopy(getRuntimeResult)
        _ <- IO(
          withWebDriver { implicit driver =>
            withNewNotebook(clusterCopy, Python3) { notebookPage =>
              //all other packages cannot be tested for their versions in this manner
              //warnings are ignored because they are benign warnings that show up for python2 because of compilation against an older numpy
              val res = notebookPage
                .executeCell(
                  "! df -H |grep sda1"
                )
                .get
              res should include("64G") //disk output is always a few more gb than what's specified
            }
          }
        )
      } yield {
        monitringStartingResult.status shouldBe ClusterStatus.Running
        val res = monitringStartingResult.runtimeConfig
          .asInstanceOf[RuntimeConfig.DataprocConfig]
        res.masterMachineType shouldBe newMasterMachineType
        res.diskSize shouldBe newDiskSize
      }
    }
    res.unsafeRunSync
  }

  override protected def withFixture(test: OneArgTest): Outcome = super.withFixture(test.toNoArgTest(()))

  override type FixtureParam = Unit
}
