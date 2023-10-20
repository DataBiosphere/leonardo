package org.broadinstitute.dsde.workbench.leonardo.azure

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.GeneratedLeonardoClient
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.client.leonardo.api.{DisksApi, RuntimesApi}
import org.broadinstitute.dsde.workbench.client.leonardo.model.{
  AzureDiskConfig,
  ClusterStatus,
  CreateAzureRuntimeRequest,
  DiskStatus
}
import org.broadinstitute.dsde.workbench.google2.streamUntilDoneOrTimeout
import org.broadinstitute.dsde.workbench.leonardo.LeonardoTestTags.ExcludeFromJenkins
import org.broadinstitute.dsde.workbench.leonardo.TestUser.Hermione
import org.broadinstitute.dsde.workbench.leonardo.{AzureBilling, LeonardoTestUtils, RuntimeName}
import org.broadinstitute.dsde.workbench.service.test.CleanUp
import org.http4s.headers.Authorization
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{DoNotDiscover, ParallelTestExecution, Retries}

import scala.concurrent.duration._

@DoNotDiscover
class AzureRuntimeSpec
    extends AzureBilling
    with LeonardoTestUtils
    with ParallelTestExecution
    with TableDrivenPropertyChecks
    with Retries
    with CleanUp {

  implicit val accessToken: IO[AuthToken] = Hermione.authToken()
  implicit val authorization: IO[Authorization] = Hermione.authorization()

  def createRuntime(workspaceId: String,
                    labelMap: java.util.HashMap[String, String],
                    runtimeName: RuntimeName,
                    runtimeClient: RuntimesApi,
                    diskClient: DisksApi
  ): Unit =
    for {

      _ <- loggerIO.info(s"AzureRuntimeSpec: Create runtime request submitted. Starting to poll GET")

      createReq = new CreateAzureRuntimeRequest()
        .labels(labelMap)
        .machineSize("Standard_DS1_v2")
        .disk(
          new AzureDiskConfig()
            .name(generateAzureDiskName())
            .size(50)
            .labels(labelMap)
        )

      _ <- IO(runtimeClient.createAzureRuntime(workspaceId, runtimeName.asString, false, createReq))
      _ <- loggerIO.info(s"AzureRuntimeSpec: Create runtime request submitted. Starting to poll GET")

      // Verify the initial getRuntime call
      _ <- IO.sleep(5 seconds)
      callGetRuntime = IO(runtimeClient.getAzureRuntime(workspaceId, runtimeName.asString))

      intitialGetRuntimeResponse <- callGetRuntime
      _ <- loggerIO.info(s"initial get runtime response ${intitialGetRuntimeResponse}")
      _ = intitialGetRuntimeResponse.getStatus shouldBe ClusterStatus.CREATING

      _ <- loggerIO.info(
        s"AzureRuntimeSpec: runtime ${workspaceId}/${runtimeName.asString} in creating status detected"
      )

      _ <- loggerIO.info("AzureRuntimeSpec: verifying get disk response")
      diskId = intitialGetRuntimeResponse.getRuntimeConfig.getAzureConfig.getPersistentDiskId
      getDisk = IO(diskClient.getDiskV2(diskId.toBigInteger.intValue()))
      diskDuringRuntimeCreate <- getDisk
      _ = diskDuringRuntimeCreate.getStatus shouldBe DiskStatus.CREATING

      _ <- loggerIO.info(
        s"AzureRuntimeSpec: disk ${workspaceId}/${diskDuringRuntimeCreate.getId()} in creating status detected"
      )

      // Verify the runtime eventually becomes Running (in 40 minutes)
      monitorCreateResult <- streamUntilDoneOrTimeout(
        callGetRuntime,
        240,
        10 seconds,
        s"AzureRuntimeSpec: runtime ${workspaceId}/${runtimeName.asString} did not finish creating after 40 minutes"
      )(implicitly, GeneratedLeonardoClient.runtimeInStateOrError(ClusterStatus.RUNNING))

      _ <- loggerIO.info(
        s"AzureRuntimeSpec: runtime ${workspaceId}/${runtimeName.asString} create monitor result: $monitorCreateResult"
      )
      _ = monitorCreateResult.getStatus() shouldBe ClusterStatus.RUNNING
    } yield ()

  def deleteWorkspace(workspaceId: String,
                      runtimeName: RuntimeName,
                      runtimeClient: RuntimesApi,
                      diskClient: DisksApi
  ): Unit =
    for {

      // Delete the runtime
      _ <- loggerIO.info(
        s"AzureRuntimeSpec: runtime ${workspaceId}/${runtimeName.asString} delete starting"
      )
      _ <- IO(runtimeClient.deleteAzureRuntime(workspaceId, runtimeName.asString, true))

      _ <- loggerIO.info(
        s"AzureRuntime: runtime ${workspaceId}/${runtimeName.asString} delete called, starting to poll on deletion"
      )

      callGetRuntime2 = IO(runtimeClient.getAzureRuntime(workspaceId, runtimeName.asString))
      monitorDeleteResult <- streamUntilDoneOrTimeout(
        callGetRuntime2,
        240,
        10 seconds,
        s"AzureRuntimeSpec: runtime ${workspaceId}/${runtimeName.asString} did not finish deleting after 40 minutes"
      )(implicitly, GeneratedLeonardoClient.runtimeInStateOrError(ClusterStatus.DELETED))

      _ <- loggerIO.info(
        s"AzureRuntime: runtime ${workspaceId}/${runtimeName.asString} delete monitor result: $monitorDeleteResult"
      )
      _ = monitorDeleteResult.getStatus() shouldBe ClusterStatus.DELETED

      deletingRuntimeResponse <- callGetRuntime2
      diskId = deletingRuntimeResponse.getRuntimeConfig.getAzureConfig.getPersistentDiskId
      getDisk = IO(diskClient.getDiskV2(diskId.toBigInteger.intValue()))
      diskAfterRuntimeDelete <- getDisk
      _ = diskAfterRuntimeDelete.getStatus shouldBe DiskStatus.DELETED

      _ <- loggerIO.info(
        s"AzureRuntimeSpec: disk ${workspaceId}/${diskAfterRuntimeDelete.getId()} in deleted status detected"
      )
    } yield ()

  "stop, start azure runtime" taggedAs ExcludeFromJenkins in { workspaceDetails =>
    val workspaceId = workspaceDetails.workspace.workspaceId

    val labelMap: java.util.HashMap[String, String] = new java.util.HashMap[String, String]()
    labelMap.put("automation", "true")

    val runtimeName = randomClusterName
    val res =
      for {
        runtimeClient <- GeneratedLeonardoClient.generateRuntimesApi
        diskClient <- GeneratedLeonardoClient.generateDisksApi

        _ <- loggerIO.info(s"AzureRuntimeSpec: About to create runtime")

        _ = createRuntime(workspaceId, labelMap, runtimeName, runtimeClient, diskClient)

        // now that the runtime is running, stop it
        _ <- IO(runtimeClient.stopRuntimeV2(workspaceId, runtimeName.asString))

        callGetRuntime = IO(runtimeClient.getAzureRuntime(workspaceId, runtimeName.asString))
        stoppingRuntimeResponse <- callGetRuntime
        _ <- loggerIO.info(s"stopping get runtime response ${stoppingRuntimeResponse}")
        _ = stoppingRuntimeResponse.getStatus shouldBe ClusterStatus.STOPPING

        _ <- loggerIO.info(
          s"AzureRuntimeSpec: runtime ${workspaceId}/${runtimeName.asString} in stopping status detected"
        )

        // Verify the runtime eventually becomes Stopped
        monitorStopResult <- streamUntilDoneOrTimeout(
          callGetRuntime,
          120,
          10 seconds,
          s"AzureRuntimeSpec: runtime ${workspaceId}/${runtimeName.asString} did not stop after 20 minutes"
        )(implicitly, GeneratedLeonardoClient.runtimeInStateOrError(ClusterStatus.STOPPED))

        _ <- loggerIO.info(
          s"AzureRuntimeSpec: runtime ${workspaceId}/${runtimeName.asString} stopped monitor result: $monitorStopResult"
        )
        _ = monitorStopResult.getStatus shouldBe ClusterStatus.STOPPED

        // now that the runtime is stopped, start it
        _ <- IO(runtimeClient.startRuntimeV2(workspaceId, runtimeName.asString))

        startingRuntimeResponse <- callGetRuntime
        _ <- loggerIO.info(s"starting get runtime response ${startingRuntimeResponse}")
        _ = startingRuntimeResponse.getStatus shouldBe ClusterStatus.STARTING

        _ <- loggerIO.info(
          s"AzureRuntimeSpec: runtime ${workspaceId}/${runtimeName.asString} in starting status detected"
        )

        // Verify the runtime eventually becomes Started
        monitorStartResult <- streamUntilDoneOrTimeout(
          callGetRuntime,
          120,
          10 seconds,
          s"AzureRuntimeSpec: runtime ${workspaceId}/${runtimeName.asString} did not start after 20 minutes"
        )(implicitly, GeneratedLeonardoClient.runtimeInStateOrError(ClusterStatus.RUNNING))

        _ <- loggerIO.info(
          s"AzureRuntimeSpec: runtime ${workspaceId}/${runtimeName.asString} start monitor result: $monitorStartResult"
        )
        _ = monitorStartResult.getStatus shouldBe ClusterStatus.RUNNING

        _ <- loggerIO.info(
          s"AzureRuntimeSpec: about to test proxyUrl"
        )

        _ <- GeneratedLeonardoClient.client.use { c =>
          implicit val client = c
          GeneratedLeonardoClient.testProxyUrl(monitorStartResult)
        }

        _ = deleteWorkspace(workspaceId, runtimeName, runtimeClient, diskClient)

        _ <- IO.sleep(1 minute) // sleep for a minute before cleaning up workspace
      } yield ()
    res.unsafeRunSync()
  }

  "create, get, delete azure runtime" taggedAs ExcludeFromJenkins in { workspaceDetails =>
    val workspaceId = workspaceDetails.workspace.workspaceId

    val labelMap: java.util.HashMap[String, String] = new java.util.HashMap[String, String]()
    labelMap.put("automation", "true")

    val runtimeName = randomClusterName
    val res =
      for {
        _ <- loggerIO.info(s"AzureRuntimeSpec: About to create runtime")

        runtimeClient <- GeneratedLeonardoClient.generateRuntimesApi
        diskClient <- GeneratedLeonardoClient.generateDisksApi

        _ = createRuntime(workspaceId, labelMap, runtimeName, runtimeClient, diskClient)

        _ = deleteWorkspace(workspaceId, runtimeName, runtimeClient, diskClient)

        _ <- IO.sleep(1 minute) // sleep for a minute before cleaning up workspace
      } yield ()
    res.unsafeRunSync()
  }

}
