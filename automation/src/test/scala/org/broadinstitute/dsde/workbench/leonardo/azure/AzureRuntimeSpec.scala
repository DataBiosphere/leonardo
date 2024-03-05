package org.broadinstitute.dsde.workbench.leonardo.azure

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.GeneratedLeonardoClient
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.client.leonardo.model.{
  AzureDiskConfig,
  ClusterStatus,
  CreateAzureRuntimeRequest,
  DiskStatus
}
import org.broadinstitute.dsde.workbench.google2.streamUntilDoneOrTimeout
import org.broadinstitute.dsde.workbench.leonardo.LeonardoTestTags.ExcludeFromJenkins
import org.broadinstitute.dsde.workbench.leonardo.{AzureBilling, LeonardoTestUtils}
import org.broadinstitute.dsde.workbench.pipeline.TestUser.Hermione
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

  "create, get, delete azure runtime" taggedAs ExcludeFromJenkins in { workspaceDetails =>
    implicit val accessToken: IO[AuthToken] = Hermione.authToken()
    implicit val authorization: IO[Authorization] = Hermione.authorization()
    val workspaceId = workspaceDetails.workspace.workspaceId

    val labelMap: java.util.HashMap[String, String] = new java.util.HashMap[String, String]()
    labelMap.put("automation", "true")

    val runtimeName = randomClusterName
    val res =
      for {
        _ <- loggerIO.info(s"AzureRuntimeSpec: About to create runtime")

        runtimeClient <- GeneratedLeonardoClient.generateRuntimesApi
        diskClient <- GeneratedLeonardoClient.generateDisksApi

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

        // Verify the runtime eventually becomes Running (in 25 minutes)
        // will reduce to 20 once https://broadworkbench.atlassian.net/browse/WOR-1397 is merged
        monitorCreateResult <- streamUntilDoneOrTimeout(
          callGetRuntime,
          150,
          10 seconds,
          s"AzureRuntimeSpec: runtime ${workspaceId}/${runtimeName.asString} did not finish creating after 25 minutes"
        )(implicitly, GeneratedLeonardoClient.runtimeInStateOrError(ClusterStatus.RUNNING))

        _ <- loggerIO.info(
          s"AzureRuntimeSpec: runtime ${workspaceId}/${runtimeName.asString} create monitor result: $monitorCreateResult"
        )
        _ = monitorCreateResult.getStatus() shouldBe ClusterStatus.RUNNING

        // Now stop the runtime
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
          60,
          10 seconds,
          s"AzureRuntimeSpec: runtime ${workspaceId}/${runtimeName.asString} did not stop after 10 minutes"
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
          60,
          10 seconds,
          s"AzureRuntimeSpec: runtime ${workspaceId}/${runtimeName.asString} did not start after 10 minutes"
        )(implicitly, GeneratedLeonardoClient.runtimeInStateOrError(ClusterStatus.RUNNING))

        _ <- loggerIO.info(
          s"AzureRuntimeSpec: runtime ${workspaceId}/${runtimeName.asString} start monitor result: $monitorStartResult"
        )
        _ = monitorStartResult.getStatus shouldBe ClusterStatus.RUNNING

        _ <- loggerIO.info(
          s"AzureRuntimeSpec: runtime ${workspaceId}/${runtimeName.asString} delete starting"
        )
        // Delete the runtime
        _ <- IO(runtimeClient.deleteAzureRuntime(workspaceId, runtimeName.asString, true))

        _ <- loggerIO.info(
          s"AzureRuntimeSpec: about to test proxyUrl"
        )

        _ <- GeneratedLeonardoClient.client.use { c =>
          implicit val client = c
          GeneratedLeonardoClient.testProxyUrl(monitorCreateResult)
        }

        _ <- loggerIO.info(
          s"AzureRuntimeSpec: runtime ${workspaceId}/${runtimeName.asString} delete called, starting to poll on deletion"
        )

        callGetRuntime2 = IO(runtimeClient.getAzureRuntime(workspaceId, runtimeName.asString))
        monitorDeleteResult <- streamUntilDoneOrTimeout(
          callGetRuntime2,
          240,
          10 seconds,
          s"AzureRuntimeSpec: runtime ${workspaceId}/${runtimeName.asString} did not finish deleting after 40 minutes"
        )(implicitly, GeneratedLeonardoClient.runtimeInStateOrError(ClusterStatus.DELETED))

        _ <- loggerIO.info(
          s"AzureRuntimeSpec: runtime ${workspaceId}/${runtimeName.asString} delete monitor result: $monitorDeleteResult"
        )
        _ = monitorDeleteResult.getStatus() shouldBe ClusterStatus.DELETED

        diskAfterRuntimeDelete <- getDisk
        _ <- loggerIO.info(
          s"AzureRuntimeSpec: disk $workspaceId/${diskAfterRuntimeDelete.getName} delete monitor result: $diskAfterRuntimeDelete"
        )
        _ = diskAfterRuntimeDelete.getStatus should (be(DiskStatus.DELETED) or be(DiskStatus.DELETING))

        _ <- loggerIO.info(
          s"AzureRuntimeSpec: disk ${workspaceId}/${diskAfterRuntimeDelete.getId()} in deleted status detected"
        )

        _ <- IO.sleep(1 minute) // sleep for a minute before cleaning up workspace
      } yield ()
    res.unsafeRunSync()
  }
}
