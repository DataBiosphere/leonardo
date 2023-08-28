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
import org.broadinstitute.dsde.workbench.leonardo.TestUser.Hermione
import org.broadinstitute.dsde.workbench.leonardo.LeonardoTestTags.ExcludeFromJenkins
import org.broadinstitute.dsde.workbench.leonardo.{AzureBillingBeforeAndAfter, LeonardoTestUtils}
import org.broadinstitute.dsde.workbench.service.test.CleanUp
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{ParallelTestExecution, Retries}

import scala.concurrent.duration._
class AzureRuntimeSpec
    extends AzureBillingBeforeAndAfter
    with LeonardoTestUtils
    with ParallelTestExecution
    with TableDrivenPropertyChecks
    with Retries
    with CleanUp {

  implicit val accessToken: IO[AuthToken] = Hermione.authToken()

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
          s"AzureRuntime: runtime ${workspaceId}/${runtimeName.asString} create monitor result: $monitorCreateResult"
        )
        _ = monitorCreateResult.getStatus() shouldBe ClusterStatus.RUNNING

        _ <- loggerIO.info(
          s"AzureRuntime: runtime ${workspaceId}/${runtimeName.asString} delete starting"
        )
        // Delete the runtime
        _ <- IO(runtimeClient.deleteAzureRuntime(workspaceId, runtimeName.asString, true))

        _ <- loggerIO.info(
          s"AzureRuntime: runtime ${workspaceId}/${runtimeName.asString} delete called, starting to poll on deletion"
        )

        monitorDeleteResult <- streamUntilDoneOrTimeout(
          callGetRuntime,
          240,
          10 seconds,
          s"AzureRuntimeSpec: runtime ${workspaceId}/${runtimeName.asString} did not finish deleting after 40 minutes"
        )(implicitly, GeneratedLeonardoClient.runtimeInStateOrError(ClusterStatus.DELETED))

        _ <- loggerIO.info(
          s"AzureRuntime: runtime ${workspaceId}/${runtimeName.asString} delete monitor result: $monitorDeleteResult"
        )
        _ = monitorDeleteResult.getStatus() shouldBe ClusterStatus.DELETED

        diskAfterRuntimeDelete <- getDisk
        _ = diskAfterRuntimeDelete.getStatus shouldBe DiskStatus.DELETED

        _ <- loggerIO.info(
          s"AzureRuntimeSpec: disk ${workspaceId}/${diskAfterRuntimeDelete.getId()} in deleted status detected"
        )

        _ <- IO.sleep(1 minute) // sleep for a minute before cleaning up workspace
      } yield ()
    res.unsafeRunSync()
  }

}
