package org.broadinstitute.dsde.workbench.leonardo.azure

import org.scalatest.prop.TableDrivenPropertyChecks
import org.broadinstitute.dsde.workbench.google2.streamUntilDoneOrTimeout
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.GeneratedLeonardoClient
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.client.leonardo.model.{AzureDiskConfig, ClusterStatus, CreateAzureRuntimeRequest}
import org.broadinstitute.dsde.workbench.leonardo.LeonardoTestTags.ExcludeFromJenkins
import org.broadinstitute.dsde.workbench.leonardo.TestUser.Hermione
import org.scalatest.{DoNotDiscover, ParallelTestExecution, Retries}
import org.broadinstitute.dsde.workbench.service.test.CleanUp
import org.broadinstitute.dsde.workbench.leonardo.{AzureBillingBeforeAndAfter, LeonardoTestUtils}

import scala.concurrent.duration._

@DoNotDiscover
class AzureAutopauseSpec
    extends AzureBillingBeforeAndAfter
    with LeonardoTestUtils
    with ParallelTestExecution
    with TableDrivenPropertyChecks
    with Retries
    with CleanUp {

  implicit val accessToken: IO[AuthToken] = Hermione.authToken()

  "azure runtime autopauses" taggedAs ExcludeFromJenkins in { workspaceDetails =>
    val workspaceId = workspaceDetails.workspace.workspaceId

    val labelMap: java.util.HashMap[String, String] = new java.util.HashMap[String, String]()
    labelMap.put("automation", "true")

    val runtimeName = randomClusterName
    val res =
      for {
        _ <- loggerIO.info(s"AzureAutoPauseSpec: About to create runtime")
        runtimeClient <- GeneratedLeonardoClient.generateRuntimesApi

        // autopause set to 15 minutes to make sure metrics monitor call is ignored (doesn't reset the clock)
        createReq = new CreateAzureRuntimeRequest()
          .labels(labelMap)
          .machineSize("Standard_DS1_v2")
          .disk(
            new AzureDiskConfig()
              .name(generateAzureDiskName())
              .size(50)
              .labels(labelMap)
          )
          .autoPauseThreshold(15)

        _ <- IO(runtimeClient.createAzureRuntime(workspaceId, runtimeName.asString, false, createReq))
        _ <- loggerIO.info(s"AzureAutoPauseSpec: Create runtime request submitted. Starting to poll GET")

        // ------------------ Waiting for Create ------------------ //

        // Verify the initial getRuntime call
        callGetRuntime = IO(runtimeClient.getAzureRuntime(workspaceId, runtimeName.asString))

        initialGetRuntimeResponse <- callGetRuntime
        _ <- loggerIO.info(s"initial get runtime response $initialGetRuntimeResponse")
        _ = initialGetRuntimeResponse.getStatus shouldBe ClusterStatus.CREATING

        _ <- loggerIO.info(
          s"AzureAutoPauseSpec: runtime $workspaceId/${runtimeName.asString} in creating status detected"
        )

        // Verify the runtime eventually becomes Running (in 20 minutes)
        monitorCreateResult <- streamUntilDoneOrTimeout(
          callGetRuntime,
          120,
          10 seconds,
          s"AzureAutoPauseSpec: runtime $workspaceId/${runtimeName.asString} did not finish creating after 20 minutes"
        )(implicitly, GeneratedLeonardoClient.runtimeInStateOrError(ClusterStatus.RUNNING))

        _ <- loggerIO.info(
          s"AzureAutoPauseSpec: runtime $workspaceId/${runtimeName.asString} create monitor result: $monitorCreateResult"
        )
        _ = monitorCreateResult.getStatus() shouldBe ClusterStatus.RUNNING

        // ------------------ Waiting for Autopause ------------------ //

        // new IO for new stream
        callGetRuntimeStopping = IO(runtimeClient.getAzureRuntime(workspaceId, runtimeName.asString))

        _ <- loggerIO.info(
          s"AzureAutoPauseSpec: runtime $workspaceId/${runtimeName.asString} waiting to autopause"
        )

        monitorAutoPauseResult <- streamUntilDoneOrTimeout(
          callGetRuntimeStopping,
          180,
          10 seconds,
          s"AzureAutoPauseSpec: runtime $workspaceId/${runtimeName.asString} did not transition to stopping after 30 minutes"
        )(implicitly, GeneratedLeonardoClient.runtimeInStateOrError(ClusterStatus.STOPPING))

        _ <- loggerIO.info(
          s"AzureAutoPauseSpec: runtime $workspaceId/${runtimeName.asString} auto-pause monitor result: $monitorAutoPauseResult"
        )
        _ = monitorAutoPauseResult.getStatus() shouldBe ClusterStatus.STOPPING

        _ <- loggerIO.info(
          s"AzureAutoPauseSpec: runtime $workspaceId/${runtimeName.asString} waiting for runtime to stop"
        )

        // new IO for new stream
        callGetRuntimeStopped = IO(runtimeClient.getAzureRuntime(workspaceId, runtimeName.asString))

        monitorStoppingResult <- streamUntilDoneOrTimeout(
          callGetRuntimeStopped,
          30,
          10 seconds,
          s"AzureAutoPauseSpec: runtime $workspaceId/${runtimeName.asString} did not transition to stopped after 5 minutes"
        )(implicitly, GeneratedLeonardoClient.runtimeInStateOrError(ClusterStatus.STOPPED))

        _ <- loggerIO.info(
          s"AzureAutoPauseSpec: runtime $workspaceId/${runtimeName.asString} stopped monitor result: $monitorStoppingResult"
        )
        _ = monitorStoppingResult.getStatus() shouldBe ClusterStatus.STOPPED

        _ <- IO.sleep(1 minute) // sleep for a minute before cleaning up workspace
      } yield ()
    res.unsafeRunSync()
  }
}
