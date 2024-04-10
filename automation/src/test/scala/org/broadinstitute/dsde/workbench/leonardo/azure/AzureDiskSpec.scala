package org.broadinstitute.dsde.workbench.leonardo.azure

import org.scalatest.prop.TableDrivenPropertyChecks
import org.broadinstitute.dsde.workbench.google2.streamUntilDoneOrTimeout
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.GeneratedLeonardoClient
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.client.leonardo.model.{
  AzureDiskConfig,
  ClusterStatus,
  CreateAzureRuntimeRequest,
  DiskStatus,
  GetRuntimeResponse
}
import org.broadinstitute.dsde.workbench.leonardo.LeonardoTestTags.ExcludeFromJenkins
import org.broadinstitute.dsde.workbench.leonardo.SSH.SSHRuntimeInfo
import org.broadinstitute.dsde.workbench.leonardo.TestUser.Hermione
import org.scalatest.{DoNotDiscover, ParallelTestExecution, Retries}
import org.broadinstitute.dsde.workbench.service.test.CleanUp
import org.broadinstitute.dsde.workbench.leonardo.{
  AzureBilling,
  CloudProvider,
  LeonardoConfig,
  LeonardoTestUtils,
  RuntimeName,
  SSH
}

import scala.concurrent.duration._

@DoNotDiscover
class AzureDiskSpec
    extends AzureBilling
    with LeonardoTestUtils
    with ParallelTestExecution
    with TableDrivenPropertyChecks
    with Retries
    with CleanUp {

  "create a disk, keep it on runtime delete, and then attach it to a new runtime" taggedAs ExcludeFromJenkins in {
    workspaceDetails =>
      implicit val accessToken: IO[AuthToken] = Hermione.authToken()

      val workspaceId = workspaceDetails.workspace.workspaceId

      val labelMap: java.util.HashMap[String, String] = new java.util.HashMap[String, String]()
      labelMap.put("automation", "true")

      val runtimeName = randomClusterName
      val runtimeName2 = randomClusterName
      val diskName = generateAzureDiskName()
      val res =
        for {
          _ <- loggerIO.info(s"AzureDiskSpec: About to create runtime")
          runtimeClient <- GeneratedLeonardoClient.generateRuntimesApi
          diskClient <- GeneratedLeonardoClient.generateDisksApi

          createReq = new CreateAzureRuntimeRequest()
            .labels(labelMap)
            .machineSize("Standard_DS1_v2")
            .disk(
              new AzureDiskConfig()
                .name(diskName)
                .size(50)
                .labels(labelMap)
            )

          _ <- IO(runtimeClient.createAzureRuntime(workspaceId, runtimeName.asString, false, createReq))
          _ <- loggerIO.info(s"AzureDiskSpec: Create runtime request submitted. Starting to poll GET")

          // Verify the initial getRuntime call
          callGetRuntime = IO(runtimeClient.getAzureRuntime(workspaceId, runtimeName.asString))

          intitialGetRuntimeResponse <- callGetRuntime
          _ <- loggerIO.info(s"initial get runtime response ${intitialGetRuntimeResponse}")
          _ = intitialGetRuntimeResponse.getStatus shouldBe ClusterStatus.CREATING

          _ <- loggerIO.info(
            s"AzureDiskSpec: runtime ${workspaceId}/${runtimeName.asString} in creating status detected"
          )

          _ <- loggerIO.info("AzureDiskSpec: verifying get disk response")
          diskId = intitialGetRuntimeResponse.getRuntimeConfig.getAzureConfig.getPersistentDiskId
          getDisk = IO(diskClient.getDiskV2(diskId.toBigInteger.intValue()))
          diskDuringRuntimeCreate <- getDisk
          _ = diskDuringRuntimeCreate.getStatus shouldBe DiskStatus.CREATING

          _ <- loggerIO.info(
            s"AzureDiskSpec: disk ${workspaceId}/${diskDuringRuntimeCreate.getId()} in creating status detected"
          )

          // Verify the runtime eventually becomes Running (in 25 minutes)
          // will reduce to 20 once https://broadworkbench.atlassian.net/browse/WOR-1397 is merged
          monitorCreateResult <- streamUntilDoneOrTimeout(
            callGetRuntime,
            150,
            10 seconds,
            s"AzureDiskSpec: runtime ${workspaceId}/${runtimeName.asString} did not finish creating after 25 minutes"
          )(implicitly, GeneratedLeonardoClient.runtimeInStateOrError(ClusterStatus.RUNNING))

          _ <- loggerIO.info(
            s"AzureDiskSpec: runtime ${workspaceId}/${runtimeName.asString} create monitor result: $monitorCreateResult"
          )
          _ = monitorCreateResult.getStatus() shouldBe ClusterStatus.RUNNING

          _ <- loggerIO.info("SSHing into first vm to add a file to the disk")
          (output1, output2) <- SSH.startAzureBastionTunnel(RuntimeName(monitorCreateResult.getRuntimeName())).use {
            t =>
              for {
                _ <- loggerIO.info("executing first command to create file for first runtime")
                output1 <- SSH.startSessionAndExecuteCommand(
                  t.hostName,
                  t.port,
                  s"echo ${LeonardoConfig.Azure.vmPassword} | sudo -S bash -c \"echo '{}' > /home/jupyter/persistent_disk/test_disk.ipynb\"",
                  SSHRuntimeInfo(None, CloudProvider.Azure)
                )
                _ <- loggerIO.info("executing second command to get file contents for first runtime")
                output2 <- SSH.startSessionAndExecuteCommand(t.hostName,
                                                             t.port,
                                                             s"cat /home/jupyter/persistent_disk/test_disk.ipynb",
                                                             SSHRuntimeInfo(None, CloudProvider.Azure)
                )
              } yield (output1, output2)
          }

          _ <- loggerIO.info(s"command result 1 and 2: \n\t1: ${output1}, \n\t2: ${output2}")
          _ = output2.outputLines.mkString shouldBe "{}"

          _ <- loggerIO.info(
            s"AzureDiskSpec: runtime ${workspaceId}/${runtimeName.asString} delete starting"
          )

          // Delete the runtime but not the disk
          _ <- IO(runtimeClient.deleteAzureRuntime(workspaceId, runtimeName.asString, false))

          _ <- loggerIO.info(
            s"AzureDiskSpecAzureDiskSpec: runtime ${workspaceId}/${runtimeName.asString} delete called"
          )

          // Wait until disk is unattached
          monitorGetRuntimeUntilUnattached <- streamUntilDoneOrTimeout(
            callGetRuntime,
            240,
            10 seconds,
            s"AzureDiskSpec: disk ${workspaceId}/${diskName} was not ready after 40 minutes"
          )(implicitly, (op: GetRuntimeResponse) => op.getRuntimeConfig.getAzureConfig.getPersistentDiskId === null)

          _ <- loggerIO.info(
            s"AzureDiskSpec: runtime ${workspaceId}/${runtimeName} between runtime monitor result: $monitorGetRuntimeUntilUnattached"
          )
          monitorGetDisk <- getDisk
          _ = monitorGetDisk.getStatus shouldBe DiskStatus.READY
          _ = monitorGetDisk.getName shouldBe diskName

          _ <- loggerIO.info(
            s"AzureRuntimeSpec: disk ${workspaceId}/${monitorGetDisk.getId()} in ready status detected"
          )

          _ <- loggerIO.info(s"AzureDiskSpec: About to create runtime and re-attached disk")

          createReq2 = new CreateAzureRuntimeRequest()
            .labels(labelMap)
            .machineSize("Standard_DS1_v2")
            .disk(
              new AzureDiskConfig()
                .name(diskName)
                .size(50)
                .labels(labelMap)
            )

          _ <- loggerIO.info(s"printing createRuntimeReq: ${createReq2.toJson}")

          _ <- IO(runtimeClient.createAzureRuntime(workspaceId, runtimeName2.asString, true, createReq2))
          _ <- loggerIO.info(s"AzureDiskSpec: Create runtime2 request submitted. Starting to poll GET")

          // Verify the initial getRuntime call
          _ <- IO.sleep(5 seconds)
          callGetRuntime2 = IO(runtimeClient.getAzureRuntime(workspaceId, runtimeName2.asString))

          intitialGetRuntimeResponse2 <- callGetRuntime2
          _ <- loggerIO.info(s"initial get runtime response for runtime2 ${intitialGetRuntimeResponse2}")
          _ = intitialGetRuntimeResponse2.getStatus shouldBe ClusterStatus.CREATING

          _ <- loggerIO.info(
            s"AzureDiskSpec: runtime2 ${workspaceId}/${runtimeName2.asString} in creating status detected"
          )

          _ <- loggerIO.info("AzureDiskSpec: verifying get disk2 response")
          diskId2 = intitialGetRuntimeResponse2.getRuntimeConfig.getAzureConfig.getPersistentDiskId
          getDisk2 = IO(diskClient.getDiskV2(diskId2.toBigInteger.intValue()))
          diskDuringRuntimeCreate2 <- getDisk2
          _ = diskDuringRuntimeCreate2.getStatus shouldBe DiskStatus.READY
          _ = diskDuringRuntimeCreate2.getName shouldBe diskName

          _ <- loggerIO.info(
            s"AzureDiskSpec: disk2 ${workspaceId}/${diskDuringRuntimeCreate2.getId()} in creating status detected"
          )

          // Verify runtime 2 eventually becomes Running (in 25 minutes)
          // will reduce to 20 once https://broadworkbench.atlassian.net/browse/WOR-1397 is merged
          monitorCreateResult2 <- streamUntilDoneOrTimeout(
            callGetRuntime2,
            150,
            10 seconds,
            s"AzureDiskSpec: runtime2 ${workspaceId}/${runtimeName.asString} did not finish creating after 25 minutes"
          )(implicitly, GeneratedLeonardoClient.runtimeInStateOrError(ClusterStatus.RUNNING))

          _ <- loggerIO.info(
            s"AzureDiskSpec: runtime2 ${workspaceId}/${runtimeName.asString} create monitor result: $monitorCreateResult2"
          )
          _ = monitorCreateResult2.getStatus() shouldBe ClusterStatus.RUNNING

          disk2 <- getDisk2
          _ = disk2.getStatus() shouldBe DiskStatus.READY
          _ = disk2.getId() shouldBe monitorGetDisk.getId()

          // TODO: re-enable once ssh issues are resolved: https://broadworkbench.atlassian.net/browse/IA-4889
//          _ <- loggerIO.info("SSHing into second vm to verify disk contents")
//          output <- SSH.startAzureBastionTunnel(RuntimeName(monitorCreateResult2.getRuntimeName())).use { t =>
//            for {
//              output <- SSH.startSessionAndExecuteCommand(t.hostName,
//                                                          t.port,
//                                                          s"cat /home/jupyter/persistent_disk/test_disk.ipynb",
//                                                          SSHRuntimeInfo(None, CloudProvider.Azure)
//              )
//            } yield output
//          }
//
//          _ = output.outputLines.mkString shouldBe "{}"
        } yield ()
      res.unsafeRunSync()
  }
}
