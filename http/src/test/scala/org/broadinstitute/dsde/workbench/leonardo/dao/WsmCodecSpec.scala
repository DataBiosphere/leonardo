package org.broadinstitute.dsde.workbench.leonardo
package dao

import _root_.io.circe.syntax._
import com.azure.resourcemanager.compute.models.VirtualMachineSizeTypes
import io.circe.parser._
import org.broadinstitute.dsde.workbench.google2.RegionName
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.dao.WsmDecoders._
import org.broadinstitute.dsde.workbench.leonardo.dao.WsmEncoders._
import org.broadinstitute.dsde.workbench.leonardo.http.ConfigReader
import org.broadinstitute.dsde.workbench.leonardo.http.service.VMCredential
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.ZonedDateTime
import java.util.UUID

class WsmCodecSpec extends AnyFlatSpec with Matchers {
  it should "encode CreateDiskRequest" in {
    val req = CreateDiskRequest(
      workspaceId,
      testCommonControlledResourceFields,
      CreateDiskRequestData(
        AzureDiskName("disk"),
        DiskSize(50)
      )
    ).asJson.deepDropNullValues.noSpaces

    req shouldBe
      """
        |{
        |  "common" : {
        |    "name" : "name",
        |    "description" : "desc",
        |    "cloningInstructions" : "COPY_NOTHING",
        |    "accessScope" : "PRIVATE_ACCESS",
        |    "managedBy" : "USER",
        |    "privateResourceUser" : {
        |      "userName" : "user1@example.com",
        |      "privateResourceIamRole" : "EDITOR"
        |    }
        |  },
        |  "azureDisk" : {
        |    "name" : "disk",
        |    "size": 50
        |  }
        |}
        |""".stripMargin.replaceAll("\\s", "")
  }

  it should "encode CreateVmRequest" in {
    val fixedUUID = UUID.randomUUID()

    val req = CreateVmRequest(
      workspaceId,
      testCommonControlledResourceFields,
      CreateVmRequestData(
        RuntimeName("runtime"),
        VirtualMachineSizeTypes.STANDARD_A2, // Standard_A2
        ConfigReader.appConfig.azure.pubsubHandler.runtimeDefaults.image,
        CustomScriptExtension(
          name = ConfigReader.appConfig.azure.pubsubHandler.runtimeDefaults.customScriptExtension.name,
          publisher = ConfigReader.appConfig.azure.pubsubHandler.runtimeDefaults.customScriptExtension.publisher,
          `type` = ConfigReader.appConfig.azure.pubsubHandler.runtimeDefaults.customScriptExtension.`type`,
          version = ConfigReader.appConfig.azure.pubsubHandler.runtimeDefaults.customScriptExtension.version,
          minorVersionAutoUpgrade =
            ConfigReader.appConfig.azure.pubsubHandler.runtimeDefaults.customScriptExtension.minorVersionAutoUpgrade,
          protectedSettings = ProtectedSettings(
            ConfigReader.appConfig.azure.pubsubHandler.runtimeDefaults.customScriptExtension.fileUris,
            ""
          )
        ),
        VMCredential("username", "password"),
        WsmControlledResourceId(fixedUUID)
      ),
      WsmJobControl(WsmJobId("job1"))
    ).asJson.deepDropNullValues.noSpaces

    req shouldBe
      s"""
         |{
         |  "common" : {
         |    "name" : "name",
         |    "description" : "desc",
         |    "cloningInstructions" : "COPY_NOTHING",
         |    "accessScope" : "PRIVATE_ACCESS",
         |    "managedBy" : "USER",
         |    "privateResourceUser" : {
         |      "userName" : "user1@example.com",
         |      "privateResourceIamRole" : "EDITOR"
         |    }
         |  },
         |  "azureVm" : {
         |    "name" : "runtime",
         |    "vmSize": "Standard_A2",
         |    "vmImage": {
         |      "publisher": "microsoft-dsvm",
         |      "offer": "ubuntu-2004",
         |      "sku": "2004-gen2",
         |      "version": "23.04.24"
         |    },
         |    "customScriptExtension": {
         |      "name": "vm-custom-script-extension",
         |      "publisher": "Microsoft.Azure.Extensions",
         |      "type": "CustomScript",
         |      "version": "2.1",
         |      "minorVersionAutoUpgrade": true,
         |      "protectedSettings": [{
         |          "key": "fileUris",
         |          "value": ["https://raw.githubusercontent.com/DataBiosphere/leonardo/1585c14a78b9de6fc900ecffc35a14f96a852584/http/src/main/resources/init-resources/azure_vm_init_script.sh"]
         |        },
         |        {
         |          "key": "commandToExecute",
         |          "value": ""
         |        }
         |      ]
         |    },
         |    "vmUser":{"name":"username","password":"password"},
         |    "diskId": "${fixedUUID.toString}"
         |  },
         |  "jobControl": {
         |    "id": "job1"
         |  }
         |}
         |""".stripMargin.replaceAll("\\s", "")
  }

  it should "encode DeleteVmRequest" in {
    val fixedUUID = UUID.randomUUID().toString
    val req = WsmDaoDeleteControlledAzureResourceRequest(WsmJobControl(WsmJobId(fixedUUID)))

    req.asJson.deepDropNullValues.noSpaces shouldBe
      s"""
         |{
         |  "jobControl": {
         |    "id": "${fixedUUID.toString}"
         |  }
         |}
         |""".stripMargin.replaceAll("\\s", "")

  }

  it should "decode CreateDiskResponse" in {
    val fixedUUID = UUID.randomUUID()
    val expected = CreateDiskResponse(WsmControlledResourceId(fixedUUID))

    val decodedResp = decode[CreateDiskResponse](
      s"""
         |{
         |  "resourceId": "${fixedUUID.toString}",
         |  "azureNetwork": {
         |    "fillerFieldsThatAreNotDecoded": "filler"
         |  }
         |}
         |""".stripMargin.replaceAll("\\s", "")
    )

    decodedResp shouldBe Right(expected)
  }

  it should "decode CreateVmResult" in {
    val jobId = WsmJobId("job1")
    val expected = CreateVmResult(
      WsmJobReport(
        jobId,
        "desc",
        WsmJobStatus.Running,
        200,
        ZonedDateTime.parse("2022-03-18T15:02:29.264756Z"),
        Some(ZonedDateTime.parse("2022-03-18T15:02:29.264756Z")),
        "resultUrl"
      ),
      Some(
        WsmErrorReport(
          "error",
          500,
          List("testCause")
        )
      )
    )

    val decodedResp = decode[CreateVmResult](
      s"""
         |{
         |  "jobReport": {
         |    "id": "${jobId.value}",
         |    "description": "desc",
         |    "status": "RUNNING",
         |    "statusCode": 200,
         |    "submitted": "2022-03-18T15:02:29.264756Z",
         |    "completed": "2022-03-18T15:02:29.264756Z",
         |    "resultURL": "resultUrl"
         |  },
         |  "errorReport": {
         |     "message": "error",
         |     "statusCode": 500,
         |     "causes": ["testCause"]
         |  }
         |}
         |""".stripMargin.replaceAll("\\s", "")
    )

    decodedResp shouldBe Right(expected)

    val decodedResp2 = decode[CreateVmResult](
      s"""
         |{
         |    "jobReport":
         |    {
         |        "id": "job2",
         |        "description": "Create controlled resource CONTROLLED_AZURE_VM; id 635e25e1-c793-4ca9-b9fe-9055cdae2f26; name automation-test-aswsimhjz",
         |        "status": "RUNNING",
         |        "statusCode": 202,
         |        "submitted": "2022-03-18T15:02:29.264756Z",
         |        "resultURL": "https://workspace.dsde-dev.broadinstitute.org/api/workspaces/v1/e1aaf25b-b298-46eb-891b-e4c326f29b0c/resources/controlled/azure/vm/create-result/1bf4d89f-53ac-4ad4-ab8e-0131c6494a69"
         |    }
         |}
         |""".stripMargin
    )

    val expected2 = CreateVmResult(
      WsmJobReport(
        WsmJobId("job2"),
        "Create controlled resource CONTROLLED_AZURE_VM; id 635e25e1-c793-4ca9-b9fe-9055cdae2f26; name automation-test-aswsimhjz",
        WsmJobStatus.Running,
        202,
        ZonedDateTime.parse("2022-03-18T15:02:29.264756Z"),
        None,
        "https://workspace.dsde-dev.broadinstitute.org/api/workspaces/v1/e1aaf25b-b298-46eb-891b-e4c326f29b0c/resources/controlled/azure/vm/create-result/1bf4d89f-53ac-4ad4-ab8e-0131c6494a69"
      ),
      None
    )
    decodedResp2 shouldBe Right(expected2)
  }

  it should "decode getCreateVmResult" in {
    val jobId = WsmJobId("job1")
    val expected = GetCreateVmJobResult(
      Some(
        WsmVm(
          WsmVMMetadata(WsmControlledResourceId(UUID.fromString("dcfa6fa4-ab46-465e-a8dd-76705cbdb4ec"))),
          WsmVMAttributes(RegionName("westcentralus"))
        )
      ),
      WsmJobReport(
        jobId,
        "desc",
        WsmJobStatus.Running,
        200,
        ZonedDateTime.parse("2022-03-18T15:02:29.264756Z"),
        Some(ZonedDateTime.parse("2022-03-18T15:02:29.264756Z")),
        "resultUrl"
      ),
      Some(
        WsmErrorReport(
          "error",
          500,
          List("testCause")
        )
      )
    )

    val decodedResp = decode[GetCreateVmJobResult](
      s"""
         |{
         |   "azureVm": {
         |        "metadata":
         |        {
         |            "workspaceId": "e1aaf25b-b298-46eb-891b-e4c326f29b0c",
         |            "resourceId": "dcfa6fa4-ab46-465e-a8dd-76705cbdb4ec",
         |            "name": "automation-test-afalskknz",
         |            "description": "Azure Vm",
         |            "resourceType": "AZURE_VM",
         |            "stewardshipType": "CONTROLLED",
         |            "cloningInstructions": "COPY_NOTHING",
         |            "controlledResourceMetadata":
         |            {
         |                "accessScope": "PRIVATE_ACCESS",
         |                "managedBy": "APPLICATION",
         |                "privateResourceUser":
         |                {
         |                    "userName": "ron.weasley@test.firecloud.org"
         |                },
         |                "privateResourceState": "ACTIVE"
         |            }
         |        },
         |        "attributes":
         |        {
         |            "vmName": "automation-test-afalskknz",
         |            "region": "westcentralus",
         |            "vmSize": "Standard_D1_v2",
         |            "vmImageUri": "/subscriptions/3efc5bdf-be0e-44e7-b1d7-c08931e3c16c/resourceGroups/mrg-qi-1-preview-20210517084351/providers/Microsoft.Compute/galleries/msdsvm/images/customized_ms_dsvm/versions/0.1.0",
         |            "ipId": "62e6dec2-94c5-4806-8594-eb6020344cbe",
         |            "diskId": "2eddd6aa-bb94-4027-aeca-0de34a583808",
         |            "networkId": "b414a42b-a27d-4072-8a03-44283f4c07f6"
         |        }
         |    },
         |  "jobReport": {
         |    "id": "${jobId.value}",
         |    "description": "desc",
         |    "status": "RUNNING",
         |    "statusCode": 200,
         |    "submitted": "2022-03-18T15:02:29.264756Z",
         |    "completed": "2022-03-18T15:02:29.264756Z",
         |    "resultURL": "resultUrl"
         |  },
         |  "errorReport": {
         |     "message": "error",
         |     "statusCode": 500,
         |     "causes": ["testCause"]
         |  }
         |}
         |""".stripMargin.replaceAll("\\s", "")
    )

    decodedResp shouldBe Right(expected)
  }

  it should "decode DeleteVmResult" in {
    val fixedUUID = UUID.randomUUID().toString
    val now = ZonedDateTime.now()
    val expected = DeleteWsmResourceResult(
      WsmJobReport(
        WsmJobId(fixedUUID),
        "desc",
        WsmJobStatus.Succeeded,
        200,
        now,
        Some(now),
        "resultUrl"
      ),
      Some(
        WsmErrorReport(
          "error",
          500,
          List("testCause")
        )
      )
    )

    val decodedResp = decode[DeleteWsmResourceResult](
      s"""
         |{
         |  "jobReport": {
         |    "id": "${fixedUUID.toString}",
         |    "description": "desc",
         |    "status": "SUCCEEDED",
         |    "statusCode": 200,
         |    "submitted": "${now.toString}",
         |    "completed": "${now.toString}",
         |    "resultURL": "resultUrl"
         |  },
         |  "errorReport": {
         |     "message": "error",
         |     "statusCode": 500,
         |     "causes": ["testCause"]
         |  }
         |}
         |""".stripMargin.replaceAll("\\s", "")
    )

    decodedResp shouldBe Right(expected)
  }
}
