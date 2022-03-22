package org.broadinstitute.dsde.workbench.leonardo
package dao

import java.util.UUID
import com.azure.core.management.Region
import org.broadinstitute.dsde.workbench.leonardo.{
  AzureDiskName,
  CidrIP,
  DiskSize,
  RuntimeName,
  WsmControlledResourceId
}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec
import _root_.io.circe.syntax._
import io.circe.parser._
import WsmDecoders._
import WsmEncoders._
import com.azure.resourcemanager.compute.models.VirtualMachineSizeTypes

import java.time.ZonedDateTime

class WsmCodecSpec extends AnyFlatSpec with Matchers {
  it should "encode CreateIpRequest" in {
    val req = CreateIpRequest(
      workspaceId,
      testCommonControlledResourceFields,
      CreateIpRequestData(
        AzureIpName("ip"),
        Region.US_EAST
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
        |      "privateResourceIamRoles" : [
        |        "EDITOR"
        |      ]
        |    }
        |  },
        |  "azureIp" : {
        |    "name" : "ip",
        |    "region" : "eastus"
        |  }
        |}
        |""".stripMargin.replaceAll("\\s", "")
  }

  it should "encode CreateNetworkRequest" in {
    val req = CreateNetworkRequest(
      workspaceId,
      testCommonControlledResourceFields,
      CreateNetworkRequestData(
        AzureNetworkName("network"),
        AzureSubnetName("subnet"),
        CidrIP("0.0.0.0/16"),
        CidrIP("0.0.0.0/24"),
        Region.US_EAST
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
        |      "privateResourceIamRoles" : [
        |        "EDITOR"
        |      ]
        |    }
        |  },
        |  "azureNetwork" : {
        |    "name" : "network",
        |    "subnetName": "subnet",
        |    "addressSpaceCidr": "0.0.0.0/16",
        |    "subnetAddressCidr": "0.0.0.0/24",
        |    "region" : "eastus"
        |  }
        |}
        |""".stripMargin.replaceAll("\\s", "")
  }

  it should "encode CreateDiskRequest" in {
    val req = CreateDiskRequest(
      workspaceId,
      testCommonControlledResourceFields,
      CreateDiskRequestData(
        AzureDiskName("disk"),
        DiskSize(50),
        Region.US_EAST
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
        |      "privateResourceIamRoles" : [
        |        "EDITOR"
        |      ]
        |    }
        |  },
        |  "azureDisk" : {
        |    "name" : "disk",
        |    "size": 50,
        |    "region" : "eastus"
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
        Region.US_EAST,
        VirtualMachineSizeTypes.STANDARD_A2, //Standard_A2
        azureImage,
        WsmControlledResourceId(fixedUUID),
        WsmControlledResourceId(fixedUUID),
        WsmControlledResourceId(fixedUUID)
      ),
      WsmJobControl(WsmJobId(fixedUUID))
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
         |      "privateResourceIamRoles" : [
         |        "EDITOR"
         |      ]
         |    }
         |  },
         |  "azureVm" : {
         |    "name" : "runtime",
         |    "region" : "eastus",
         |    "vmSize": "Standard_A2",
         |    "vmImageUri": "${azureImage.imageUrl}",
         |    "ipId": "${fixedUUID.toString}",
         |    "diskId": "${fixedUUID.toString}",
         |    "networkId": "${fixedUUID.toString}"
         |  },
         |  "jobControl": {
         |    "id": "${fixedUUID.toString}"
         |  }
         |}
         |""".stripMargin.replaceAll("\\s", "")
  }

  it should "encode DeleteVmRequest" in {
    val fixedUUID = UUID.randomUUID()
    val req = DeleteControlledAzureResourceRequest(WsmJobControl(WsmJobId(fixedUUID)))

    req.asJson.deepDropNullValues.noSpaces shouldBe
      s"""
         |{
         |  "jobControl": {
         |    "id": "${fixedUUID.toString}"
         |  }
         |}
         |""".stripMargin.replaceAll("\\s", "")

  }

  it should "decode CreateIpResponse" in {
    val fixedUUID = UUID.randomUUID()
    val expected = CreateIpResponse(WsmControlledResourceId(fixedUUID))

    val decodedResp = decode[CreateIpResponse](
      s"""
         |{
         |  "resourceId": "${fixedUUID.toString}",
         |  "azureIp": {
         |    "fillerFieldsThatAreNotDecoded": "filler"
         |  }
         |}
         |""".stripMargin.replaceAll("\\s", "")
    )

    decodedResp shouldBe Right(expected)
  }

  it should "decode CreateNetworkResponse" in {
    val fixedUUID = UUID.randomUUID()
    val expected = CreateNetworkResponse(WsmControlledResourceId(fixedUUID))

    val decodedResp = decode[CreateNetworkResponse](
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
    val fixedUUID = UUID.randomUUID()
    val expected = CreateVmResult(
      WsmJobReport(
        WsmJobId(fixedUUID),
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
         |    "id": "${fixedUUID.toString}",
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
         |        "id": "1bf4d89f-53ac-4ad4-ab8e-0131c6494a69",
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
        WsmJobId(UUID.fromString("1bf4d89f-53ac-4ad4-ab8e-0131c6494a69")),
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
    val fixedUUID = UUID.randomUUID()
    val expected = GetCreateVmJobResult(
      Some(
        WsmVm(
          WsmVMMetadata(WsmControlledResourceId(UUID.fromString("dcfa6fa4-ab46-465e-a8dd-76705cbdb4ec")))
        )
      ),
      WsmJobReport(
        WsmJobId(fixedUUID),
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
         |    "id": "${fixedUUID.toString}",
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
    val fixedUUID = UUID.randomUUID()
    val now = ZonedDateTime.now()
    val expected = DeleteVmResult(
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

    val decodedResp = decode[DeleteVmResult](
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
