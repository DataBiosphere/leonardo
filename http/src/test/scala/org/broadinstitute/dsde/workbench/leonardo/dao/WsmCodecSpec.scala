package org.broadinstitute.dsde.workbench.leonardo.dao

import java.util.UUID

import com.azure.core.management.Region
import org.broadinstitute.dsde.workbench.leonardo.{CidrIP, DiskSize, RuntimeName, WsmControlledResourceId}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec
import _root_.io.circe.syntax._
import io.circe.parser._
import WsmDecoders._
import WsmEncoders._
import com.azure.resourcemanager.compute.models.VirtualMachineSizeTypes

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
        |    "networkName" : "network",
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
      )
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
      WsmVm(WsmControlledResourceId(fixedUUID)),
      WsmJobReport(
        WsmJobId(fixedUUID),
        "desc",
        WsmJobStatus.Succeeded,
        200,
        "submittedTimestamp",
        "completedTimestamp",
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
         |  "azureVm": {
         |    "resourceId": "${fixedUUID.toString}",
         |    "fillerFieldsThatAreNotDecoded": "filler"
         |  },
         |  "jobReport": {
         |    "id": "${fixedUUID.toString}",
         |    "description": "desc",
         |    "status": "SUCCEEDED",
         |    "statusCode": 200,
         |    "submitted": "submittedTimestamp",
         |    "completed": "completedTimestamp",
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
    val expected = DeleteVmResult(
      WsmJobReport(
        WsmJobId(fixedUUID),
        "desc",
        WsmJobStatus.Succeeded,
        200,
        "submittedTimestamp",
        "completedTimestamp",
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
         |    "submitted": "submittedTimestamp",
         |    "completed": "completedTimestamp",
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
