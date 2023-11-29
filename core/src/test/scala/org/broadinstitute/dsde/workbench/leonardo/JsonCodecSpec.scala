package org.broadinstitute.dsde.workbench.leonardo

import JsonCodec._
import io.circe.CursorOp.DownField
import io.circe.{DecodingFailure, Json}
import io.circe.parser._
import io.circe.syntax.EncoderOps
import org.broadinstitute.dsde.workbench.google2.{MachineTypeName, RegionName, ZoneName}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

class JsonCodecSpec extends LeonardoTestSuite with Matchers with AnyFlatSpecLike {
  "JsonCodec" should "decode DataprocConfig properly" in {
    val inputString =
      """
        |{
        |   "numberOfWorkers": 10,
        |   "masterMachineType": "n1-standard-8",
        |   "masterDiskSize": 500,
        |   "numberOfPreemptibleWorkers": -1,
        |   "cloudService": "DATAPROC",
        |   "region": "us-west2",
        |   "componentGatewayEnabled": true,
        |   "workerPrivateAccess": true
        |}
        |""".stripMargin

    val res = decode[RuntimeConfig](inputString)
    val expected = RuntimeConfig.DataprocConfig(
      10,
      MachineTypeName("n1-standard-8"),
      DiskSize(500),
      None,
      None,
      None,
      Some(-1),
      Map.empty,
      RegionName("us-west2"),
      true,
      true
    )
    res shouldBe (Right(expected))
  }

  it should "decode GCEConfig without GPU properly" in {
    val inputString =
      """
        |{
        |   "machineType": "n1-standard-8",
        |   "diskSize": 500,
        |   "cloudService": "GCE",
        |   "zone": "us-west2-b"
        |}
        |""".stripMargin

    val res = decode[RuntimeConfig](inputString)
    val expected = RuntimeConfig.GceConfig(
      MachineTypeName("n1-standard-8"),
      DiskSize(500),
      None,
      ZoneName("us-west2-b"),
      None
    )
    res shouldBe (Right(expected))
  }

  it should "decode GCEConfig with GPU properly" in {
    val inputString =
      """
        |{
        |   "machineType": "n1-standard-8",
        |   "diskSize": 500,
        |   "cloudService": "GCE",
        |   "zone": "us-west2-b",
        |   "gpuConfig": {
        |     "gpuType": "nvidia-tesla-t4",
        |     "numOfGpus": 2
        |   }
        |}
        |""".stripMargin

    val res = decode[RuntimeConfig](inputString)
    val expected = RuntimeConfig.GceConfig(
      MachineTypeName("n1-standard-8"),
      DiskSize(500),
      None,
      ZoneName("us-west2-b"),
      Some(GpuConfig(GpuType.NvidiaTeslaT4, 2))
    )
    res shouldBe (Right(expected))
  }

  it should "decode GceWithPdConfig properly" in {
    // Since persistentDisk is optional (when disk is dettached, it can be None), hence if `diskSize` is not specified, we try to decode it to GceWithPdConfig
    val inputString =
      """
        |{
        |   "machineType": "n1-standard-8",
        |   "cloudService": "GCE",
        |   "bootDiskSize": 50,
        |   "zone": "us-west2-b"
        |}
        |""".stripMargin

    val res = decode[RuntimeConfig](inputString)
    val expected = RuntimeConfig.GceWithPdConfig(
      MachineTypeName("n1-standard-8"),
      None,
      DiskSize(50),
      ZoneName("us-west2-b"),
      None
    )
    res shouldBe (Right(expected))
  }

  it should "fail with minimumDiskSizeDecodingFailure when GCE diskSize is less than 5" in {
    val inputString =
      """
        |{
        |   "machineType": "n1-standard-8",
        |   "diskSize": 3,
        |   "cloudService": "GCE"
        |}
        |""".stripMargin

    val res = decode[RuntimeConfig.GceConfig](inputString)
    res shouldBe Left(DecodingFailure("Minimum required disk size is 5GB", List(DownField("diskSize"))))
  }

  it should "fail with minimumDiskSizeDecodingFailure when decoding a disk size of 3" in {
    val res = decode[DiskSize]("3")
    res shouldBe Left(DecodingFailure("Minimum required disk size is 5GB", List()))
  }

  it should "decode RuntimeConfig properly" in {
    val inputString =
      """
        |{
        |   "cloudService": "gce",
        |   "machineType": "n1-standard-8",
        |   "persistentDiskId": 50,
        |   "bootDiskSize": 50,
        |   "zone": "us-west2-b",
        |   "gpuConfig": {
        |     "gpuType": "nvidia-tesla-t4",
        |     "numOfGpus": 2
        |   }
        |}
        |""".stripMargin

    val res = decode[RuntimeConfig](inputString)
    res shouldBe Right(
      RuntimeConfig
        .GceWithPdConfig(MachineTypeName("n1-standard-8"),
                         Some(DiskId(50)),
                         DiskSize(50),
                         ZoneName("us-west2-b"),
                         Some(GpuConfig(GpuType.NvidiaTeslaT4, 2))
        )
    )
  }

  it should "fail decoding if diskName has upper case" in {
    val diskNameJson = Json.fromString("PersistentDisk")
    val res = diskNameDecoder.decodeJson(diskNameJson)
    res shouldBe Left(
      DecodingFailure(
        "Invalid name PersistentDisk. Only lowercase alphanumeric characters, numbers and dashes are allowed in leo names",
        List()
      )
    )
  }

  it should "decode duration" in {
    durationDecoder.decodeJson("""15 minutes""".asJson) shouldBe Right(Duration(15, TimeUnit.MINUTES))
    durationDecoder.decodeJson("""15m""".asJson) shouldBe Right(Duration(15, TimeUnit.MINUTES))
    durationDecoder.decodeJson("""15d""".asJson) shouldBe Right(Duration(15, TimeUnit.DAYS))
    durationDecoder.decodeJson("""Inf""".asJson) shouldBe Right(Duration.Inf)
  }
}
