package org.broadinstitute.dsde.workbench.leonardo

import org.scalatest.{FlatSpecLike, Matchers}
import JsonCodec._
import io.circe.CursorOp.DownField
import io.circe.DecodingFailure
import io.circe.parser._
import org.broadinstitute.dsde.workbench.google2.MachineTypeName

class JsonCodecSpec extends LeonardoTestSuite with Matchers with FlatSpecLike {
  "JsonCodec" should "decode DataprocConfig properly" in {
    val inputString =
      """
        |{
        |   "numberOfWorkers": 10,
        |   "masterMachineType": "n1-standard-8",
        |   "masterDiskSize": 500,
        |   "numberOfPreemptibleWorkers": -1,
        |   "cloudService": "DATAPROC"
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
      Map.empty
    )
    res shouldBe (Right(expected))
  }

  it should "decode GCEConfig properly" in {
    val inputString =
      """
        |{
        |   "machineType": "n1-standard-8",
        |   "diskSize": 500,
        |   "cloudService": "GCE"
        |}
        |""".stripMargin

    val res = decode[RuntimeConfig](inputString)
    val expected = RuntimeConfig.GceConfig(
      MachineTypeName("n1-standard-8"),
      DiskSize(500)
    )
    res shouldBe (Right(expected))
  }

  it should "fail with minimumDiskSizeDecodingFailure when GCE diskSize is less than 50" in {
    val inputString =
      """
        |{
        |   "machineType": "n1-standard-8",
        |   "diskSize": 35,
        |   "cloudService": "GCE"
        |}
        |""".stripMargin

    val res = decode[RuntimeConfig](inputString)
    res.left shouldBe Left(DecodingFailure("Minimum required disk size is 50GB", List(DownField("diskSize"))))
  }

  it should "fail with minimumDiskSizeDecodingFailure when Dataproc diskSize is less than 50" in {
    val inputString =
      """
        |{
        |   "numberOfWorkers": 10,
        |   "masterMachineType": "n1-standard-8",
        |   "masterDiskSize": 49,
        |   "numberOfPreemptibleWorkers": -1,
        |   "cloudService": "DATAPROC"
        |}
        |""".stripMargin

    val res = decode[RuntimeConfig](inputString)
    res shouldBe Left(DecodingFailure("Minimum required disk size is 50GB", List(DownField("masterDiskSize"))))
  }

  it should "fail with minimumDiskSizeDecodingFailure when decoding a disk size of 10" in {
    val res = decode[DiskSize]("10")
    res shouldBe Left(DecodingFailure("Minimum required disk size is 50GB", List()))
  }
}
