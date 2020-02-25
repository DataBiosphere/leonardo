package org.broadinstitute.dsde.workbench.leonardo

import org.scalatest.{FlatSpecLike, Matchers}
import JsonCodec._
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
      "n1-standard-8",
      500,
      None,
      None,
      None,
      Some(-1)
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
      500
    )
    res shouldBe (Right(expected))
  }
}
