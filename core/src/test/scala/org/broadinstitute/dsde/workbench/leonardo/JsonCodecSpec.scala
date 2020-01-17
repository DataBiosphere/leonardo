package org.broadinstitute.dsde.workbench.leonardo

import org.scalatest.{FlatSpecLike, Matchers}
import JsonCodec._

class JsonCodecSpec extends LeonardoTestSuite with Matchers with FlatSpecLike {
  "JsonCodec" should "fail to decode MachineConfig when masterMachineType is empty string" in {
    val inputString =
      """
        |{
        |   "numberOfWorkers": 0,
        |   "masterMachineType": "",
        |   "masterDiskSize": 500
        |}
        |""".stripMargin

    val decodeResult = for {
      json <- io.circe.parser.parse(inputString)
      r <- json.as[MachineConfig]
    } yield r
    decodeResult shouldBe Left(emptyMasterMachineType)
  }

  it should "fail with negativeNumberDecodingFailure when numberOfPreemptibleWorkers is negative" in {
    val inputString =
      """
        |{
        |   "numberOfWorkers": 10,
        |   "masterMachineType": "n1-standard-8",
        |   "masterDiskSize": 500,
        |   "numberOfPreemptibleWorkers": -1
        |}
        |""".stripMargin

    val decodeResult = for {
      json <- io.circe.parser.parse(inputString)
      r <- json.as[MachineConfig]
    } yield r
    decodeResult shouldBe Left(negativeNumberDecodingFailure)
  }

  it should "fail with negativeNumberDecodingFailure when masterDiskSize is negative" in {
    val inputString =
      """
        |{
        |   "numberOfWorkers": 10,
        |   "masterMachineType": "n1-standard-8",
        |   "masterDiskSize": -1
        |}
        |""".stripMargin

    val decodeResult = for {
      json <- io.circe.parser.parse(inputString)
      r <- json.as[MachineConfig]
    } yield r
    decodeResult shouldBe Left(negativeNumberDecodingFailure)
  }

  it should "fail with oneWorkerSpecifiedDecodingFailure when numberOfPreemptibleWorkers is negative" in {
    val inputString =
      """
        |{
        |   "numberOfWorkers": 1,
        |   "masterMachineType": "n1-standard-8",
        |   "masterDiskSize": 500
        |}
        |""".stripMargin

    val decodeResult = for {
      json <- io.circe.parser.parse(inputString)
      r <- json.as[MachineConfig]
    } yield r
    decodeResult shouldBe Left(oneWorkerSpecifiedDecodingFailure)
  }
}
