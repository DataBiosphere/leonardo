package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import io.circe.parser.decode
import org.broadinstitute.dsde.workbench.google2.{DiskName, MachineTypeName}
import org.broadinstitute.dsde.workbench.leonardo.http.api.RuntimeRoutes._
import org.scalatest.{FlatSpec, Matchers}

class RuntimeRoutesSpec extends FlatSpec with Matchers with LeonardoTestSuite {
  it should "decode RuntimeConfigRequest.GceWithPdConfig correctly" in {
    val jsonString =
      """
        |{
        |  "cloudService": "gce",
        |  "persistentDisk": {
        |    "name": "qi-disk-c1",
        |    "size": 200
        |  }
        |}
        |""".stripMargin
    val expectedResult = RuntimeConfigRequest.GceWithPdConfig(
      None,
      PersistentDiskRequest(DiskName("qi-disk-c1"), Some(DiskSize(200)), None, None, Map.empty)
    )
    decode[RuntimeConfigRequest](jsonString) shouldBe Right(expectedResult)
  }

  it should "decode RuntimeConfigRequest.GceConfig correctly" in {
    val jsonString =
      """
        |{
        |  "cloudService": "gce"
        |}
        |""".stripMargin
    val expectedResult = RuntimeConfigRequest.GceConfig(
      None,
      None
    )
    decode[RuntimeConfigRequest](jsonString) shouldBe Right(expectedResult)
  }

  it should "decode RuntimeConfigRequest correctly" in {
    val jsonString =
      """
        |{
        |  "cloudService": "gce",
        |  "persistentDisk": {
        |    "name": "qi-disk-c1",
        |    "size": 30
        |  }
        |}
        |""".stripMargin
    val expectedResult = RuntimeConfigRequest.GceWithPdConfig(
      None,
      PersistentDiskRequest(DiskName("qi-disk-c1"), Some(DiskSize(30)), None, None, Map.empty)
    )
    decode[RuntimeConfigRequest](jsonString) shouldBe Right(expectedResult)
  }

  it should "decode empty CreateRuntime2Request correctly" in {
    val jsonString =
      """
        |{}
        |""".stripMargin
    val expectedResult = CreateRuntime2Request(
      Map.empty,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      Set.empty,
      Map.empty
    )
    decode[CreateRuntime2Request](jsonString) shouldBe Right(expectedResult)
  }

  it should "decode CreateRuntime2Request correctly" in {
    val jsonString =
      """
        |{
        |  "runtimeConfig": {
        |    "cloudService": "gce",
        |    "machineType": "n1-standard-4",
        |    "diskSize": 100
        |  }
        |}
        |""".stripMargin
    val expectedResult = CreateRuntime2Request(
      Map.empty,
      None,
      None,
      Some(
        RuntimeConfigRequest.GceConfig(
          Some(MachineTypeName("n1-standard-4")),
          Some(DiskSize(100))
        )
      ),
      None,
      None,
      None,
      None,
      None,
      None,
      Set.empty,
      Map.empty
    )
    decode[CreateRuntime2Request](jsonString) shouldBe Right(expectedResult)
  }
}
