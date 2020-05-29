package org.broadinstitute.dsde.workbench.leonardo
package api

import io.circe.parser.decode
import org.broadinstitute.dsde.workbench.google2.DiskName
import org.broadinstitute.dsde.workbench.leonardo.http.api.RuntimeRoutes._
import org.broadinstitute.dsde.workbench.leonardo.http.service.{PersistentDiskRequest, RuntimeConfigRequest}
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
}
