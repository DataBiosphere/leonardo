package org.broadinstitute.dsde.workbench.leonardo.http.api

import io.circe.parser.decode
import org.broadinstitute.dsde.workbench.google2.DiskName
import org.broadinstitute.dsde.workbench.leonardo.http.api.AppV2Routes.createAppDecoder
import org.broadinstitute.dsde.workbench.leonardo.http.{CreateAppRequest, PersistentDiskRequest}
import org.broadinstitute.dsde.workbench.leonardo.{AllowedChartName, AppType, LeonardoTestSuite, WorkspaceId}
import org.scalatest.flatspec.AnyFlatSpec

import java.util.UUID

class AppV2RouteSpec extends AnyFlatSpec with LeonardoTestSuite {
  it should "decode createApp request properly" in {
    val workspaceId = WorkspaceId(UUID.randomUUID())
    val jsonString =
      s"""
         |{
         |  "diskConfig": {
         |    "name": "disk1"
         |  },
         |  "appType": "ALLOWED",
         |  "allowedChartName": "rstudio",
         |  "workspaceId": "${workspaceId.value.toString}"
         |}
         |""".stripMargin

    val res = decode[CreateAppRequest](jsonString)

    val expected = CreateAppRequest(
      None,
      AppType.Allowed,
      Some(AllowedChartName.RStudio),
      None,
      Some(PersistentDiskRequest(DiskName("disk1"), None, None, Map.empty)),
      Map.empty,
      Map.empty,
      None,
      List.empty,
      Some(workspaceId),
      None,
      None,
      None,
      None
    )
    res shouldBe (Right(expected))
  }
}
