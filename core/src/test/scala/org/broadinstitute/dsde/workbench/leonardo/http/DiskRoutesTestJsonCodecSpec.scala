package org.broadinstitute.dsde.workbench.leonardo
package http

import io.circe.parser._
import org.broadinstitute.dsde.workbench.google2._
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.PersistentDiskSamResourceId
import org.broadinstitute.dsde.workbench.leonardo.http.DiskRoutesTestJsonCodec._
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import java.util.UUID

class DiskRoutesTestJsonCodecSpec extends LeonardoTestSuite with Matchers with AnyFlatSpecLike {
  it should "decode GetPersistentDiskResponse properly" in {
    val inputString =
      """
        |{
        |    "id": 107,
        |    "googleProject": "gpalloc-dev-master-tzprbkr",
        |    "cloudContext": {"cloudResource": "gpalloc-dev-master-tzprbkr", "cloudProvider": "GCP"},
        |    "zone": "us-central1-a",
        |    "name": "rzxybksgvy",
        |    "googleId": "3579418231488887016",
        |    "serviceAccount": "b305pet-114763077412354570085@gpalloc-dev-master-tzprbkr.iam.gserviceaccount.com",
        |    "status": "Ready",
        |    "auditInfo":
        |    {
        |        "creator": "ron.weasley@test.firecloud.org",
        |        "createdDate": "2021-07-01T18:14:27.688698Z",
        |        "destroyedDate": null,
        |        "dateAccessed": "2021-07-01T18:14:30.915Z"
        |    },
        |    "size": 451,
        |    "diskType": "pd-standard",
        |    "blockSize": 4096,
        |    "labels":
        |    {
        |        "diskName": "rzxybksgvy",
        |        "creator": "ron.weasley@test.firecloud.org",
        |        "googleProject": "gpalloc-dev-master-tzprbkr",
        |        "serviceAccount": "b305pet-114763077412354570085@gpalloc-dev-master-tzprbkr.iam.gserviceaccount.com"
        |    },
        |    "workspaceId": "5955382f-c8be-464b-b5b9-2c9260cd2661"
        |}
        |""".stripMargin

    val res = decode[GetPersistentDiskResponse](inputString)
    val expected = GetPersistentDiskResponse(
      DiskId(107),
      CloudContext.Gcp(GoogleProject("gpalloc-dev-master-tzprbkr")),
      ZoneName("us-central1-a"),
      DiskName("rzxybksgvy"),
      WorkbenchEmail("b305pet-114763077412354570085@gpalloc-dev-master-tzprbkr.iam.gserviceaccount.com"),
      PersistentDiskSamResourceId("test"),
      DiskStatus.Ready,
      AuditInfo(
        WorkbenchEmail("ron.weasley@test.firecloud.org"),
        Instant.parse("2021-07-01T18:14:27.688698Z"),
        None,
        Instant.parse("2021-07-01T18:14:30.915Z")
      ),
      DiskSize(451),
      DiskType.Standard,
      BlockSize(4096),
      Map(
        "diskName" -> "rzxybksgvy",
        "creator" -> "ron.weasley@test.firecloud.org",
        "googleProject" -> "gpalloc-dev-master-tzprbkr",
        "serviceAccount" -> "b305pet-114763077412354570085@gpalloc-dev-master-tzprbkr.iam.gserviceaccount.com"
      ),
      None,
      Some(WorkspaceId(UUID.fromString("5955382f-c8be-464b-b5b9-2c9260cd2661")))
    )
    res shouldBe (Right(expected))
  }

}
