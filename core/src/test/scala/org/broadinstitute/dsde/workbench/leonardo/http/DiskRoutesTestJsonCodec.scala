package org.broadinstitute.dsde.workbench.leonardo
package http

import io.circe.{Decoder, Encoder}
import org.broadinstitute.dsde.workbench.google2.{DiskName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.PersistentDiskSamResourceId
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

object DiskRoutesTestJsonCodec {
  implicit val createDiskRequestEncoder: Encoder[CreateDiskRequest] = Encoder.forProduct5(
    "labels",
    "size",
    "diskType",
    "blockSize",
    "zone"
  )(x =>
    (
      x.labels,
      x.size,
      x.diskType,
      x.blockSize,
      x.zone
    )
  )

  implicit val getDiskResponseDecoder: Decoder[GetPersistentDiskResponse] = Decoder.instance { x =>
    for {
      id <- x.downField("id").as[DiskId]
      _ <- x
        .downField("googleProject")
        .as[GoogleProject] //this is only here for backwards-compatibility test. Once the API move away from googleProject, we can remove this as well
      cloudContext <- x.downField("cloudContext").as[CloudContext]
      zone <- x.downField("zone").as[ZoneName]
      name <- x.downField("name").as[DiskName]
      googleId <- x.downField("googleId").as[Option[ProxyHostName]]
      serviceAccount <- x.downField("serviceAccount").as[WorkbenchEmail]
      status <- x.downField("status").as[DiskStatus]
      auditInfo <- x.downField("auditInfo").as[AuditInfo]
      size <- x.downField("size").as[DiskSize]
      diskType <- x.downField("diskType").as[DiskType]
      blockSize <- x.downField("blockSize").as[BlockSize]
      labels <- x.downField("labels").as[LabelMap]
    } yield GetPersistentDiskResponse(id,
                                      cloudContext,
                                      zone,
                                      name,
                                      serviceAccount,
                                      PersistentDiskSamResourceId("test"),
                                      status,
                                      auditInfo,
                                      size,
                                      diskType,
                                      blockSize,
                                      labels)
  }

  implicit val listDiskResponseDecoder: Decoder[ListPersistentDiskResponse] = Decoder.instance { x =>
    for {
      id <- x.downField("id").as[DiskId]
      _ <- x
        .downField("googleProject")
        .as[GoogleProject] //this is only here for backwards-compatibility test. Once the API move away from googleProject, we can remove this as well
      cloudContext <- x.downField("cloudContext").as[CloudContext]
      zone <- x.downField("zone").as[ZoneName]
      name <- x.downField("name").as[DiskName]
      status <- x.downField("status").as[DiskStatus]
      auditInfo <- x.downField("auditInfo").as[AuditInfo]
      size <- x.downField("size").as[DiskSize]
      diskType <- x.downField("diskType").as[DiskType]
      blockSize <- x.downField("blockSize").as[BlockSize]
      formattedBy <- x.downField("googleId").as[Option[FormattedBy]]
      labels <- x.downField("labels").as[LabelMap]
    } yield ListPersistentDiskResponse(
      id,
      cloudContext,
      zone,
      name,
      status,
      auditInfo,
      size,
      diskType,
      blockSize,
      formattedBy,
      labels
    )
  }
}
