package org.broadinstitute.dsde.workbench.leonardo.db

import org.broadinstitute.dsde.workbench.azure.AzureCloudContext
import org.broadinstitute.dsde.workbench.google2.{MachineTypeName, RegionName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.RuntimeSamResourceId
import org.broadinstitute.dsde.workbench.leonardo.{
  AuditInfo,
  CloudContext,
  CloudService,
  DiskId,
  DiskSize,
  GpuType,
  LabelMap,
  RuntimeConfig,
  RuntimeName,
  RuntimeStatus,
  WorkspaceId
}
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.ColumnDecodingException
import org.broadinstitute.dsde.workbench.model.{IP, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import slick.jdbc.GetResult

import java.sql.SQLDataException
import java.time.Instant
import java.util.UUID

object GetResultInstances {
  implicit val getResultWorkspaceId: GetResult[Option[WorkspaceId]] =
    GetResult.GetStringOption.andThen(sOpt => sOpt.map(s => WorkspaceId(UUID.fromString(s))))
  implicit val getResultRuntimeName: GetResult[RuntimeName] =
    GetResult.GetString.andThen(s => RuntimeName(s))
  implicit val getResultRuntimeSamResourceId: GetResult[RuntimeSamResourceId] =
    GetResult.GetString.andThen(s => RuntimeSamResourceId(s))
  implicit val getResultCloudService: GetResult[CloudService] =
    GetResult.GetString.andThen(s => CloudService.withName(s))
  implicit val getResultDiskSizeOpt: GetResult[Option[DiskSize]] =
    GetResult.GetIntOption.andThen(size => size.map(s => DiskSize(s)))
  implicit val getResultMachineTypeName: GetResult[MachineTypeName] =
    GetResult.GetString.andThen(s => MachineTypeName(s))
  implicit val getResultMachineTypeNameOpt: GetResult[Option[MachineTypeName]] =
    GetResult.GetStringOption.andThen(s => s.map(MachineTypeName))
  implicit val getResultRegionNameOpt: GetResult[Option[RegionName]] =
    GetResult.GetStringOption.andThen(s => s.map(ss => RegionName(ss)))
  implicit val getResultZoneNameOpt: GetResult[Option[ZoneName]] =
    GetResult.GetStringOption.andThen(s => s.map(ss => ZoneName(ss)))
  implicit val getResultIPOpt: GetResult[Option[IP]] =
    GetResult.GetStringOption.andThen(s => s.map(ss => IP(ss)))
  implicit val getResultGpuTypeOpt: GetResult[Option[GpuType]] =
    GetResult.GetStringOption.andThen(s =>
      s.map(ss => GpuType.stringToObject.getOrElse(ss, throw ColumnDecodingException(s"invalid gpuType $ss")))
    )
  implicit val getResultInstant: GetResult[Instant] =
    GetResult.GetTimestamp.andThen(t => t.toInstant)
  implicit val getResultInstantOpt: GetResult[Option[Instant]] =
    GetResult.GetTimestampOption.andThen(t => t.map(_.toInstant))
  implicit val getResultDiskIdOpt: GetResult[Option[DiskId]] =
    GetResult.GetLongOption.andThen(t => t.map(DiskId))
  implicit val getResultMap: GetResult[Option[Map[String, String]]] =
    GetResult.GetStringOption.andThen { sOpt =>
      sOpt match {
        case Some(s) =>
          val res = for {
            s <- _root_.io.circe.parser.parse(s)
            map <- s.as[Map[String, String]]
          } yield map
          res match {
            case Left(e)      => throw e
            case Right(value) => Some(value)
          }
        case None => None
      }
    }
  implicit val getResultLabelMap: GetResult[LabelMap] =
    GetResult.createGetTuple2[String, String].andThen { case (keys, values) =>
      if (keys == null || values == null)
        Map.empty
      else {
        val keysList = keys.split(",")
        val valueList = values.split(",")

        keysList.zip(valueList).toMap
      }
    }
  implicit val getResultRuntimeStatus: GetResult[RuntimeStatus] =
    GetResult.GetString.andThen(s =>
      RuntimeStatus
        .withNameInsensitiveOption(s)
        .getOrElse(throw ColumnDecodingException(s"unexpected runtime status ${s} from database"))
    )
  implicit val getResultCloudContext: GetResult[CloudContext] =
    GetResult.createGetTuple2[String, String].andThen { case (provider, context) =>
      provider match {
        case "GCP" =>
          CloudContext.Gcp(GoogleProject(context)): CloudContext
        case "AZURE" =>
          val aContext =
            AzureCloudContext.fromString(context) match {
              case Left(e)      => throw new SQLDataException(e)
              case Right(value) => value
            }
          CloudContext.Azure(aContext): CloudContext
      }
    }

  implicit val getResultAuditInfo: GetResult[AuditInfo] =
    GetResult.createGetTuple4[String, Instant, Option[Instant], Instant].andThen {
      case (creator, createdDate, destroyedDate, dateAccessed) =>
        AuditInfo(
          WorkbenchEmail(creator),
          createdDate,
          destroyedDate.flatMap(LeoProfile.unmarshalDestroyedDate(_)),
          dateAccessed
        )
    }

  implicit val getResultRuntimeConfig: GetResult[RuntimeConfig] =
    GetResult
      .createGetTuple16[
        CloudService,
        Int,
        MachineTypeName,
        Option[DiskSize],
        Option[DiskSize],
        Option[MachineTypeName],
        Option[DiskSize],
        Option[Int],
        Option[Int],
        Option[Map[String, String]],
        Option[DiskId],
        Option[ZoneName],
        Option[RegionName],
        (Option[GpuType], Option[Int]),
        Boolean,
        Boolean
      ]
      .andThen {
        case (
              cloudService,
              numberOfWorkers,
              machineType,
              diskSize,
              bootDiskSize,
              workerMachineType,
              workerDiskSize,
              numberOfWorkerLocalSSDs,
              numberOfPreemptibleWorkers,
              dataprocProperties,
              persistentDiskId,
              zone,
              region,
              gpuConfig,
              componentGatewayEnabled,
              workerPrivateAccess
            ) =>
          RuntimeConfigTable.toRuntimeConfig(
            cloudService,
            numberOfWorkers,
            machineType,
            diskSize,
            bootDiskSize,
            workerMachineType,
            workerDiskSize,
            numberOfWorkerLocalSSDs,
            numberOfPreemptibleWorkers,
            dataprocProperties,
            persistentDiskId,
            zone,
            region,
            gpuConfig,
            componentGatewayEnabled,
            workerPrivateAccess
          )
      }
}
