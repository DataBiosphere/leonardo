package org.broadinstitute.dsde.workbench.leonardo
package db

import java.time.Instant
import io.circe.Printer
import io.circe.syntax._
import org.broadinstitute.dsde.workbench.google2.{
  DiskName,
  Location,
  MachineTypeName,
  NetworkName,
  RegionName,
  SubnetworkName,
  ZoneName
}
import org.broadinstitute.dsde.workbench.google2.GKEModels.{KubernetesClusterName, NodepoolName}
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{
  NamespaceName,
  ServiceAccountName,
  ServiceName
}
import org.broadinstitute.dsde.workbench.model.{IP, TraceId, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId._
import org.broadinstitute.dsde.workbench.model.google.{parseGcsPath, GcsPath, GoogleProject}
import org.broadinstitute.dsp.Release
import org.http4s.Uri
import slick.jdbc.{MySQLProfile, SetParameter}
import slick.jdbc.MySQLProfile.api._

import java.nio.file.{Path, Paths}
import java.sql.SQLDataException
import java.util.UUID

private[leonardo] object LeoProfile extends MySQLProfile {
  final val dummyDate: Instant = Instant.ofEpochMilli(1000)

  // We use dummyDate when we don't have a destroyedDate but we need to insert something
  // into the database for that column as it can't be nullable since the column is used
  // as part of a unique key (along with googleProject and clusterName)
  // This comparison depends on timezone, use `export JAVA_OPTS="-Duser.timezone=UTC" sbt run`
  def unmarshalDestroyedDate(destroyedDate: Instant): Option[Instant] =
    if (destroyedDate == dummyDate)
      None
    else
      Some(destroyedDate)

  object NewJdbcTypes extends super.JdbcTypes {
    // overwrite Instant type mapping because slick 3.3.1 converts java.time.Instant to varchar by default
    // https://scala-slick.org/doc/3.3.1/upgrade.html#support-for-java.time-columns
    override val instantType = new InstantJdbcType
  }

  override val columnTypes = NewJdbcTypes

  object mappedColumnImplicits {
    implicit val userScriptPathMappedColumnType: BaseColumnType[UserScriptPath] =
      MappedColumnType
        .base[UserScriptPath, String](
          _.asString,
          s =>
            UserScriptPath
              .stringToUserScriptPath(s,
                                      false
              ) // We don't make additional check here becuz there're invalid data in DB already, which we need to tolerate for listRuntime API
              .fold(e => throw new SQLDataException(s"${s} is invalid UserScriptPath due to ${e.getMessage}"), identity)
        )
    implicit val gsPathMappedColumnType: BaseColumnType[GcsPath] =
      MappedColumnType
        .base[GcsPath, String](_.toUri,
                               s => parseGcsPath(s).fold(e => throw ColumnDecodingException(e.toString()), identity)
        )
    implicit val computeClassMappedColumnType: BaseColumnType[ComputeClass] =
      MappedColumnType
        .base[ComputeClass, String](
          _.toString,
          s =>
            ComputeClass.stringToObject.get(s.toLowerCase) match {
              case Some(v) => v
              case None    => throw ColumnDecodingException(s"Invalid compute class in DB ${s}")
            }
        )

    implicit val statusMappedColumnType: BaseColumnType[RuntimeStatus] =
      MappedColumnType
        .base[RuntimeStatus, String](
          _.toString,
          s =>
            RuntimeStatus
              .withNameInsensitiveOption(s)
              .getOrElse(throw ColumnDecodingException(s"unexpected runtime status ${s} from database"))
        )

    implicit val cloudContextDbMappedColumnType: BaseColumnType[CloudContextDb] =
      MappedColumnType
        .base[CloudContextDb, String](_.value, CloudContextDb.apply)
    implicit val cloudProviderMappedColumnType: BaseColumnType[CloudProvider] =
      MappedColumnType
        .base[CloudProvider, String](
          _.asString,
          s =>
            CloudProvider.stringToCloudProvider
              .get(s)
              .getOrElse(throw ColumnDecodingException(s"unexpected CloudProvider ${s} from database"))
        )
    implicit val googleProjectMappedColumnType: BaseColumnType[GoogleProject] =
      MappedColumnType
        .base[GoogleProject, String](_.value, GoogleProject.apply)
    implicit val pathMappedColumnType: BaseColumnType[Path] =
      MappedColumnType
        .base[Path, String](_.toString, x => Paths.get(x))
    implicit val clusterNameMappedColumnType: BaseColumnType[RuntimeName] =
      MappedColumnType
        .base[RuntimeName, String](_.asString, RuntimeName.apply)
    implicit val workbenchEmailMappedColumnType: BaseColumnType[WorkbenchEmail] =
      MappedColumnType
        .base[WorkbenchEmail, String](_.value, WorkbenchEmail.apply)
    implicit val machineTypeMappedColumnType: BaseColumnType[MachineTypeName] =
      MappedColumnType
        .base[MachineTypeName, String](_.value, MachineTypeName.apply)
    implicit val cloudServiceMappedColumnType: BaseColumnType[CloudService] =
      MappedColumnType
        .base[CloudService, String](_.asString, s => CloudService.withName(s))
    // mysql 5.6 doesns't support json. Hence writing properties field as string in json format
    implicit val mapMappedColumnType: BaseColumnType[Map[String, String]] =
      MappedColumnType
        .base[Map[String, String], String](
          _.asJson.printWith(Printer.noSpaces),
          s => {
            val res = for {
              s <- _root_.io.circe.parser.parse(s)
              map <- s.as[Map[String, String]]
            } yield map
            res.fold(e => throw e, identity)
          }
        )
    implicit val mapListColumnType: BaseColumnType[List[String]] =
      MappedColumnType.base[List[String], String](
        _.asJson.printWith(Printer.noSpaces),
        s => {
          val res = for {
            s <- _root_.io.circe.parser.parse(s)
            list <- s.as[List[String]]
          } yield list
          res.fold(e => throw e, identity)
        }
      )
    implicit def customImageTypeMapper: BaseColumnType[RuntimeImageType] =
      MappedColumnType.base[RuntimeImageType, String](
        _.toString,
        s => RuntimeImageType.withName(s)
      )
    implicit val runtimeConfigIdMappedColumnType: BaseColumnType[RuntimeConfigId] =
      MappedColumnType.base[RuntimeConfigId, Long](_.id, RuntimeConfigId.apply)
    implicit val googleIdMappedColumnType: BaseColumnType[ProxyHostName] =
      MappedColumnType.base[ProxyHostName, String](_.value, ProxyHostName.apply)
    implicit val diskIdMappedColumnType: BaseColumnType[DiskId] =
      MappedColumnType.base[DiskId, Long](_.value, DiskId.apply)
    implicit val diskSizeMappedColumnType: BaseColumnType[DiskSize] =
      MappedColumnType.base[DiskSize, Int](_.gb, DiskSize.apply)
    implicit val zoneNameMappedColumnType: BaseColumnType[ZoneName] =
      MappedColumnType.base[ZoneName, String](_.value, ZoneName.apply)
    implicit val diskNameMappedColumnType: BaseColumnType[DiskName] =
      MappedColumnType.base[DiskName, String](_.value, DiskName.apply)
    implicit val diskSamResourceMappedColumnType: BaseColumnType[PersistentDiskSamResourceId] =
      MappedColumnType.base[PersistentDiskSamResourceId, String](_.resourceId, PersistentDiskSamResourceId.apply)
    implicit val diskStatusMappedColumnType: BaseColumnType[DiskStatus] =
      MappedColumnType.base[DiskStatus, String](_.entryName, DiskStatus.withName)
    implicit val diskTypeMappedColumnType: BaseColumnType[DiskType] =
      MappedColumnType.base[DiskType, String](_.entryName, DiskType.withName)
    implicit val blockSizeMappedColumnType: BaseColumnType[BlockSize] =
      MappedColumnType.base[BlockSize, Int](_.bytes, BlockSize.apply)
    implicit val pvcIdMappedColumnType: BaseColumnType[PvcId] =
      MappedColumnType.base[PvcId, String](_.asString, PvcId.apply)
    implicit val traceIdIdMappedColumnType: BaseColumnType[TraceId] =
      MappedColumnType.base[TraceId, String](_.asString, TraceId.apply)
    implicit val labelResourceTypeColumnMapper: BaseColumnType[LabelResourceType] =
      MappedColumnType.base[LabelResourceType, String](
        _.asString,
        x => LabelResourceType.stringToLabelResourceType.getOrElse(x, throw new Exception(s"Unknown resource type $x"))
      )
    implicit val formattedByMappedColumnType: BaseColumnType[FormattedBy] =
      MappedColumnType.base[FormattedBy, String](
        _.asString,
        s => FormattedBy.withNameInsensitiveOption(s).getOrElse(throw new RuntimeException(s"Unknown formattedBy $s"))
      )

    // Kubernetes column implicits
    implicit val kubernetesClusterLeoIdColumnType: BaseColumnType[KubernetesClusterLeoId] =
      MappedColumnType.base[KubernetesClusterLeoId, Long](_.id, KubernetesClusterLeoId.apply)
    implicit val kubernetesStatusColumnType: BaseColumnType[KubernetesClusterStatus] =
      MappedColumnType.base[KubernetesClusterStatus, String](
        _.toString,
        s => KubernetesClusterStatus.stringToObject.getOrElse(s, throw new Exception(s"invalid cluster status ${s}"))
      )
    implicit val kubernetesClusterNameColumnType: BaseColumnType[KubernetesClusterName] =
      MappedColumnType.base[KubernetesClusterName, String](_.value, KubernetesClusterName.apply)
    implicit val regionNameColumnType: BaseColumnType[RegionName] =
      MappedColumnType.base[RegionName, String](_.value, RegionName.apply)
    implicit val locationColumnType: BaseColumnType[Location] =
      MappedColumnType.base[Location, String](_.value, Location.apply)
    implicit val ipColumnType: BaseColumnType[IP] =
      MappedColumnType.base[IP, String](_.asString, IP.apply)

    implicit val networkNameColumnType: BaseColumnType[NetworkName] =
      MappedColumnType.base[NetworkName, String](_.value, NetworkName.apply)
    implicit val subNetworkNameColumnType: BaseColumnType[SubnetworkName] =
      MappedColumnType.base[SubnetworkName, String](_.value, SubnetworkName.apply)
    implicit val ipRange: BaseColumnType[IpRange] =
      MappedColumnType.base[IpRange, String](_.value, IpRange.apply)

    implicit val namespaceNameColumnType: BaseColumnType[NamespaceName] =
      MappedColumnType.base[NamespaceName, String](_.value, NamespaceName.apply)
    implicit val kubernetesServiceAccountColumnType: BaseColumnType[ServiceAccountName] =
      MappedColumnType.base[ServiceAccountName, String](_.value, ServiceAccountName.apply)

    implicit val nodepoolIdColumnType: BaseColumnType[NodepoolLeoId] =
      MappedColumnType.base[NodepoolLeoId, Long](_.id, NodepoolLeoId.apply)
    implicit val nodepoolNameColumnType: BaseColumnType[NodepoolName] =
      MappedColumnType.base[NodepoolName, String](_.value, NodepoolName.apply)
    implicit val numNodesColumnType: BaseColumnType[NumNodes] =
      MappedColumnType.base[NumNodes, Int](_.amount, NumNodes.apply)
    implicit val autoscalingMinColumnType: BaseColumnType[AutoscalingMin] =
      MappedColumnType.base[AutoscalingMin, Int](_.amount, AutoscalingMin.apply)
    implicit val autoscalingMaxColumnType: BaseColumnType[AutoscalingMax] =
      MappedColumnType.base[AutoscalingMax, Int](_.amount, AutoscalingMax.apply)
    implicit val nodepoolStatusColumnType: BaseColumnType[NodepoolStatus] =
      MappedColumnType.base[NodepoolStatus, String](
        _.toString,
        s => NodepoolStatus.stringToObject.getOrElse(s, throw ColumnDecodingException(s"invalid nodepool status ${s}"))
      )

    implicit val appIdColumnType: BaseColumnType[AppId] =
      MappedColumnType.base[AppId, Long](_.id, AppId.apply)
    implicit val appUsageRecordIdColumnType: BaseColumnType[AppUsageId] =
      MappedColumnType.base[AppUsageId, Long](_.id, AppUsageId.apply)
    implicit val appNameColumnType: BaseColumnType[AppName] =
      MappedColumnType.base[AppName, String](_.value, AppName.apply)
    implicit val appAccessScopeColumnType: BaseColumnType[AppAccessScope] =
      MappedColumnType.base[AppAccessScope, String](
        _.toString,
        s => AppAccessScope.stringToObject.getOrElse(s, throw ColumnDecodingException(s"invalid app access scope ${s}"))
      )
    implicit val appSamResourceIdColumnType: BaseColumnType[AppSamResourceId] =
      MappedColumnType.base[AppSamResourceId, String](_.resourceId, s => AppSamResourceId.apply(s, None))
    implicit val appTypeColumnType: BaseColumnType[AppType] =
      MappedColumnType.base[AppType, String](
        _.toString,
        s => AppType.stringToObject.getOrElse(s, throw ColumnDecodingException(s"invalid app type ${s}"))
      )
    implicit val appStatusColumnType: BaseColumnType[AppStatus] =
      MappedColumnType.base[AppStatus, String](
        _.toString,
        s => AppStatus.stringToObject.getOrElse(s, throw ColumnDecodingException(s"invalid app status ${s}"))
      )
    implicit val chartColumnType: BaseColumnType[Chart] =
      MappedColumnType.base[Chart, String](
        _.toString,
        s => Chart.fromString(s).getOrElse(throw ColumnDecodingException(s"invalid chart ${s}"))
      )
    implicit val releaseColumnType: BaseColumnType[Release] =
      MappedColumnType.base[Release, String](_.asString, Release.apply)

    implicit val errorSourceColumnType: BaseColumnType[ErrorSource] =
      MappedColumnType.base[ErrorSource, String](
        _.toString,
        s => ErrorSource.stringToObject.getOrElse(s, throw ColumnDecodingException(s"invalid error source ${s}"))
      )

    implicit val errorActionColumnType: BaseColumnType[ErrorAction] =
      MappedColumnType.base[ErrorAction, String](
        _.toString,
        s => ErrorAction.stringToObject.getOrElse(s, throw ColumnDecodingException(s"invalid error action ${s}"))
      )

    implicit val errorIdColumnType: BaseColumnType[KubernetesErrorId] =
      MappedColumnType.base[KubernetesErrorId, Long](_.value, KubernetesErrorId.apply)

    implicit val serviceIdColumnType: BaseColumnType[ServiceId] =
      MappedColumnType.base[ServiceId, Long](_.id, ServiceId.apply)
    implicit val serviceNameColumnType: BaseColumnType[ServiceName] =
      MappedColumnType.base[ServiceName, String](_.value, ServiceName.apply)
    implicit val serviceKindColumnType: BaseColumnType[KubernetesServiceKindName] =
      MappedColumnType.base[KubernetesServiceKindName, String](_.value, KubernetesServiceKindName.apply)
    implicit val servicePathColumnType: BaseColumnType[ServicePath] =
      MappedColumnType.base[ServicePath, String](_.value, ServicePath.apply)

    implicit val uriColumnType: BaseColumnType[Uri] =
      MappedColumnType.base[Uri, String](
        _.toString,
        s => Uri.fromString(s).getOrElse(throw ColumnDecodingException(s"invalid uri $s"))
      )
    implicit val gpuTypeColumnType: BaseColumnType[GpuType] =
      MappedColumnType.base[GpuType, String](
        _.asString,
        s => GpuType.stringToObject.getOrElse(s, throw ColumnDecodingException(s"invalid gpuType $s"))
      )

    implicit val wsmResourceTypeColumnType: BaseColumnType[WsmResourceType] =
      MappedColumnType.base[WsmResourceType, String](
        _.toString,
        s => WsmResourceType.stringToObject.getOrElse(s, throw ColumnDecodingException(s"invalid wsmResourceType $s"))
      )

    implicit val wsmControlledResourceIdColumnType: BaseColumnType[WsmControlledResourceId] =
      MappedColumnType
        .base[WsmControlledResourceId, String](_.value.toString, s => WsmControlledResourceId(UUID.fromString(s)))

    implicit val workspaceIdColumnType: BaseColumnType[WorkspaceId] =
      MappedColumnType
        .base[WorkspaceId, String](_.value.toString, s => WorkspaceId(UUID.fromString(s)))

    implicit val diskLinkColumnType: BaseColumnType[DiskLink] =
      MappedColumnType
        .base[DiskLink, String](_.asString, DiskLink.apply)

    implicit val appControlledResourceStatusColumnType: BaseColumnType[AppControlledResourceStatus] =
      MappedColumnType.base[AppControlledResourceStatus, String](
        _.toString,
        s =>
          AppControlledResourceStatus.stringToObject
            .getOrElse(s, throw ColumnDecodingException(s"invalid app controlled resource status ${s}"))
      )

    implicit val instantSetParameter: SetParameter[Instant] =
      SetParameter.SetTimestamp.contramap(instant => java.sql.Timestamp.from(instant))

    implicit val autodeleteThresholdColumnType: BaseColumnType[Option[AutodeleteThreshold]] =
      MappedColumnType.base[Option[AutodeleteThreshold], Int](_.map(_.value).getOrElse(0),
                                                              i => Option(AutodeleteThreshold(i))
      )
  }

  case class ColumnDecodingException(message: String) extends Exception
}
