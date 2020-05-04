package org.broadinstitute.dsde.workbench.leonardo
package db

import java.time.Instant

import io.circe.Printer
import io.circe.syntax._
import org.broadinstitute.dsde.workbench.google2.{DiskName, ZoneName}
import org.broadinstitute.dsde.workbench.google2.GKEModels.{KubernetesClusterName, NodePoolName}
import org.broadinstitute.dsde.workbench.google2.KubernetesModels.KubernetesMasterIP
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.KubernetesNamespaceName
import org.broadinstitute.dsde.workbench.google2.{Location, MachineTypeName, NetworkName, SubnetworkName}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{parseGcsPath, GcsPath, GoogleProject}
import slick.jdbc.MySQLProfile
import slick.jdbc.MySQLProfile.api._

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
    //overwrite Instant type mapping because slick 3.3.1 converts java.time.Instant to varchar by default
    //https://scala-slick.org/doc/3.3.1/upgrade.html#support-for-java.time-columns
    override val instantType = new InstantJdbcType
  }

  override val columnTypes = NewJdbcTypes

  object mappedColumnImplicits {
    implicit val userScriptPathMappedColumnType: BaseColumnType[UserScriptPath] =
      MappedColumnType
        .base[UserScriptPath, String](_.asString,
                                      s => UserScriptPath.stringToUserScriptPath(s).fold(e => throw e, identity))
    implicit val gsPathMappedColumnType: BaseColumnType[GcsPath] =
      MappedColumnType
        .base[GcsPath, String](_.toUri, s => parseGcsPath(s).fold(e => throw new Exception(e.toString()), identity))

    implicit val statusMappedColumnType: BaseColumnType[RuntimeStatus] =
      MappedColumnType
        .base[RuntimeStatus, String](_.entryName, s => RuntimeStatus.withName(s))

    implicit val googleProjectMappedColumnType: BaseColumnType[GoogleProject] =
      MappedColumnType
        .base[GoogleProject, String](_.value, GoogleProject.apply)
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
    implicit def customImageTypeMapper = MappedColumnType.base[RuntimeImageType, String](
      _.toString,
      s => RuntimeImageType.withName(s)
    )
    implicit val runtimeConfigIdMappedColumnType: BaseColumnType[RuntimeConfigId] =
      MappedColumnType.base[RuntimeConfigId, Long](_.id, RuntimeConfigId.apply)
    implicit val googleIdMappedColumnType: BaseColumnType[GoogleId] =
      MappedColumnType.base[GoogleId, String](_.value, GoogleId.apply)
    implicit val diskIdMappedColumnType: BaseColumnType[DiskId] =
      MappedColumnType.base[DiskId, Long](_.id, DiskId.apply)
    implicit val diskSizeMappedColumnType: BaseColumnType[DiskSize] =
      MappedColumnType.base[DiskSize, Int](_.gb, DiskSize.apply)
    implicit val zoneNameMappedColumnType: BaseColumnType[ZoneName] =
      MappedColumnType.base[ZoneName, String](_.value, ZoneName.apply)
    implicit val diskNameMappedColumnType: BaseColumnType[DiskName] =
      MappedColumnType.base[DiskName, String](_.value, DiskName.apply)
    implicit val diskSamResourceIdMappedColumnType: BaseColumnType[DiskSamResourceId] =
      MappedColumnType.base[DiskSamResourceId, String](_.asString, DiskSamResourceId.apply)
    implicit val diskStatusMappedColumnType: BaseColumnType[DiskStatus] =
      MappedColumnType.base[DiskStatus, String](_.entryName, DiskStatus.withName)
    implicit val diskTypeMappedColumnType: BaseColumnType[DiskType] =
      MappedColumnType.base[DiskType, String](_.entryName, DiskType.withName)
    implicit val blockSizeMappedColumnType: BaseColumnType[BlockSize] =
      MappedColumnType.base[BlockSize, Int](_.bytes, BlockSize.apply)
    implicit val labelResourceTypeColumnMapper: BaseColumnType[LabelResourceType] =
      MappedColumnType.base[LabelResourceType, String](
        _.asString,
        x => LabelResourceType.stringToLabelResourceType.getOrElse(x, throw new Exception(s"Unknown resource type $x"))
      )

    //Kubernetes column implicits
    implicit val kubernetesClusterLeoIdColumnType: BaseColumnType[KubernetesClusterLeoId] =
      MappedColumnType.base[KubernetesClusterLeoId, Long](_.id, KubernetesClusterLeoId.apply)
    implicit val kubernetesStatusColumnType: BaseColumnType[KubernetesClusterStatus] =
      MappedColumnType.base[KubernetesClusterStatus, String](_.entryName, s => KubernetesClusterStatus.withName(s))
    implicit val kubernetesClusterNameColumnType: BaseColumnType[KubernetesClusterName] =
      MappedColumnType.base[KubernetesClusterName, String](_.value, KubernetesClusterName.apply)
    implicit val operationNameColumnType: BaseColumnType[OperationName] =
      MappedColumnType.base[OperationName, String](_.value, OperationName.apply)
    implicit val locationColumnType: BaseColumnType[Location] =
      MappedColumnType.base[Location, String](_.value, Location.apply)
    implicit val apiServerIpColumnType: BaseColumnType[KubernetesMasterIP] =
      MappedColumnType.base[KubernetesMasterIP, String](_.value, KubernetesMasterIP.apply)
    implicit val kubernetesClusterSamResourceIdColumnType: BaseColumnType[KubernetesClusterSamResource] =
      MappedColumnType.base[KubernetesClusterSamResource, String](_.resourceId, KubernetesClusterSamResource.apply)
    implicit val networkNameColumnType: BaseColumnType[NetworkName] =
      MappedColumnType.base[NetworkName, String](_.value, NetworkName.apply)
    implicit val subNetworkNameColumnType: BaseColumnType[SubnetworkName] =
      MappedColumnType.base[SubnetworkName, String](_.value, SubnetworkName.apply)
    implicit val ipRange: BaseColumnType[IpRange] =
      MappedColumnType.base[IpRange, String](_.value, IpRange.apply)

    implicit val kubernetesNamespaceIdColumnType: BaseColumnType[KubernetesNamespaceId] =
      MappedColumnType.base[KubernetesNamespaceId, Long](_.id, KubernetesNamespaceId.apply)
    implicit val kubernetesNamespaceNameColumnType: BaseColumnType[KubernetesNamespaceName] =
      MappedColumnType.base[KubernetesNamespaceName, String](_.value, KubernetesNamespaceName.apply)

    implicit val nodepoolIdColumnType: BaseColumnType[NodepoolLeoId] =
      MappedColumnType.base[NodepoolLeoId, Long](_.id, NodepoolLeoId.apply)
    implicit val nodepoolNameColumnType: BaseColumnType[NodePoolName] =
      MappedColumnType.base[NodePoolName, String](_.value, NodePoolName.apply)
    implicit val numNodesColumnType: BaseColumnType[NumNodes] =
      MappedColumnType.base[NumNodes, Int](_.amount, NumNodes.apply)
    implicit val autoScalingMinColumnType: BaseColumnType[AutoScalingMin] =
      MappedColumnType.base[AutoScalingMin, Int](_.amount, AutoScalingMin.apply)
    implicit val autoScalingMaxColumnType: BaseColumnType[AutoScalingMax] =
      MappedColumnType.base[AutoScalingMax, Int](_.amount, AutoScalingMax.apply)
    implicit val nodepoolStatusColumnType: BaseColumnType[NodepoolStatus] =
      MappedColumnType.base[NodepoolStatus, String](_.entryName, s => NodepoolStatus.withName(s))
  }
}
