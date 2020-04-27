package org.broadinstitute.dsde.workbench.leonardo
package db

import java.time.Instant

import io.circe.Printer
import io.circe.syntax._
import org.broadinstitute.dsde.workbench.google2.{DiskName, MachineTypeName, ZoneName}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{parseGcsPath, GcsPath, GoogleProject}
import slick.jdbc.MySQLProfile
import slick.jdbc.MySQLProfile.api._

private[leonardo] object LeoProfile extends MySQLProfile {
  final val dummyDate: Instant = Instant.ofEpochMilli(1000)

  // We use dummyDate when we don't have a destroyedDate but we need to insert something
  // into the database for that column as it can't be nullable since the column is used
  // as part of a unique key (along with googleProject and clusterName)
  // This comparison depends on timezone, use `env JAVA_OPTS="-Duser.timezone=UTC" sbt run`
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
  }
}
