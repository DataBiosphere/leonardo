package org.broadinstitute.dsde.workbench.leonardo
package db

import java.time.Instant

import io.circe.Printer
import io.circe.syntax._
import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterImageType, UserScriptPath}
import org.broadinstitute.dsde.workbench.leonardo.model.google.{ClusterName, ClusterStatus}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{GcsPath, GoogleProject, parseGcsPath}
import slick.jdbc.MySQLProfile
import slick.jdbc.MySQLProfile.api._

private[db] object LeoProfile extends MySQLProfile {
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

    implicit val statusMappedColumnType: BaseColumnType[ClusterStatus] =
      MappedColumnType
        .base[ClusterStatus, String](_.entryName, s => ClusterStatus.withName(s))

    implicit val googleProjectMappedColumnType: BaseColumnType[GoogleProject] =
      MappedColumnType
        .base[GoogleProject, String](_.value, GoogleProject.apply)
    implicit val clusterNameMappedColumnType: BaseColumnType[ClusterName] =
      MappedColumnType
        .base[ClusterName, String](_.value, ClusterName.apply)
    implicit val workbenchEmailMappedColumnType: BaseColumnType[WorkbenchEmail] =
      MappedColumnType
        .base[WorkbenchEmail, String](_.value, WorkbenchEmail.apply)
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
    implicit def customImageTypeMapper = MappedColumnType.base[ClusterImageType, String](
      _.toString,
      s => ClusterImageType.withName(s)
    )
  }
}
