package org.broadinstitute.dsde.workbench.leonardo
package db

import java.sql.SQLDataException
import java.time.Instant

import cats.implicits._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.mappedColumnImplicits._

class AppConfigTable(tag: Tag) extends Table[AppConfigRecord](tag, "APP_CONFIG") {
  def id = column[AppConfigId]("id", O.PrimaryKey, O.AutoInc)
  def appType = column[AppType]("appType", O.Length(254))
  def chart = column[Chart]("chart", O.Length(254))
  def diskId = column[DiskId]("diskId")
  def postgresDiskId = column[Option[DiskId]]("postgresDiskId")
  def descriptorName = column[Option[String]]("descriptorName", O.Length(254))
  def descriptorPath = column[Option[String]]("descriptorPath", O.Length(1024))
  def descriptorVersion = column[Option[String]]("descriptorVersion", O.Length(254))
  def descriptorExtraArgs = column[List[String]]("descriptorExtraArgs")
  def dateAccessed = column[Instant]("dateAccessed", O.SqlType("TIMESTAMP(6)"))

  def * =
    (
      id,
      (
        appType,
        chart,
        diskId,
        postgresDiskId,
        descriptorName,
        descriptorPath,
        descriptorVersion,
        descriptorExtraArgs
      ),
      dateAccessed
    ).shaped <> ({
      case (id,
            (appType,
             chart,
             diskId,
             postgresDiskId,
             descriptorName,
             descriptorPath,
             descriptorVersion,
             descriptorExtraArgs),
            dateAccessed) =>
        val res = appType match {
          case AppType.Galaxy =>
            AppConfig.GalaxyConfig(
              chart,
              diskId,
              postgresDiskId.getOrElse(throw new SQLDataException("Galaxy has to have a postgres disk"))
            )
          case AppType.Custom =>
            val descriptor = (descriptorName, descriptorPath, descriptorVersion).mapN {
              case (n, p, v) =>
                CustomAppDescriptor(n, p, v, descriptorExtraArgs)
            }
            AppConfig.Custom(descriptor.getOrElse(throw new SQLDataException("Custom apps need to have a descriptor")),
                             chart,
                             diskId)
        }

        AppConfigRecord(id, res, dateAccessed)
    }, { x: AppConfigRecord =>
      x.appConfig match {
        case AppConfig.GalaxyConfig(chart, diskId, postgresDiskId) =>
          Some(x.id,
               (AppType.Galaxy, chart, diskId, Some(postgresDiskId), None, None, None, List.empty),
               x.dateAccessed)
        case AppConfig.Custom(descriptor, chart, diskId) =>
          Some(x.id,
               (AppType.Custom,
                chart,
                diskId,
                None,
                Some(descriptor.name),
                Some(descriptor.path),
                Some(descriptor.version),
                descriptor.extraArgs),
               x.dateAccessed)
      }
    })
}

final case class AppConfigRecord(id: AppConfigId, appConfig: AppConfig, dateAccessed: Instant)
