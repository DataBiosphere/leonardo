package org.broadinstitute.dsde.workbench.leonardo
package db

import java.time.Instant

import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.mappedColumnImplicits._
import slick.dbio.DBIO
import slick.lifted.TableQuery

object AppConfigQueries {

  val appConfigs = TableQuery[AppConfigTable]

  /**
   * return DB generated id
   */
  def insertAppConfig(appConfig: AppConfig, dateAccessed: Instant): DBIO[AppConfigId] =
    appConfigs.returning(appConfigs.map(_.id)) += AppConfigRecord(AppConfigId(0), appConfig, dateAccessed)
}
