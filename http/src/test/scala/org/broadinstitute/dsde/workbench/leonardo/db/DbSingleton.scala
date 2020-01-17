package org.broadinstitute.dsde.workbench.leonardo.db

import cats.effect.IO
import cats.effect.concurrent.Semaphore
import org.broadinstitute.dsde.workbench.leonardo.LeonardoTestSuite
import org.broadinstitute.dsde.workbench.leonardo.config.LiquibaseConfig
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference.initWithLiquibase
import slick.basic.DatabaseConfig
import slick.jdbc.JdbcProfile

// initialize database tables and connection pool only once
object DbSingleton extends LeonardoTestSuite {
  private val concurrentPermits = Semaphore[IO](100).unsafeRunSync()
  private val dbConfig =
    DatabaseConfig.forConfig[JdbcProfile]("mysql", org.broadinstitute.dsde.workbench.leonardo.config.Config.config)

  private val db = dbConfig.db //TODO: this should be initlized and closed on each test suite ideally

  private val liquidBaseConfig =
    LiquibaseConfig("org/broadinstitute/dsde/workbench/leonardo/liquibase/changelog.xml", true)

  val conn = db.source.createConnection()
  initWithLiquibase(conn, liquidBaseConfig)
  conn.close()

  val dbRef: DbReference[IO] = new DbRef[IO](dbConfig, db, concurrentPermits, blocker)
}
