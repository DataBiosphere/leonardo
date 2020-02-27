package org.broadinstitute.dsde.workbench.leonardo.db

import java.time.Instant

import cats.effect.IO
import cats.effect.concurrent.Semaphore
import org.broadinstitute.dsde.workbench.leonardo.config.LiquibaseConfig
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference.initWithLiquibase
import org.broadinstitute.dsde.workbench.leonardo.model.Cluster
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, GcsPathUtils, LeonardoTestSuite, RuntimeConfig}
import org.broadinstitute.dsde.workbench.model.google.{GoogleProject, ServiceAccountKeyId}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, TestSuite}
import slick.basic.DatabaseConfig
import slick.dbio.DBIO
import slick.jdbc.JdbcProfile

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

trait TestComponent extends LeonardoTestSuite with ScalaFutures with GcsPathUtils with BeforeAndAfterAll {
  this: TestSuite =>
  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(10, Seconds)))

  val defaultServiceAccountKeyId = ServiceAccountKeyId("123")

  val initWithLiquibaseProp = "initLiquibase"
  val liquidBaseConfig =
    LiquibaseConfig("org/broadinstitute/dsde/workbench/leonardo/liquibase/changelog.xml", true)

  // Not using beforeAll because it's needed before beforeAll is called
  implicit protected lazy val dbRef: DbRef[IO] = initDbRef

  override def afterAll(): Unit = {
    dbRef.close()
    super.afterAll()
  }

  private def initDbRef = {
    val concurrentPermits = Semaphore[IO](100).unsafeRunSync()
    val dbConfig =
      DatabaseConfig.forConfig[JdbcProfile]("mysql", org.broadinstitute.dsde.workbench.leonardo.config.Config.config)
    val db = dbConfig.db

    if (sys.props.get(initWithLiquibaseProp).isEmpty) {
      val conn = db.source.createConnection()
      initWithLiquibase(conn, liquidBaseConfig)
      conn.close()
      sys.props.put(initWithLiquibaseProp, "done")
    }

    new DbRef[IO](dbConfig, db, concurrentPermits, blocker)
  }

  def dbFutureValue[T](f: DBIO[T]): T = dbRef.inTransaction(f).timeout(30 seconds).unsafeRunSync()
  def dbFailure[T](f: DBIO[T]): Throwable =
    dbRef.inTransaction(f).attempt.timeout(30 seconds).unsafeRunSync().swap.toOption.get

  // clean up after tests
  def isolatedDbTest[T](testCode: => T): T =
    try {
      if (dbRef != null) dbFutureValue(dbRef.dataAccess.truncateAll)
      testCode
    } catch {
      case t: Throwable => t.printStackTrace(); throw t
    } finally {
      if (dbRef != null) dbFutureValue(dbRef.dataAccess.truncateAll)
    }

  protected def getClusterId(getClusterIdRequest: GetClusterKey): Long =
    getClusterId(getClusterIdRequest.googleProject, getClusterIdRequest.clusterName, getClusterIdRequest.destroyedDate)

  protected def getClusterId(googleProject: GoogleProject,
                             clusterName: ClusterName,
                             destroyedDateOpt: Option[Instant]): Long =
    dbFutureValue { clusterQuery.getIdByUniqueKey(googleProject, clusterName, destroyedDateOpt) }.get

  implicit class ClusterExtensions(cluster: Cluster) {
    def save(serviceAccountKeyId: Option[ServiceAccountKeyId] = Some(defaultServiceAccountKeyId)): Cluster =
      dbFutureValue {
        clusterQuery.save(
          SaveCluster(cluster,
                      Some(gcsPath("gs://bucket" + cluster.clusterName.toString().takeRight(1))),
                      serviceAccountKeyId,
                      CommonTestData.defaultRuntimeConfig,
                      Instant.now)
        )
      }

    def saveWithRuntimeConfig(
      runtimeConfig: RuntimeConfig,
      serviceAccountKeyId: Option[ServiceAccountKeyId] = Some(defaultServiceAccountKeyId)
    ): Cluster =
      dbFutureValue {
        clusterQuery.save(
          SaveCluster(cluster,
                      Some(gcsPath("gs://bucket" + cluster.clusterName.toString().takeRight(1))),
                      serviceAccountKeyId,
                      runtimeConfig,
                      Instant.now)
        )
      }
  }
}
