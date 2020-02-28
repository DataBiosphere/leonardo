package org.broadinstitute.dsde.workbench.leonardo.db

import java.time.Instant

import cats.effect.concurrent.Semaphore
import cats.effect.{IO, Resource}
import cats.implicits._
import org.broadinstitute.dsde.workbench.leonardo.config.LiquibaseConfig
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
  val liquiBaseConfig =
    LiquibaseConfig("org/broadinstitute/dsde/workbench/leonardo/liquibase/changelog.xml", true)

  // Not using beforeAll because the dbRef is needed before beforeAll is called
  implicit protected lazy val dbRef: DbRef[IO] = initDbRef.unsafeRunSync()

  override def afterAll(): Unit = {
    dbRef.close()
    super.afterAll()
  }

  // This is a bit duplicative of DbReference.init but that method returns a Resource[F, DbRef[F]]
  // which doesn't play nicely with ScalaTest BeforeAndAfterAll. This version returns an F[DbRef[F]].
  private def initDbRef: IO[DbRef[IO]] =
    for {
      concurrentPermits <- Semaphore[IO](100)
      dbConfig <- IO(
        DatabaseConfig.forConfig[JdbcProfile]("mysql", org.broadinstitute.dsde.workbench.leonardo.config.Config.config)
      )
      db <- IO(dbConfig.db)
      // init with liquibase if we haven't done it yet
      _ <- if (sys.props.get(initWithLiquibaseProp).isEmpty)
        Resource
          .make(IO(db.source.createConnection()))(conn => IO(conn.close()))
          .use(conn => IO(DbReference.initWithLiquibase(conn, liquiBaseConfig))) >> IO(
          sys.props.put(initWithLiquibaseProp, "done")
        )
      else IO.unit
    } yield new DbRef[IO](dbConfig, db, concurrentPermits, blocker)

  def dbFutureValue[T](f: DBIO[T]): T = dbRef.inTransaction(f).timeout(30 seconds).unsafeRunSync()
  def dbFailure[T](f: DBIO[T]): Throwable =
    dbRef.inTransaction(f).attempt.timeout(30 seconds).unsafeRunSync().swap.toOption.get

  // clean up after tests
  def isolatedDbTest[T](testCode: => T): T =
    try {
      dbFutureValue(dbRef.dataAccess.truncateAll)
      testCode
    } catch {
      case t: Throwable => t.printStackTrace(); throw t
    } finally {
      dbFutureValue(dbRef.dataAccess.truncateAll)
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
