package org.broadinstitute.dsde.workbench.leonardo
package db

import cats.effect.concurrent.Semaphore
import cats.effect.{Blocker, IO}
import cats.syntax.all._
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.leonaroBaseUrl
import org.broadinstitute.dsde.workbench.model.google.{GoogleProject, ServiceAccountKeyId}
import org.scalatest.{BeforeAndAfterAll, TestSuite}
import slick.dbio.DBIO

import java.time.Instant
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

trait TestComponent extends LeonardoTestSuite with GcsPathUtils with BeforeAndAfterAll {
  this: TestSuite =>
  val config = ConfigFactory.parseResources("leonardo.conf").withFallback(ConfigFactory.load()).resolve()

  val defaultServiceAccountKeyId = ServiceAccountKeyId("123")

  val initWithLiquibaseProp = "initLiquibase"

  // Not using beforeAll because the dbRef is needed before beforeAll is called
//  implicit protected lazy val testDbRef: DbRef[IO] = initDbRef.unsafeRunSync()

  override def afterAll(): Unit =
    super.afterAll()

  def dbFutureValue[T](f: DBIO[T])(implicit dbRef: DbReference[IO]): T =
    dbRef.inTransaction(f).timeout(30 seconds).unsafeRunSync()
  def dbFailure[T](f: DBIO[T])(implicit dbRef: DbReference[IO]): Throwable =
    dbRef.inTransaction(f).attempt.timeout(30 seconds).unsafeRunSync().swap.toOption.get

  // clean up after tests
  def isolatedDbTest[T](testCode: DbReference[IO] => T): T = {
    val res = for {
      concurrentPermits <- Semaphore[IO](200)
      blocker = Blocker.liftExecutionContext(global)
      liquibaseConfig <- if (sys.props.get(initWithLiquibaseProp).isEmpty)
        IO(sys.props.put(initWithLiquibaseProp, "done"))
          .as(LiquibaseConfig("org/broadinstitute/dsde/workbench/leonardo/liquibase/changelog.xml", true))
      else
        IO.pure(LiquibaseConfig("org/broadinstitute/dsde/workbench/leonardo/liquibase/changelog.xml", false))
      r <- (new DbReferenceInitializer[IO]).init(liquibaseConfig, config, concurrentPermits, blocker).use { dbRef =>
        dbRef.dataAccess.truncateAll.transaction(dbRef) >> IO(testCode(dbRef))
      }
    } yield r
    res.unsafeRunSync()
  }

  protected def getClusterId(getClusterIdRequest: GetClusterKey)(implicit dbReference: DbReference[IO]): Long =
    getClusterId(getClusterIdRequest.googleProject, getClusterIdRequest.clusterName, getClusterIdRequest.destroyedDate)

  protected def getClusterId(googleProject: GoogleProject, clusterName: RuntimeName, destroyedDateOpt: Option[Instant])(
    implicit dbReference: DbReference[IO]
  ): Long =
    dbFutureValue(clusterQuery.getIdByUniqueKey(googleProject, clusterName, destroyedDateOpt)).get

  implicit class ClusterExtensions(cluster: Runtime)(implicit dbReference: DbReference[IO]) {
    def save(serviceAccountKeyId: Option[ServiceAccountKeyId] = Some(defaultServiceAccountKeyId)): Runtime =
      dbFutureValue {
        clusterQuery.save(
          SaveCluster(
            cluster,
            Some(gcsPath("gs://bucket" + cluster.runtimeName.asString.takeRight(1))),
            serviceAccountKeyId,
            CommonTestData.defaultDataprocRuntimeConfig,
            Instant.now
          )
        )
      }

    def saveWithRuntimeConfig(
      runtimeConfig: RuntimeConfig,
      serviceAccountKeyId: Option[ServiceAccountKeyId] = Some(defaultServiceAccountKeyId)
    ): Runtime =
      dbFutureValue {
        clusterQuery.save(
          SaveCluster(cluster,
                      Some(gcsPath("gs://bucket" + cluster.runtimeName.asString.takeRight(1))),
                      serviceAccountKeyId,
                      runtimeConfig,
                      Instant.now)
        )
      }
  }

  implicit class DiskExtensions(disk: PersistentDisk)(implicit dbReference: DbReference[IO]) {
    def save(): IO[PersistentDisk] = dbReference.inTransaction(persistentDiskQuery.save(disk))
  }

  implicit class KubernetesClusterExtensions(c: KubernetesCluster)(implicit dbReference: DbReference[IO]) {
    def save(): KubernetesCluster =
      dbFutureValue {
        kubernetesClusterQuery.save(
          SaveKubernetesCluster(
            c.googleProject,
            c.clusterName,
            c.location,
            c.region,
            c.status,
            c.ingressChart,
            c.auditInfo,
            DefaultNodepool.fromNodepool(
              c.nodepools.headOption
                .getOrElse(throw new Exception("test clusters to be saved must have at least 1 nodepool"))
            )
          )
        )
      }
  }

  implicit class NodepoolExtension(n: Nodepool)(implicit dbReference: DbReference[IO]) {
    def save(): Nodepool =
      dbFutureValue {
        nodepoolQuery.saveForCluster(n)
      }
  }

  implicit class AppExtension(a: App)(implicit dbReference: DbReference[IO]) {
    def save(): App =
      dbFutureValue {
        appQuery.save(
          SaveApp(a)
        )
      }
  }
}
