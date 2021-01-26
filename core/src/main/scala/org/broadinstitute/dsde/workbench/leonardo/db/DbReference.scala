package org.broadinstitute.dsde.workbench.leonardo
package db

import java.sql.{Connection, SQLTimeoutException}
import cats.effect.concurrent.Semaphore
import cats.effect.{Async, Blocker, ContextShift, Resource, Timer}
import com.google.common.base.Throwables
import _root_.liquibase.database.jvm.JdbcConnection
import _root_.liquibase.resource.{ClassLoaderResourceAccessor, ResourceAccessor}
import _root_.liquibase.{Contexts, Liquibase}
import slick.basic.DatabaseConfig
import slick.jdbc.{JdbcBackend, JdbcProfile, TransactionIsolation}
import sun.security.provider.certpath.SunCertPathBuilderException
import LeoProfile.api._
import io.chrisdavenport.log4cats.Logger
import cats.syntax.all._

import scala.concurrent.duration._
import scala.concurrent.Future

class DbReferenceInitializer[F[_]: ContextShift: Timer](implicit logger: Logger[F], F: Async[F]) {

  private def initWithLiquibase(dbConnection: Connection,
                                liquibaseConfig: LiquibaseConfig,
                                changelogParameters: Map[String, AnyRef] = Map.empty): F[Unit] = {
    val liquibaseConnection = new JdbcConnection(dbConnection)
    val resourceAccessor: ResourceAccessor = new ClassLoaderResourceAccessor()
    val liquibase = new Liquibase(liquibaseConfig.changelog, resourceAccessor, liquibaseConnection)
    changelogParameters.foreach { case (key, value) => liquibase.setChangeLogParameter(key, value) }

    for {
      _ <- F.delay(liquibase.update(new Contexts())).onError {
        case e: SQLTimeoutException =>
          val isCertProblem = Throwables.getRootCause(e).isInstanceOf[SunCertPathBuilderException]
          if (isCertProblem) {
            val k = "javax.net.ssl.keyStore"
            if (System.getProperty(k) == null) {
              logger.warn("************") >> logger.warn(
                s"The system property '${k}' is null. This is likely the cause of the database connection failure."
              ) >> logger.warn("************")
            } else logger.error("unexpected DB error")
          } else logger.error("unexpected DB error")
      }
    } yield ()
  }

  def init(config: LiquibaseConfig,
           entireConfig: com.typesafe.config.Config,
           concurrentDbAccessPermits: Semaphore[F],
           blocker: Blocker): Resource[F, DbReference[F]] = {
    val dbConfig =
      DatabaseConfig.forConfig[JdbcProfile]("mysql", entireConfig)

    for {
      db <- Resource.make(F.delay(dbConfig.db))(db => logger.info("closing database") >> F.delay(db.close()))
      getConnection = fs2.Stream
        .retry(
          Async[F].delay(db.source.createConnection()),
          2 seconds,
          _ * 2,
          5, {
            case _: java.sql.SQLTransientException => true
            case _                                 => false
          }
        )
        .compile
        .lastOrError
      dbConnection <- Resource.make(getConnection)(conn =>
        logger.info(s"Max database connection ${db.source.maxConnections}. Closing connections") >> F.delay(
          conn.close()
        )
      )
      initLiquibase = if (config.initWithLiquibase)
        initWithLiquibase(dbConnection, config)
      else F.unit
      _ <- Resource.liftF(initLiquibase)
    } yield new DbRef[F](db, concurrentDbAccessPermits, blocker)
  }
}

trait DbReference[F[_]] {
  def dataAccess: DataAccess
  def inTransaction[T](
    dbio: DBIO[T],
    isolationLevel: TransactionIsolation = TransactionIsolation.RepeatableRead
  ): F[T]
}

private[db] class DbRef[F[_]: Async: ContextShift](database: JdbcBackend#DatabaseDef,
                                                   concurrentDbAccessPermits: Semaphore[F],
                                                   blocker: Blocker)
    extends DbReference[F] {
  import LeoProfile.api._

  val dataAccess = new DataAccess(blocker)

  private def inTransactionFuture[T](
    dbio: DBIO[T],
    isolationLevel: TransactionIsolation = TransactionIsolation.RepeatableRead
  ): Future[T] =
    database.run(dbio.transactionally.withTransactionIsolation(isolationLevel))

  def inTransaction[T](
    dbio: DBIO[T],
    isolationLevel: TransactionIsolation = TransactionIsolation.RepeatableRead
  ): F[T] =
    concurrentDbAccessPermits.withPermit(
      blocker.blockOn(Async.fromFuture(Async[F].delay(inTransactionFuture(dbio, isolationLevel))))
    )

  private[db] def close(): Unit =
    database.close()
}

final class DataAccess(blocker: Blocker) {
  implicit val executionContext = blocker.blockingContext

  def truncateAll(): DBIO[Int] =
    // important to keep the right order for referential integrity !
    // if table X has a Foreign Key to table Y, delete table X first
    TableQuery[LabelTable].delete andThen
      TableQuery[ClusterErrorTable].delete andThen
      TableQuery[InstanceTable].delete andThen
      TableQuery[ExtensionTable].delete andThen
      TableQuery[ClusterImageTable].delete andThen
      TableQuery[ScopeTable].delete andThen
      TableQuery[PatchTable].delete andThen
      TableQuery[ClusterTable].delete andThen
      RuntimeConfigQueries.runtimeConfigs.delete andThen
      TableQuery[ServiceTable].delete andThen
      TableQuery[AppErrorTable].delete andThen
      TableQuery[AppTable].delete andThen
      TableQuery[NamespaceTable].delete andThen
      TableQuery[NodepoolTable].delete andThen
      TableQuery[KubernetesClusterTable].delete andThen
      TableQuery[PersistentDiskTable].delete

  def sqlDBStatus() =
    sql"select version()".as[String]
}

final class DBIOOps[A](private val dbio: DBIO[A]) extends AnyVal {
  def transaction[F[_]](implicit dbRef: DbReference[F]): F[A] = dbRef.inTransaction(dbio)
  def transaction[F[_]](isolationLevel: TransactionIsolation = TransactionIsolation.RepeatableRead)(
    implicit dbRef: DbReference[F]
  ): F[A] = dbRef.inTransaction(dbio, isolationLevel)
}

case class LiquibaseConfig(changelog: String, initWithLiquibase: Boolean)
