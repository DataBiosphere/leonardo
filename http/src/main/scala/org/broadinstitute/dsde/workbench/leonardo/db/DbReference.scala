package org.broadinstitute.dsde.workbench.leonardo
package db

import java.sql.{Connection, SQLTimeoutException}

import cats.effect.concurrent.Semaphore
import cats.effect.{Async, Blocker, ContextShift, Resource}
import com.google.common.base.Throwables
import com.typesafe.scalalogging.LazyLogging
import _root_.liquibase.database.jvm.JdbcConnection
import _root_.liquibase.resource.{ClassLoaderResourceAccessor, ResourceAccessor}
import _root_.liquibase.{Contexts, Liquibase}
import org.broadinstitute.dsde.workbench.leonardo.config.LiquibaseConfig
import slick.basic.DatabaseConfig
import slick.jdbc.{JdbcBackend, JdbcProfile, TransactionIsolation}
import sun.security.provider.certpath.SunCertPathBuilderException
import LeoProfile.api._
import io.chrisdavenport.log4cats.Logger
import cats.implicits._

import scala.concurrent.Future

object DbReference extends LazyLogging {

  private[db] def initWithLiquibase(dbConnection: Connection,
                                    liquibaseConfig: LiquibaseConfig,
                                    changelogParameters: Map[String, AnyRef] = Map.empty): Unit =
    try {
      val liquibaseConnection = new JdbcConnection(dbConnection)
      val resourceAccessor: ResourceAccessor = new ClassLoaderResourceAccessor()
      val liquibase = new Liquibase(liquibaseConfig.changelog, resourceAccessor, liquibaseConnection)

      changelogParameters.foreach { case (key, value) => liquibase.setChangeLogParameter(key, value) }
      liquibase.update(new Contexts())
    } catch {
      case e: SQLTimeoutException =>
        val isCertProblem = Throwables.getRootCause(e).isInstanceOf[SunCertPathBuilderException]
        if (isCertProblem) {
          val k = "javax.net.ssl.keyStore"
          if (System.getProperty(k) == null) {
            logger.warn("************")
            logger.warn(
              s"The system property '${k}' is null. This is likely the cause of the database connection failure."
            )
            logger.warn("************")
          }
        }
        throw e
    }

  def init[F[_]: Async: ContextShift: Logger](config: LiquibaseConfig,
                                              concurrentDbAccessPermits: Semaphore[F],
                                              blocker: Blocker): Resource[F, DbReference[F]] = {
    val dbConfig =
      DatabaseConfig.forConfig[JdbcProfile]("mysql", org.broadinstitute.dsde.workbench.leonardo.config.Config.config)

    for {
      db <- Resource.make(Async[F].delay(dbConfig.db))(db => Async[F].delay(db.close()))
      dbConnection <- Resource.make(Async[F].delay(db.source.createConnection()))(conn => Async[F].delay(conn.close()))
      initLiquibase = if (config.initWithLiquibase)
        Async[F].delay(initWithLiquibase(dbConnection, config)) >> Logger[F].info("Applied liquidbase changelog")
      else Async[F].unit
      _ <- Resource.liftF(initLiquibase)
    } yield new DbRef[F](dbConfig, db, concurrentDbAccessPermits, blocker)
  }
}

trait DbReference[F[_]] {
  def dataAccess: DataAccess
  def inTransaction[T](
    dbio: DBIO[T],
    isolationLevel: TransactionIsolation = TransactionIsolation.RepeatableRead
  ): F[T]
}

private[db] class DbRef[F[_]: Async: ContextShift](dbConfig: DatabaseConfig[JdbcProfile],
                                                   database: JdbcBackend#DatabaseDef,
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
      RuntimeConfigQueries.runtimeConfigs.delete andThen
      TableQuery[PersistentDiskLabelTable].delete andThen
      TableQuery[PersistentDiskTable].delete andThen
      TableQuery[ClusterTable].delete

  def sqlDBStatus() =
    sql"select version()".as[String]
}

final class DBIOOps[A](private val dbio: DBIO[A]) extends AnyVal {
  def transaction[F[_]](implicit dbRef: DbReference[F]): F[A] = dbRef.inTransaction(dbio)
  def transaction[F[_]](isolationLevel: TransactionIsolation = TransactionIsolation.RepeatableRead)(
    implicit dbRef: DbReference[F]
  ): F[A] = dbRef.inTransaction(dbio, isolationLevel)
}
