package org.broadinstitute.dsde.workbench.leonardo
package db

import _root_.liquibase.database.jvm.JdbcConnection
import _root_.liquibase.resource.{ClassLoaderResourceAccessor, ResourceAccessor}
import _root_.liquibase.Liquibase
import cats.effect.std.Semaphore
import cats.effect.{Async, Resource}
import cats.syntax.all._
import com.google.common.base.Throwables
import com.typesafe.scalalogging.LazyLogging
import _root_.liquibase.command.CommandScope
import liquibase.command.core.UpdateCommandStep
import org.broadinstitute.dsde.workbench.leonardo.config.LiquibaseConfig
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
import org.typelevel.log4cats.Logger
import slick.basic.DatabaseConfig
import slick.jdbc.{JdbcBackend, JdbcProfile, TransactionIsolation}
import sun.security.provider.certpath.SunCertPathBuilderException

import java.sql.{Connection, SQLTimeoutException}
import scala.concurrent.Future

object DbReference extends LazyLogging {

  private[db] def initWithLiquibase(dbConnection: Connection,
                                    liquibaseConfig: LiquibaseConfig,
                                    changelogParameters: Map[String, AnyRef] = Map.empty
  ): Unit =
    try {
      val liquibaseConnection = new JdbcConnection(dbConnection)
      val resourceAccessor: ResourceAccessor = new ClassLoaderResourceAccessor()
      val liquibase = new Liquibase(liquibaseConfig.changelog, resourceAccessor, liquibaseConnection)

      changelogParameters.foreach { case (key, value) => liquibase.setChangeLogParameter(key, value) }

      val updateCommand = new CommandScope("update")
        .addArgumentValue(UpdateCommandStep.CHANGELOG_ARG.getName, liquibase.getChangeLogFile)
      updateCommand.execute()

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

  def init[F[_]: Async: Logger](config: LiquibaseConfig,
                                concurrentDbAccessPermits: Semaphore[F]
  ): Resource[F, DbReference[F]] = {
    val dbConfig =
      DatabaseConfig.forConfig[JdbcProfile]("mysql", org.broadinstitute.dsde.workbench.leonardo.config.Config.config)

    for {
      db <- Resource.make(Async[F].delay(dbConfig.db))(db => Async[F].delay(db.close()))
      initLiquibase =
        if (config.initWithLiquibase)
          Resource
            .make(Async[F].delay(db.source.createConnection()))(conn => Async[F].delay(conn.close()))
            .use(dbConn =>
              Async[F].delay(initWithLiquibase(dbConn, config)) >> Logger[F].info("Applied liquidbase changelog")
            )
        else Async[F].unit
      _ <- Resource.eval(initLiquibase)
    } yield new DbRef[F](db, concurrentDbAccessPermits)
  }
}

trait DbReference[F[_]] {
  def inTransaction[T](
    dbio: DBIO[T],
    isolationLevel: TransactionIsolation = TransactionIsolation.RepeatableRead
  ): F[T]
}

private[db] class DbRef[F[_]](database: JdbcBackend#DatabaseDef, concurrentDbAccessPermits: Semaphore[F])(implicit
  F: Async[F]
) extends DbReference[F] {
  import LeoProfile.api._

  val dataAccess = DataAccess

  private def inTransactionFuture[T](
    dbio: DBIO[T],
    isolationLevel: TransactionIsolation = TransactionIsolation.RepeatableRead
  ): Future[T] =
    database.run(dbio.transactionally.withTransactionIsolation(isolationLevel))

  def inTransaction[T](
    dbio: DBIO[T],
    isolationLevel: TransactionIsolation = TransactionIsolation.RepeatableRead
  ): F[T] =
    concurrentDbAccessPermits.permit.use(_ => F.fromFuture(F.delay(inTransactionFuture(dbio, isolationLevel))))

  private[db] def close(): Unit =
    database.close()
}

object DataAccess {
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
      TableQuery[RuntimeControlledResourceTable].delete andThen
      TableQuery[AppControlledResourceTable].delete andThen
      TableQuery[ClusterTable].delete andThen
      RuntimeConfigQueries.runtimeConfigs.delete andThen
      persistentDiskQuery.nullifyDiskIds andThen
      TableQuery[ServiceTable].delete andThen
      TableQuery[UpdateAppLogTable].delete andThen
      TableQuery[AppErrorTable].delete andThen
      TableQuery[AppUsageTable].delete andThen
      TableQuery[AppTable].delete andThen
      TableQuery[NodepoolTable].delete andThen
      TableQuery[KubernetesClusterTable].delete andThen
      persistentDiskQuery.tableQuery.delete

  def sqlDBStatus() =
    sql"select version()".as[String]
}

final class DBIOOps[A](private val dbio: DBIO[A]) extends AnyVal {
  def transaction[F[_]](implicit dbRef: DbReference[F]): F[A] = dbRef.inTransaction(dbio)
  def transaction[F[_]](isolationLevel: TransactionIsolation = TransactionIsolation.RepeatableRead)(implicit
    dbRef: DbReference[F]
  ): F[A] = dbRef.inTransaction(dbio, isolationLevel)
}
