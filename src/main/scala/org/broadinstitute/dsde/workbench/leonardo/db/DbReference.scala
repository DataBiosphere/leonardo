package org.broadinstitute.dsde.workbench.leonardo.db

import java.sql.SQLTimeoutException

import com.google.common.base.Throwables
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import liquibase.{Contexts, Liquibase}
import liquibase.database.jvm.JdbcConnection
import liquibase.resource.{ClassLoaderResourceAccessor, ResourceAccessor}
import org.broadinstitute.dsde.workbench.leonardo.config.LiquibaseConfig
import slick.basic.DatabaseConfig
import slick.dbio.DBIO
import slick.jdbc.{JdbcBackend, JdbcDataSource, JdbcProfile, TransactionIsolation}
import net.ceedubs.ficus.Ficus._
import sun.security.provider.certpath.SunCertPathBuilderException

import scala.concurrent.{ExecutionContext, Future}

object DbReference extends LazyLogging {

  private def initWithLiquibase(dataSource: JdbcDataSource, liquibaseConfig: LiquibaseConfig, changelogParameters: Map[String, AnyRef] = Map.empty): Unit = {
    val dbConnection = dataSource.createConnection()
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
            logger.warn(s"The system property '${k}' is null. This is likely the cause of the database connection failure.")
            logger.warn("************")
          }
        }
        throw e
    } finally {
      dbConnection.close()
    }
  }

  def init(config: Config)(implicit executionContext: ExecutionContext): DbReference = {
    val dbConfig = DatabaseConfig.forConfig[JdbcProfile]("mysql", config)

    val liquibaseConf = config.as[LiquibaseConfig]("liquibase")
    if (liquibaseConf.initWithLiquibase)
      initWithLiquibase(dbConfig.db.source, liquibaseConf)

    DbReference(dbConfig)
  }
}

case class DbReference(private val dbConfig: DatabaseConfig[JdbcProfile])(implicit val executionContext: ExecutionContext) {
  val dataAccess = new DataAccess(dbConfig.profile)
  val database: JdbcBackend#DatabaseDef = dbConfig.db

  def inTransaction[T](f: (DataAccess) => DBIO[T], isolationLevel: TransactionIsolation = TransactionIsolation.RepeatableRead): Future[T] = {
    import dataAccess.profile.api._
    database.run(f(dataAccess).transactionally.withTransactionIsolation(isolationLevel))
  }
}

class DataAccess(val profile: JdbcProfile)(implicit val executionContext: ExecutionContext) extends AllComponents {

  def truncateAll(): DBIO[Int] = {
    import profile.api._

    // important to keep the right order for referential integrity !
    // if table X has a Foreign Key to table Y, delete table X first
    TableQuery[LabelTable].delete andThen TableQuery[ClusterTable].delete
  }
}
