package org.broadinstitute.dsde.workbench.leonardo.db

import java.util.UUID

import org.broadinstitute.dsde.workbench.leonardo.TestExecutionContext
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Span}
import org.scalatest.Matchers
import slick.dbio.DBIO
import slick.jdbc.JdbcProfile

import scala.concurrent.ExecutionContext

trait TestComponent extends Matchers with ScalaFutures with LeoComponent {
  override val profile: JdbcProfile = DbSingleton.ref.dataAccess.profile
  override implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext
  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(10, Seconds)))

  def dbFutureValue[T](f: DataAccess => DBIO[T]): T = DbSingleton.ref.inTransaction(f).futureValue
  def dbFailure[T](f: DataAccess => DBIO[T]): Throwable = DbSingleton.ref.inTransaction(f).failed.futureValue

  // clean up after tests
  def isolatedDbTest[T](testCode: => T): T = {
    try {
      dbFutureValue { _ => DbSingleton.ref.dataAccess.truncateAll() }
      testCode
    } catch {
      case t: Throwable => t.printStackTrace(); throw t
    } finally {
      dbFutureValue { _ => DbSingleton.ref.dataAccess.truncateAll() }
    }
  }

  protected def getClusterId(googleId: Option[UUID]): Long = {
    dbFutureValue { _.clusterQuery.getIdByGoogleId(googleId) }.get
  }
//
//  protected def getClusterId(cluster: Cluster): Long = {
//    getClusterId(cluster.googleProject, cluster.clusterName, cluster.destroyedDate)
//  }
//
//  protected def getClusterId(googleProject: GoogleProject,
//                             clusterName: ClusterName,
//                             destroyedDateOpt: Option[Instant]): Long = {
//    dbFutureValue { _.clusterQuery.getIdByUniqueKey(googleProject, clusterName, destroyedDateOpt) }.get
//  }
}
