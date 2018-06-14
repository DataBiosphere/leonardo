package org.broadinstitute.dsde.workbench.leonardo.db

import java.util.UUID

import org.broadinstitute.dsde.workbench.leonardo.TestExecutionContext
import org.broadinstitute.dsde.workbench.leonardo.model.Cluster
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{Assertion, Matchers}
import slick.dbio.DBIO
import slick.jdbc.JdbcProfile

import scala.concurrent.ExecutionContext

trait TestComponent extends Matchers with ScalaFutures with LeoComponent {
  override val profile: JdbcProfile = DbSingleton.ref.dataAccess.profile
  override implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext
  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(10, Seconds)))

  def dbFutureValue[T](f: (DataAccess) => DBIO[T]): T = DbSingleton.ref.inTransaction(f).futureValue
  def dbFailure[T](f: (DataAccess) => DBIO[T]): Throwable = DbSingleton.ref.inTransaction(f).failed.futureValue

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

  // Equivalence means clusters have the same fields when ignoring the id field
  protected def assertEquivalent(cs1: Set[Cluster])(cs2: Set[Cluster]): Assertion = {
    val FixedId = 0

    val (cs1WithFixedId, cs2WithFixedId) = setClusterId(cs1, cs2, FixedId)

    cs1WithFixedId shouldEqual cs2WithFixedId
  }

  protected def assertEquivalent(c1: Cluster)(c2: Cluster): Assertion = {
    assertEquivalent(Set(c1))(Set(c2))
  }

  // Set all clusters' ids to the given value
  private[db] def setClusterId(cs1: Set[Cluster], cs2: Set[Cluster], fixedId: Long): (Set[Cluster], Set[Cluster]) = {
    val cs1WithFixedId = cs1 map { _.copy(id = fixedId) }
    val cs2WithFixedId = cs2 map { _.copy(id = fixedId) }

    (cs1WithFixedId, cs2WithFixedId)
  }
}
