package org.broadinstitute.dsde.workbench.leonardo.db

import java.time.Instant

import org.broadinstitute.dsde.workbench.leonardo.{GcsPathUtils, TestExecutionContext}
import org.broadinstitute.dsde.workbench.leonardo.model.Cluster
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.model.google.{GoogleProject, ServiceAccountKeyId}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Span}
import org.scalatest.Matchers
import slick.dbio.DBIO
import slick.jdbc.JdbcProfile

import scala.concurrent.ExecutionContext

trait TestComponent extends Matchers with ScalaFutures with LeoComponent with GcsPathUtils{
  override val profile: JdbcProfile = DbSingleton.ref.dataAccess.profile
  override implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext
  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(10, Seconds)))
  val defaultServiceAccountKeyId = ServiceAccountKeyId("123")

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

  protected def getClusterId(cluster: Cluster): Long = {
    getClusterId(cluster.googleProject, cluster.clusterName, cluster.auditInfo.destroyedDate)
  }

  protected def getClusterId(googleProject: GoogleProject,
                             clusterName: ClusterName,
                             destroyedDateOpt: Option[Instant]): Long = {
    dbFutureValue { _.clusterQuery.getIdByUniqueKey(googleProject, clusterName, destroyedDateOpt) }.get
  }

  implicit class ClusterExtensions(cluster: Cluster) {
    def save(serviceAccountKeyId: Option[ServiceAccountKeyId] = Some(defaultServiceAccountKeyId)) = {
      dbFutureValue { _.clusterQuery.save(cluster, Option(gcsPath("gs://bucket" + cluster.clusterName.toString().takeRight(1))), serviceAccountKeyId) }
    }
  }
}
