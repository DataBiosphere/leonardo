package org.broadinstitute.dsde.workbench.leonardo.db

import java.time.Instant

import org.broadinstitute.dsde.workbench.leonardo.model.Cluster
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.leonardo.{GcsPathUtils, LeonardoTestSuite}
import org.broadinstitute.dsde.workbench.model.google.{GoogleProject, ServiceAccountKeyId}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Span}
import slick.dbio.DBIO
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.duration._

trait TestComponent extends LeonardoTestSuite with ScalaFutures with GcsPathUtils {
  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(10, Seconds)))

  val defaultServiceAccountKeyId = ServiceAccountKeyId("123")

  implicit val dbRef = DbSingleton.dbRef

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

  protected def getClusterId(cluster: Cluster): Long =
    getClusterId(cluster.googleProject, cluster.clusterName, cluster.auditInfo.destroyedDate)

  protected def getClusterId(googleProject: GoogleProject,
                             clusterName: ClusterName,
                             destroyedDateOpt: Option[Instant]): Long =
    dbFutureValue { clusterQuery.getIdByUniqueKey(googleProject, clusterName, destroyedDateOpt) }.get

  implicit class ClusterExtensions(cluster: Cluster) {
    def save(serviceAccountKeyId: Option[ServiceAccountKeyId] = Some(defaultServiceAccountKeyId)): Cluster =
      dbFutureValue {
        clusterQuery.save(cluster,
                          Option(gcsPath("gs://bucket" + cluster.clusterName.toString().takeRight(1))),
                          serviceAccountKeyId)
      }
  }
}
