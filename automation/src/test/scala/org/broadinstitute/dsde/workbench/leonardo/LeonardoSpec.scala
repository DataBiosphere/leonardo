package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.config.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.ClusterStatus.ClusterStatus
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Minutes, Seconds, Span}
import org.scalatest.{FreeSpec, Matchers}

import scala.util.Random

class LeonardoSpec extends FreeSpec with Matchers with Eventually {
  implicit val ronAuthToken = AuthToken(LeonardoConfig.Users.ron)

  "Leonardo" - {
    "should ping" in {
      Leonardo.test.ping() shouldBe "OK"
    }

    "should return an empty cluster list" in {
      Leonardo.cluster.list() shouldBe "[]"
    }

    val project = GoogleProject(LeonardoConfig.Projects.default)
    val sa = GoogleServiceAccount(LeonardoConfig.Leonardo.notebooksServiceAccountEmail)

    "should create, monitor, and delete a cluster" in {
      val name = ClusterName(s"automation-test-a${Random.alphanumeric.take(10).mkString.toLowerCase}z")
      val bucket = GcsBucketName("mah-bukkit")
      val labels = Map("foo" -> Random.alphanumeric.take(10).mkString)

      val request = ClusterRequest(bucket, sa, labels, None)

      def clusterCheck(cluster: Cluster, status: ClusterStatus): Unit = {
        cluster.googleProject shouldBe project
        cluster.clusterName shouldBe name
        cluster.googleServiceAccount shouldBe sa
        cluster.googleBucket shouldBe bucket
        cluster.labels shouldBe labels
        cluster.status shouldBe status
      }

      val cluster = Leonardo.cluster.create(project, name, request)
      clusterCheck(cluster, ClusterStatus.Creating)

      // verify with get()
      clusterCheck(Leonardo.cluster.get(project, name), ClusterStatus.Creating)

      // wait for "Running"
      eventually {
        clusterCheck(Leonardo.cluster.get(project, name), ClusterStatus.Running)
      } (PatienceConfig(timeout = scaled(Span(6, Minutes)), interval = scaled(Span(20, Seconds))))

      Leonardo.cluster.delete(project, name) shouldBe "The request has been accepted for processing, but the processing has not been completed."

      // verify with get()
      clusterCheck(Leonardo.cluster.get(project, name), ClusterStatus.Deleting)
    }
  }
}
