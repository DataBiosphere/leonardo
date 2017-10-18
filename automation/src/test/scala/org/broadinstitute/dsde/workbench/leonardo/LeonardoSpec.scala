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

    val project = GoogleProject(LeonardoConfig.Projects.default)
    val sa = GoogleServiceAccount(LeonardoConfig.Leonardo.notebooksServiceAccountEmail)

    "should create, monitor, and delete a cluster" in {
      val name = ClusterName(s"automation-test-a${Random.alphanumeric.take(10).mkString.toLowerCase}z")
      val bucket = GcsBucketName("mah-bukkit")

      val requestLabels = Map("foo" -> Random.alphanumeric.take(10).mkString)
      val responseLabels = requestLabels ++ DefaultLabels(name, project, bucket, sa, None).toMap

      val request = ClusterRequest(bucket, sa, requestLabels, None)

      def clusterCheck(cluster: Cluster, statuses: Iterable[ClusterStatus]): Unit = {
        cluster.googleProject shouldBe project
        cluster.clusterName shouldBe name
        cluster.googleServiceAccount shouldBe sa
        cluster.googleBucket shouldBe bucket
        cluster.labels shouldBe responseLabels
        statuses should contain (cluster.status)
      }

      val cluster = Leonardo.cluster.create(project, name, request)
      clusterCheck(cluster, Seq(ClusterStatus.Creating))

      // verify with get()
      clusterCheck(Leonardo.cluster.get(project, name), Seq(ClusterStatus.Creating))

      // wait for "Running" or error (fail fast)
      eventually {
        clusterCheck(Leonardo.cluster.get(project, name), Seq(ClusterStatus.Running, ClusterStatus.Error))
      } (PatienceConfig(timeout = scaled(Span(6, Minutes)), interval = scaled(Span(20, Seconds))))

      clusterCheck(Leonardo.cluster.get(project, name), Seq(ClusterStatus.Running))

      Leonardo.cluster.delete(project, name) shouldBe "The request has been accepted for processing, but the processing has not been completed."
    }
  }
}
