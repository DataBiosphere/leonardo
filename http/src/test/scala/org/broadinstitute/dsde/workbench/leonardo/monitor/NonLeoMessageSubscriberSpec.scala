package org.broadinstitute.dsde.workbench.leonardo
package monitor

import cats.effect.IO
import cats.mtl.Ask
import com.google.cloud.compute.v1.Instance
import io.circe.parser.decode
import org.broadinstitute.dsde.workbench.google2.mock.FakeGoogleComputeService
import org.broadinstitute.dsde.workbench.google2.{GoogleComputeService, InstanceName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.makeCluster
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.makeKubeCluster
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, kubernetesClusterQuery, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.broadinstitute.dsde.workbench.leonardo.monitor.NonLeoMessage.DeleteKubernetesClusterMessage
import org.broadinstitute.dsde.workbench.leonardo.monitor.NonLeoMessageSubscriber.nonLeoMessageDecoder
import org.broadinstitute.dsde.workbench.leonardo.util.GKEAlgebra
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.scalatest.flatspec.AnyFlatSpec

import scala.concurrent.ExecutionContext.Implicits.global

class NonLeoMessageSubscriberSpec extends AnyFlatSpec with LeonardoTestSuite with TestComponent {
  it should "decode NonLeoMessage properly" in {
    val jsonString =
      """
        |{
        |  "insertId": "1b6nno4f2ybl2l",
        |  "logName": "projects/general-dev-billing-account/logs/cryptomining",
        |  "receiveTimestamp": "2020-11-13T17:43:14.851633055Z",
        |  "resource": {
        |    "labels": {
        |      "instance_id": "715447017152936528",
        |      "project_id": "general-dev-billing-account",
        |      "zone": "us-central1-a"
        |    },
        |    "type": "gce_instance"
        |  },
        |  "severity": "ERROR",
        |  "textPayload": "CRYPTOMINING_DETECTED\n",
        |  "timestamp": "2020-11-13T17:43:15.135933929Z"
        |}
        |""".stripMargin
    val expectedResult = NonLeoMessage.CryptoMining(
      "CRYPTOMINING_DETECTED\n",
      GoogleResource(
        GoogleLabels(715447017152936528L, GoogleProject("general-dev-billing-account"), ZoneName("us-central1-a"))
      )
    )
    decode[NonLeoMessage](jsonString) shouldBe Right(expectedResult)

    val jsonStringDeleteKubernetesCluster =
      """
        |{
        | "clusterId": 1,
        | "project": "project1"
        |}
        |""".stripMargin
    val expectedResult2 = NonLeoMessage.DeleteKubernetesClusterMessage(
      KubernetesClusterLeoId(1),
      GoogleProject("project1")
    )
    decode[NonLeoMessage](jsonStringDeleteKubernetesCluster) shouldBe Right(expectedResult2)
  }

  it should "handle cryptomining message" in {
    for {
      runtime <- IO(makeCluster(1).save())
      computeService = new FakeGoogleComputeService {
        override def getInstance(project: GoogleProject, zone: ZoneName, instanceName: InstanceName)(
          implicit ev: Ask[IO, TraceId]
        ): cats.effect.IO[scala.Option[com.google.cloud.compute.v1.Instance]] =
          IO.pure(Some(Instance.newBuilder().setName(runtime.runtimeName.asString).build()))
      }
      subscriber = makeSubscribler(computeService = computeService)
      _ <- subscriber.handleCryptoMiningMessage(
        NonLeoMessage
          .CryptoMining("CRYPTOMINING_DETECTED",
                        GoogleResource(GoogleLabels(123L, runtime.googleProject, ZoneName("us-central1-a"))))
      )
      statusAfterUpdate <- clusterQuery.getClusterStatus(runtime.id).transaction
      deletedFrom <- clusterQuery.getDeletedFrom(runtime.id).transaction
    } yield {
      statusAfterUpdate.get shouldBe (RuntimeStatus.Deleted)
      deletedFrom.get shouldBe ("cryptomining")
    }
  }

  it should "handle DeleteKubernetesClusterMessage" in isolatedDbTest {
    val subscriber = makeSubscribler()

    val res = for {
      savedCluster <- IO(makeKubeCluster(1).save())
      msg = DeleteKubernetesClusterMessage(
        savedCluster.id,
        savedCluster.googleProject
      )
      _ <- subscriber.handleDeleteKubernetesClusterMessage(msg)
      clusterOpt <- kubernetesClusterQuery.getMinimalClusterById(savedCluster.id).transaction
    } yield {
      clusterOpt.get.status shouldBe KubernetesClusterStatus.Deleting
    }

    res.unsafeRunSync()
  }

  def makeSubscribler(
    gkeInterp: GKEAlgebra[IO] = new MockGKEService,
    computeService: GoogleComputeService[IO] = FakeGoogleComputeService
  ): NonLeoMessageSubscriber[IO] = {
    val googleSubscriber = new FakeGoogleSubcriber[NonLeoMessage]
    new NonLeoMessageSubscriber(gkeInterp, computeService, googleSubscriber)
  }
}
