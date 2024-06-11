package org.broadinstitute.dsde.workbench.leonardo
package monitor

import cats.effect.IO
import cats.effect.std.Queue
import cats.mtl.Ask
import com.google.cloud.compute.v1.Instance
import io.circe.parser.decode
import io.circe.{CursorOp, DecodingFailure}
import org.broadinstitute.dsde.workbench.google2.mock.FakeGoogleComputeService
import org.broadinstitute.dsde.workbench.google2.{GoogleComputeService, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.Task
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.{makeCluster, traceId}
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.{makeKubeCluster, makeNodepool}
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao.{MockSamDAO, SamDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, kubernetesClusterQuery, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.broadinstitute.dsde.workbench.leonardo.monitor.NonLeoMessage.{
  DeleteKubernetesClusterMessage,
  DeleteNodepoolMessage
}
import org.broadinstitute.dsde.workbench.leonardo.monitor.NonLeoMessageSubscriber.nonLeoMessageDecoder
import org.broadinstitute.dsde.workbench.leonardo.util.GKEAlgebra
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.util2.InstanceName
import org.broadinstitute.dsde.workbench.util2.messaging.CloudPublisher
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar
import org.broadinstitute.dsde.workbench.leonardo.FakeGoogleSubcriber

import scala.concurrent.ExecutionContext.Implicits.global

class NonLeoMessageSubscriberSpec extends AnyFlatSpec with LeonardoTestSuite with TestComponent with MockitoSugar {
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
        GoogleLabels(715447017152936528L, ZoneName("us-central1-a"))
      ),
      GoogleProject("general-dev-billing-account")
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

    val jsonStringDeleteNodepool =
      """
        |{
        | "messageType": "deleteNodepool",
        | "nodepoolId": 1,
        | "googleProject": "project1",
        | "traceId": "test"
        |}
        |""".stripMargin
    val expectedResultDeleteNodepool = NonLeoMessage.DeleteNodepoolMessage(
      NodepoolLeoId(1),
      GoogleProject("project1"),
      Some(TraceId("test"))
    )
    decode[NonLeoMessage](jsonStringDeleteNodepool) shouldBe Right(expectedResultDeleteNodepool)
  }

  it should "decode NonLeoMessage.CryptominingScc properly" in {
    val jsonString =
      """
        |{
        |  "notificationConfigName": "organizations/386193000800/notificationConfigs/leonardo-crypto-test",
        |  "finding": {
        |    "name": "organizations/386193000800/sources/6119898994322174789/findings/f3aec71664673d61db90d8efb99e7bac",
        |    "parent": "organizations/386193000800/sources/6119898994322174789",
        |    "resourceName": "//compute.googleapis.com/projects/terra-2d61a51b/zones/us-central1-a/instances/5289438569693667937",
        |    "state": "ACTIVE",
        |    "category": "Execution: Cryptocurrency Mining Combined Detection",
        |    "sourceProperties": {
        |      "threats": [{
        |        "memory_hash_detector": {
        |          "detections": [{
        |            "percent_pages_matched": 0.23588924387646432,
        |            "binary_name": "linux-x86-64_xmrig_6.15.1"
        |          }, {
        |            "binary_name": "linux-x86-64_xmrig_6.15.2",
        |            "percent_pages_matched": 0.7875399361022364
        |          }],
        |          "binary": "XMRig"
        |        }
        |      }, {
        |        "yara_rule_detector": {
        |          "yara_rule_name": "YARA_RULE1"
        |        }
        |      }, {
        |        "yara_rule_detector": {
        |          "yara_rule_name": "YARA_RULE9"
        |        }
        |      }, {
        |        "yara_rule_detector": {
        |          "yara_rule_name": "YARA_RULE10"
        |        }
        |      }, {
        |        "yara_rule_detector": {
        |          "yara_rule_name": "DYNAMIC_YARA_RULE_BFGMINER_2"
        |        }
        |      }]
        |    },
        |    "securityMarks": {
        |      "name": "organizations/386193000800/sources/6119898994322174789/findings/f3aec71664673d61db90d8efb99e7bac/securityMarks"
        |    },
        |    "eventTime": "2021-10-28T17:34:28.678071760Z",
        |    "createTime": "2021-10-28T17:39:31.719Z",
        |    "severity": "HIGH",
        |    "canonicalName": "projects/1089695574439/sources/6119898994322174789/findings/f3aec71664673d61db90d8efb99e7bac",
        |    "findingClass": "THREAT"
        |  },
        |  "resource": {
        |    "name": "//compute.googleapis.com/projects/terra-2d61a51b/zones/us-central1-a/instances/5289438569693667937",
        |    "project": "//cloudresourcemanager.googleapis.com/projects/1089695574439",
        |    "projectDisplayName": "terra-2d61a51b",
        |    "parent": "//cloudresourcemanager.googleapis.com/projects/1089695574439",
        |    "parentDisplayName": "terra-2d61a51b",
        |    "type": "google.compute.Instance",
        |    "folders": [{
        |      "resourceFolder": "//cloudresourcemanager.googleapis.com/folders/710468670182",
        |      "resourceFolderDisplayName": "CommunityWorkbench"
        |    }, {
        |      "resourceFolder": "//cloudresourcemanager.googleapis.com/folders/617814117274",
        |      "resourceFolderDisplayName": "prod"
        |    }],
        |    "displayName": "saturn-5434a6f7-6739-4843-9b5e-4fa03fe51d76"
        |  }
        |}
        |""".stripMargin
    val expectedResult = NonLeoMessage.CryptoMiningScc(
      CryptoMiningSccResource(
        GoogleProject("terra-2d61a51b"),
        CloudService.GCE,
        RuntimeName("saturn-5434a6f7-6739-4843-9b5e-4fa03fe51d76"),
        ZoneName("us-central1-a")
      ),
      Finding(SccCategory("Execution: Cryptocurrency Mining Combined Detection"))
    )
    decode[NonLeoMessage](jsonString) shouldBe Right(expectedResult)
  }

  it should "ignore NonLeoMessage.CryptominingScc when category is not supported" in {
    val jsonString =
      """
        |{
        |  "notificationConfigName": "organizations/386193000800/notificationConfigs/leonardo-crypto-test",
        |  "finding": {
        |    "name": "organizations/386193000800/sources/6119898994322174789/findings/f3aec71664673d61db90d8efb99e7bac",
        |    "parent": "organizations/386193000800/sources/6119898994322174789",
        |    "resourceName": "//compute.googleapis.com/projects/terra-2d61a51b/zones/us-central1-a/instances/5289438569693667937",
        |    "state": "ACTIVE",
        |    "category": "Execution: wrong category",
        |    "sourceProperties": {
        |      "threats": [{
        |        "memory_hash_detector": {
        |          "detections": [{
        |            "percent_pages_matched": 0.23588924387646432,
        |            "binary_name": "linux-x86-64_xmrig_6.15.1"
        |          }, {
        |            "binary_name": "linux-x86-64_xmrig_6.15.2",
        |            "percent_pages_matched": 0.7875399361022364
        |          }],
        |          "binary": "XMRig"
        |        }
        |      }, {
        |        "yara_rule_detector": {
        |          "yara_rule_name": "YARA_RULE1"
        |        }
        |      }, {
        |        "yara_rule_detector": {
        |          "yara_rule_name": "YARA_RULE9"
        |        }
        |      }, {
        |        "yara_rule_detector": {
        |          "yara_rule_name": "YARA_RULE10"
        |        }
        |      }, {
        |        "yara_rule_detector": {
        |          "yara_rule_name": "DYNAMIC_YARA_RULE_BFGMINER_2"
        |        }
        |      }]
        |    },
        |    "securityMarks": {
        |      "name": "organizations/386193000800/sources/6119898994322174789/findings/f3aec71664673d61db90d8efb99e7bac/securityMarks"
        |    },
        |    "eventTime": "2021-10-28T17:34:28.678071760Z",
        |    "createTime": "2021-10-28T17:39:31.719Z",
        |    "severity": "HIGH",
        |    "canonicalName": "projects/1089695574439/sources/6119898994322174789/findings/f3aec71664673d61db90d8efb99e7bac",
        |    "findingClass": "THREAT"
        |  },
        |  "resource": {
        |    "name": "//compute.googleapis.com/projects/terra-2d61a51b/zones/us-central1-a/instances/5289438569693667937",
        |    "project": "//cloudresourcemanager.googleapis.com/projects/1089695574439",
        |    "projectDisplayName": "terra-2d61a51b",
        |    "parent": "//cloudresourcemanager.googleapis.com/projects/1089695574439",
        |    "parentDisplayName": "terra-2d61a51b",
        |    "type": "google.compute.Instance",
        |    "folders": [{
        |      "resourceFolder": "//cloudresourcemanager.googleapis.com/folders/710468670182",
        |      "resourceFolderDisplayName": "CommunityWorkbench"
        |    }, {
        |      "resourceFolder": "//cloudresourcemanager.googleapis.com/folders/617814117274",
        |      "resourceFolderDisplayName": "prod"
        |    }],
        |    "displayName": "saturn-5434a6f7-6739-4843-9b5e-4fa03fe51d76"
        |  }
        |}
        |""".stripMargin
    val expectedResult = Left(
      DecodingFailure("Unsupported SCC category Execution: wrong category",
                      List(CursorOp.DownField("category"), CursorOp.DownField("finding"))
      )
    )
    decode[NonLeoMessage](jsonString) shouldBe expectedResult
  }

  it should "handle cryptomining message" in isolatedDbTest {
    ioAssertion {
      for {
        runtime <- IO(makeCluster(1).save())
        computeService = new FakeGoogleComputeService {
          override def getInstance(project: GoogleProject, zone: ZoneName, instanceName: InstanceName)(implicit
            ev: Ask[IO, TraceId]
          ): cats.effect.IO[scala.Option[com.google.cloud.compute.v1.Instance]] =
            IO.pure(Some(Instance.newBuilder().setName(runtime.runtimeName.asString).build()))
        }
        subscriber = makeSubscribler(computeService = computeService)
        _ <- subscriber.handleCryptoMiningMessage(
          NonLeoMessage
            .CryptoMining("CRYPTOMINING_DETECTED",
                          GoogleResource(GoogleLabels(123L, ZoneName("us-central1-a"))),
                          GoogleProject(runtime.cloudContext.asString)
            )
        )
        statusAfterUpdate <- clusterQuery.getClusterStatus(runtime.id).transaction
        deletedFrom <- clusterQuery.getDeletedFrom(runtime.id).transaction
      } yield {
        statusAfterUpdate.get shouldBe (RuntimeStatus.Deleted)
        deletedFrom.get shouldBe "cryptomining: custom detector"
      }
    }
  }

  it should "handle cryptomining-scc message" in isolatedDbTest {
    ioAssertion {
      for {
        runtime <- IO(
          makeCluster(1)
            .copy(status = RuntimeStatus.Running)
            .saveWithRuntimeConfig(CommonTestData.defaultGceRuntimeConfig)
        )
        subscriber = makeSubscribler()
        _ <- subscriber.messageResponder(
          NonLeoMessage.CryptoMiningScc(
            CryptoMiningSccResource(GoogleProject(runtime.cloudContext.asString),
                                    CloudService.GCE,
                                    runtime.runtimeName,
                                    CommonTestData.defaultGceRuntimeConfig.zone
            ),
            Finding(SccCategory("Execution: Cryptocurrency Mining Combined Detection"))
          )
        )
        statusAfterUpdate <- clusterQuery.getClusterStatus(runtime.id).transaction
        deletedFrom <- clusterQuery.getDeletedFrom(runtime.id).transaction
      } yield {
        statusAfterUpdate.get shouldBe (RuntimeStatus.Deleted)
        deletedFrom.get shouldBe "cryptomining: scc"
      }
    }
  }

  it should "handle DeleteKubernetesClusterMessage" in isolatedDbTest {
    val subscriber = makeSubscribler()

    val res = for {
      savedCluster <- IO(makeKubeCluster(1).save())
      msg = DeleteKubernetesClusterMessage(
        savedCluster.id,
        savedCluster.cloudContext.asInstanceOf[CloudContext.Gcp].value
      )
      _ <- subscriber.handleDeleteKubernetesClusterMessage(msg)
      clusterOpt <- kubernetesClusterQuery.getMinimalClusterById(savedCluster.id).transaction
    } yield clusterOpt.get.status shouldBe KubernetesClusterStatus.Deleting

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle DeleteNodepoolMessage" in isolatedDbTest {
    val subscriber = makeSubscribler()

    val res = for {
      traceId <- traceId.ask[TraceId]
      savedCluster = makeKubeCluster(1).save()
      savedNodepool = makeNodepool(1, savedCluster.id).save()
      msg = DeleteNodepoolMessage(savedNodepool.id,
                                  savedCluster.cloudContext.asInstanceOf[CloudContext.Gcp].value,
                                  Some(traceId)
      )

      attempt <- subscriber.messageResponder(msg).attempt
    } yield attempt shouldBe Right(())

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  def makeSubscribler(
    gkeInterp: GKEAlgebra[IO] = new MockGKEService,
    samDao: SamDAO[IO] = new MockSamDAO,
    computeService: GoogleComputeService[IO] = FakeGoogleComputeService,
    publisher: CloudPublisher[IO] = mock[CloudPublisher[IO]],
    asyncTaskQueue: Queue[IO, Task[IO]] =
      Queue.bounded[IO, Task[IO]](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  ): NonLeoMessageSubscriber[IO] = {
    val googleSubscriber = new FakeGoogleSubcriber[NonLeoMessage]
    new NonLeoMessageSubscriber(
      NonLeoMessageSubscriberConfig(Config.gceConfig.userDiskDeviceName),
      gkeInterp,
      computeService,
      samDao,
      MockAuthProvider,
      googleSubscriber,
      publisher,
      asyncTaskQueue
    )
  }
}
