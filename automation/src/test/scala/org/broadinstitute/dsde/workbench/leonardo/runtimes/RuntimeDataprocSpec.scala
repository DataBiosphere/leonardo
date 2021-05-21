package org.broadinstitute.dsde.workbench.leonardo
package runtimes

import cats.data.NonEmptyList
import cats.effect.IO
import cats.mtl.Ask
import cats.syntax.all._
import com.google.cloud.Identity
import org.broadinstitute.dsde.workbench.google2.DataprocRole.{Master, SecondaryWorker, Worker}
import org.broadinstitute.dsde.workbench.google2.{
  DataprocClusterName,
  GcsBlobName,
  GoogleDataprocInterpreter,
  GoogleDataprocService,
  GoogleStorageService,
  MachineTypeName,
  RegionName,
  StorageRole
}
import org.broadinstitute.dsde.workbench.leonardo.LeonardoApiClient.defaultCreateRuntime2Request
import org.broadinstitute.dsde.workbench.leonardo.RuntimeConfig.DataprocConfig
import org.broadinstitute.dsde.workbench.leonardo.http.RuntimeConfigRequest
import org.broadinstitute.dsde.workbench.leonardo.notebooks.{NotebookTestUtils, Python3}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsObjectName, GcsPath, GoogleProject}
import org.broadinstitute.dsde.workbench.service.Sam
import org.http4s.client.Client
import org.http4s.headers.Authorization
import org.http4s.{AuthScheme, Credentials}
import org.scalatest.{DoNotDiscover, ParallelTestExecution}
import java.nio.charset.{Charset, StandardCharsets}
import java.util.UUID
import org.scalatest.tagobjects.Retryable

@DoNotDiscover
class RuntimeDataprocSpec
    extends GPAllocFixtureSpec
    with ParallelTestExecution
    with LeonardoTestUtils
    with NotebookTestUtils {
  implicit val authTokenForOldApiClient = ronAuthToken
  implicit val auth: Authorization = Authorization(Credentials.Token(AuthScheme.Bearer, ronCreds.makeAuthToken().value))
  implicit val traceId = Ask.const[IO, TraceId](TraceId(UUID.randomUUID()))

  override def withFixture(test: NoArgTest) =
    if (isRetryable(test))
      withRetry(super.withFixture(test))
    else
      super.withFixture(test)

  val dependencies = for {
    dataprocService <- googleDataprocService
    storage <- google2StorageResource
    httpClient <- LeonardoApiClient.client
  } yield RuntimeDataprocSpecDependencies(httpClient, dataprocService, storage)

  "should create a Dataproc cluster in a non-default region" taggedAs Retryable in { project =>
    val runtimeName = randomClusterName

    // In a europe region
    val createRuntimeRequest = defaultCreateRuntime2Request.copy(
      runtimeConfig = Some(
        RuntimeConfigRequest.DataprocConfig(
          Some(2),
          Some(MachineTypeName("n1-standard-4")),
          Some(DiskSize(100)),
          Some(MachineTypeName("n1-standard-4")),
          Some(DiskSize(100)),
          None,
          Some(1),
          Map.empty,
          Some(RegionName("europe-west1"))
        )
      ),
      toolDockerImage = Some(ContainerImage(LeonardoConfig.Leonardo.hailImageUrl, ContainerRegistry.GCR))
    )

    val res = dependencies.use { dep =>
      implicit val client = dep.httpClient
      for {
        // create runtime
        getRuntimeResponse <- LeonardoApiClient.createRuntimeWithWait(project, runtimeName, createRuntimeRequest)
        runtime = ClusterCopy.fromGetRuntimeResponseCopy(getRuntimeResponse)

        // check cluster status in Dataproc
        _ <- verifyDataproc(project, runtime.clusterName, dep.dataproc, 2, 1, RegionName("europe-west1"))
        _ = getRuntimeResponse.runtimeConfig.asInstanceOf[DataprocConfig].region shouldBe RegionName("europe-west1")

        _ <- LeonardoApiClient.deleteRuntime(project, runtimeName)
      } yield ()
    }

    res.unsafeRunSync()
  }

  "should create a Dataproc cluster with workers and preemptible workers" taggedAs Retryable in { project =>
    val runtimeName = randomClusterName

    // 2 workers and 5 preemptible workers
    val createRuntimeRequest = defaultCreateRuntime2Request.copy(
      runtimeConfig = Some(
        RuntimeConfigRequest.DataprocConfig(
          Some(2),
          Some(MachineTypeName("n1-standard-4")),
          Some(DiskSize(100)),
          Some(MachineTypeName("n1-standard-4")),
          Some(DiskSize(100)),
          None,
          Some(5),
          Map.empty
        )
      ),
      toolDockerImage = Some(ContainerImage(LeonardoConfig.Leonardo.hailImageUrl, ContainerRegistry.GCR))
    )

    val res = dependencies.use { dep =>
      implicit val client = dep.httpClient
      for {
        // create runtime
        getRuntimeResponse <- LeonardoApiClient.createRuntimeWithWait(project, runtimeName, createRuntimeRequest)
        runtime = ClusterCopy.fromGetRuntimeResponseCopy(getRuntimeResponse)

        // check cluster status in Dataproc
        _ <- verifyDataproc(project, runtime.clusterName, dep.dataproc, 2, 5, RegionName("us-central1"))

        // check output of yarn node -list command
        _ <- IO(
          withWebDriver { implicit driver =>
            withNewNotebook(runtime, Python3) { notebookPage =>
              val output = notebookPage.executeCell("""!yarn node -list""")
              output.get should include("Total Nodes:")
            }
          }
        )

        _ <- LeonardoApiClient.deleteRuntime(project, runtimeName)
      } yield ()
    }

    res.unsafeRunSync()
  }

  "should stop/start a Dataproc cluster with workers and preemptible workers" taggedAs Retryable in { project =>
    val runtimeName = randomClusterName

    val res = dependencies.use { dep =>
      implicit val client = dep.httpClient
      for {
        // Set up test bucket for startup script
        petSA <- IO(Sam.user.petServiceAccountEmail(project.value))
        bucketName <- IO(UUID.randomUUID()).map(u => GcsBucketName(s"leo-test-bucket-${u.toString}"))
        userScriptObjectName = GcsBlobName("test-user-script.sh")
        userStartScriptObjectName = GcsBlobName("test-user-start-script.sh")
        _ <- dep.storage.insertBucket(project, bucketName).compile.drain
        _ <- dep.storage
          .setIamPolicy(
            bucketName,
            Map(StorageRole.ObjectViewer -> NonEmptyList.one(Identity.serviceAccount(petSA.value)))
          )
          .compile
          .drain
        userScriptString = "#!/usr/bin/env bash\n\necho 'This is a user script'"
        userStartScriptString = "#!/usr/bin/env bash\n\necho 'This is a start user script'"
        _ <- fs2.Stream
          .emits(userScriptString.getBytes(Charset.forName("UTF-8")))
          .covary[IO]
          .through(dep.storage.streamUploadBlob(bucketName, userScriptObjectName))
          .compile
          .drain
        _ <- fs2.Stream
          .emits(userStartScriptString.getBytes(Charset.forName("UTF-8")))
          .covary[IO]
          .through(dep.storage.streamUploadBlob(bucketName, userStartScriptObjectName))
          .compile
          .drain

        // create runtime
        // 2 workers and 5 preemptible workers
        createRuntimeRequest = defaultCreateRuntime2Request.copy(
          userScriptUri = Some(UserScriptPath.Gcs(GcsPath(bucketName, GcsObjectName(userScriptObjectName.value)))),
          startUserScriptUri =
            Some(UserScriptPath.Gcs(GcsPath(bucketName, GcsObjectName(userStartScriptObjectName.value)))),
          runtimeConfig = Some(
            RuntimeConfigRequest.DataprocConfig(
              Some(2),
              Some(MachineTypeName("n1-standard-4")),
              Some(DiskSize(100)),
              Some(MachineTypeName("n1-standard-4")),
              Some(DiskSize(100)),
              None,
              Some(5),
              Map.empty
            )
          ),
          toolDockerImage = Some(ContainerImage(LeonardoConfig.Leonardo.hailImageUrl, ContainerRegistry.GCR))
        )
        getRuntimeResponse <- LeonardoApiClient.createRuntimeWithWait(project, runtimeName, createRuntimeRequest)
        runtime = ClusterCopy.fromGetRuntimeResponseCopy(getRuntimeResponse)

        // check cluster status in Dataproc
        _ <- verifyDataproc(project, runtime.clusterName, dep.dataproc, 2, 5, RegionName("us-central1"))

        // verify user scripts were executed
        userScriptOutput <- dep.storage
          .getBlobBody(runtime.stagingBucket.get, GcsBlobName("userscript_output.txt"))
          .compile
          .toList
          .map(bytes => new String(bytes.toArray, StandardCharsets.UTF_8))

        _ = userScriptOutput.trim shouldBe "This is a user script"

        startScriptOutputs <- dep.storage
          .listBlobsWithPrefix(
            runtime.stagingBucket.get,
            "startscript_output",
            true
          )
          .evalMap(blob =>
            dep.storage
              .getBlobBody(runtime.stagingBucket.get, GcsBlobName(blob.getName))
              .compile
              .toList
              .map(bytes => new String(bytes.toArray, StandardCharsets.UTF_8))
          )
          .compile
          .toList

        _ = startScriptOutputs.size shouldBe 1
        _ = startScriptOutputs.foreach(o => o.trim shouldBe "This is a start user script")

        // stop the cluster
        _ <- IO(stopAndMonitorRuntime(runtime.googleProject, runtime.clusterName))

        // preemptibles should be removed in Dataproc
        _ <- verifyDataproc(project, runtime.clusterName, dep.dataproc, 2, 0, RegionName("us-central1"))

        // start the cluster
        _ <- IO(startAndMonitorRuntime(runtime.googleProject, runtime.clusterName))

        // preemptibles should be added in Dataproc
        _ <- verifyDataproc(project, runtime.clusterName, dep.dataproc, 2, 5, RegionName("us-central1"))

        // check output of yarn node -list command
        _ <- IO(
          withWebDriver { implicit driver =>
            withNewNotebook(runtime, Python3) { notebookPage =>
              val output = notebookPage.executeCell("""!yarn node -list""")
              output.get should include("Total Nodes:")
            }
          }
        )

        // startup script should have run again
        startScriptOutputs <- dep.storage
          .listBlobsWithPrefix(
            runtime.stagingBucket.get,
            "startscript_output",
            true
          )
          .evalMap(blob =>
            dep.storage
              .getBlobBody(runtime.stagingBucket.get, GcsBlobName(blob.getName))
              .compile
              .toList
              .map(bytes => new String(bytes.toArray, StandardCharsets.UTF_8))
          )
          .compile
          .toList

        _ = startScriptOutputs.size shouldBe 2
        _ = startScriptOutputs.foreach(o => o.trim shouldBe "This is a start user script")

        _ <- LeonardoApiClient.deleteRuntime(project, runtimeName)
      } yield ()
    }

    res.unsafeRunSync()
  }

  private def verifyDataproc(project: GoogleProject,
                             runtimeName: RuntimeName,
                             dataproc: GoogleDataprocService[IO],
                             expectedNumWorkers: Int,
                             expectedPreemptibles: Int,
                             expectedRegion: RegionName): IO[Unit] =
    for {
      // check cluster status in Dataproc
      clusterOpt <- dataproc.getCluster(project, expectedRegion, DataprocClusterName(runtimeName.asString))
      cluster <- IO.fromOption(clusterOpt)(
        fail(s"Cluster not found in dataproc: ${project.value}/${runtimeName.asString}")
      )
      status <- IO.fromOption(DataprocClusterStatus.withNameInsensitiveOption(cluster.getStatus.getState.name))(
        fail(s"Unknown Dataproc status ${cluster.getStatus.getState.name}")
      )
      _ <- IO(status shouldBe DataprocClusterStatus.Running)

      // check cluster instances in Dataproc
      instances = GoogleDataprocInterpreter.getAllInstanceNames(cluster)
      _ <- IO(instances.size should be((expectedNumWorkers, expectedPreemptibles) match {
        case (0, 0) => 1
        case (_, 0) => 2
        case _      => 3
      }))
      _ <- instances.toList.traverse {
        case (k, v) =>
          IO(
            k.role match {
              case Master => v.size shouldBe 1
              case Worker => v.size shouldBe expectedNumWorkers
              case SecondaryWorker =>
                v.size shouldBe expectedPreemptibles
                k.isPreemptible shouldBe true
            }
          )
      }
    } yield ()

}

final case class RuntimeDataprocSpecDependencies(httpClient: Client[IO],
                                                 dataproc: GoogleDataprocService[IO],
                                                 storage: GoogleStorageService[IO])
