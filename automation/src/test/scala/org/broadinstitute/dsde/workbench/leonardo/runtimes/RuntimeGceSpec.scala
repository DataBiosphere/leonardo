package org.broadinstitute.dsde.workbench.leonardo
package runtimes

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.mtl.Ask
import com.google.cloud.Identity
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.google2.Generators.genDiskName
import org.broadinstitute.dsde.workbench.google2.{
  GcsBlobName,
  GoogleStorageService,
  MachineTypeName,
  StorageRole,
  ZoneName
}
import org.broadinstitute.dsde.workbench.leonardo.LeonardoApiClient.{defaultCreateRuntime2Request, getRuntime}
import org.broadinstitute.dsde.workbench.leonardo.LeonardoTestTags.ExcludeFromPRCommit
import org.broadinstitute.dsde.workbench.leonardo.TestUser.{getAuthTokenAndAuthorization, Ron}
import org.broadinstitute.dsde.workbench.leonardo.http.{PersistentDiskRequest, RuntimeConfigRequest}
import org.broadinstitute.dsde.workbench.leonardo.notebooks.{NotebookTestUtils, Python3}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsObjectName, GcsPath, GoogleProject}
import org.broadinstitute.dsde.workbench.service.Sam
import org.http4s.client.Client
import org.http4s.headers.Authorization
import org.scalatest.tagobjects.Retryable
import org.scalatest.{DoNotDiscover, ParallelTestExecution}

import java.nio.charset.{Charset, StandardCharsets}
import java.util.UUID

@DoNotDiscover
class RuntimeGceSpec
    extends BillingProjectFixtureSpec
    with ParallelTestExecution
    with LeonardoTestUtils
    with NotebookTestUtils {
  implicit val (authTokenForOldApiClient: IO[AuthToken], auth: IO[Authorization]) = getAuthTokenAndAuthorization(Ron)
  implicit val traceId: Ask[IO, TraceId] = Ask.const[IO, TraceId](TraceId(UUID.randomUUID()))

  val dependencies = for {
    storage <- google2StorageResource
    httpClient <- LeonardoApiClient.client
  } yield RuntimeGceSpecDependencies(httpClient, storage)

  "should create a GCE instance in a non-default zone" in { project =>
    val runtimeName = randomClusterName
    val diskName = genDiskName.sample.get
    val targetZone = ZoneName(
      "europe-west1-b"
    )

    // In a europe zone
    val createRuntimeRequest = defaultCreateRuntime2Request.copy(
      runtimeConfig = Some(
        RuntimeConfigRequest.GceWithPdConfig(
          Some(MachineTypeName("n1-standard-4")),
          PersistentDiskRequest(
            diskName,
            None,
            None,
            Map.empty
          ),
          Some(targetZone),
          None
        )
      )
    )
    val res = dependencies.use { deps =>
      implicit val httpClient = deps.httpClient
      for {
        getRuntimeResponse <- LeonardoApiClient.createRuntimeWithWait(project, runtimeName, createRuntimeRequest)
        _ = getRuntimeResponse.runtimeConfig.asInstanceOf[RuntimeConfig.GceWithPdConfig].zone shouldBe targetZone
        disk <- LeonardoApiClient.getDisk(project, getRuntimeResponse.diskConfig.get.name)
        _ = disk.zone shouldBe targetZone
        _ <- LeonardoApiClient.deleteRuntime(project, runtimeName)
      } yield ()
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  // TODO: Add back before merging!!! taggedAs ExcludeFromPRCommit
  "should be able to create a VM with GPU enabled" in { project =>
    val runtimeName = randomClusterName
    val diskName = genDiskName.sample.get

    val toolImage = ContainerImage.fromImageUrl(
      "us.gcr.io/broad-dsp-gcr-public/terra-jupyter-python:1.1.5"
    )
    val createRuntimeRequest = defaultCreateRuntime2Request.copy(
      runtimeConfig = Some(
        RuntimeConfigRequest.GceWithPdConfig(
          Some(MachineTypeName("n1-standard-4")),
          PersistentDiskRequest(
            diskName,
            None,
            None,
            Map.empty
          ),
          Some(ZoneName("us-west1-a")),
          Some(GpuConfig(GpuType.NvidiaTeslaT4, 2))
        )
      ),
      toolDockerImage = toolImage
    )

    val res = dependencies.use { deps =>
      implicit val httpClient = deps.httpClient
      for {
        runtime <- LeonardoApiClient.createRuntimeWithWait(project, runtimeName, createRuntimeRequest)
        clusterCopy = ClusterCopy.fromGetRuntimeResponseCopy(runtime)
        implicit0(authToken: AuthToken) <- Ron.authToken()
        _ <- IO(withWebDriver { implicit driver =>
          withNewNotebook(clusterCopy, Python3) { notebookPage =>
            notebookPage.executeCell("import tensorflow as tf")
            val deviceNameOutput =
              """gpus = tf.config.experimental.list_physical_devices('GPU')
                |print(gpus)
                |""".stripMargin
            val output = notebookPage.executeCell(deviceNameOutput).get
            output should include("GPU:0")
            output should include("GPU:1")
          }
        })
        _ <- LeonardoApiClient.deleteRuntime(project, runtimeName)
      } yield ()
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  "should run a user script and startup script for Jupyter" in { project =>
    testStartupScripts(project).unsafeRunSync()
  }

  "should run a user script and startup script for RStudio" taggedAs Retryable in { project =>
    testStartupScripts(project, Some(LeonardoConfig.Leonardo.rstudioBioconductorImage)).unsafeRunSync()
  }

  private def testStartupScripts(project: GoogleProject, image: Option[ContainerImage] = None): IO[Unit] = {
    dependencies.use { deps =>
      implicit val client = deps.httpClient
      for {
        // Set up test bucket for startup script
        implicit0(authToken: AuthToken) <- Ron.authToken()
        petSA <- IO(Sam.user.petServiceAccountEmail(project.value))
        bucketName <- IO(UUID.randomUUID()).map(u => GcsBucketName(s"leo-test-bucket-${u.toString}"))
        userScriptObjectName = GcsBlobName("test-user-script.sh")
        userStartScriptObjectName = GcsBlobName("test-user-start-script.sh")
        _ <- deps.storage.insertBucket(project, bucketName).compile.drain
        _ <- deps.storage
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
          .through(deps.storage.streamUploadBlob(bucketName, userScriptObjectName))
          .compile
          .drain
        _ <- fs2.Stream
          .emits(userStartScriptString.getBytes(Charset.forName("UTF-8")))
          .covary[IO]
          .through(deps.storage.streamUploadBlob(bucketName, userStartScriptObjectName))
          .compile
          .drain

        // create runtime
        runtimeName = randomClusterName
        createRuntimeRequest = defaultCreateRuntime2Request.copy(
          toolDockerImage = image,
          userScriptUri = Some(UserScriptPath.Gcs(GcsPath(bucketName, GcsObjectName(userScriptObjectName.value)))),
          startUserScriptUri =
            Some(UserScriptPath.Gcs(GcsPath(bucketName, GcsObjectName(userStartScriptObjectName.value))))
        )

        getRuntimeResponse <- LeonardoApiClient.createRuntimeWithWait(project, runtimeName, createRuntimeRequest)
        runtime = ClusterCopy.fromGetRuntimeResponseCopy(getRuntimeResponse)
        _ = runtime.status shouldBe ClusterStatus.Running
        _ = runtime.stagingBucket shouldBe defined

        // verify user scripts were executed
        userScriptOutput <- deps.storage
          .getBlobBody(runtime.stagingBucket.get, GcsBlobName("userscript_output.txt"))
          .compile
          .toList
          .map(bytes => new String(bytes.toArray, StandardCharsets.UTF_8))

        _ = userScriptOutput.trim shouldBe "This is a user script"

        startScriptOutputs <- deps.storage
          .listBlobsWithPrefix(
            runtime.stagingBucket.get,
            "startscript_output",
            true
          )
          .evalMap(blob =>
            deps.storage
              .getBlobBody(runtime.stagingBucket.get, GcsBlobName(blob.getName))
              .compile
              .toList
              .map(bytes => new String(bytes.toArray, StandardCharsets.UTF_8))
          )
          .compile
          .toList

        _ = startScriptOutputs.size shouldBe 1
        _ = startScriptOutputs.foreach(o => o.trim shouldBe "This is a start user script")

        // stop/start the runtime
        _ <- IO(stopAndMonitorRuntime(runtime.googleProject, runtime.clusterName))
        _ <- IO(startAndMonitorRuntime(runtime.googleProject, runtime.clusterName))

        getRuntimeResponse <- getRuntime(runtime.googleProject, runtime.clusterName)
        runtime = ClusterCopy.fromGetRuntimeResponseCopy(getRuntimeResponse)
        _ = runtime.status shouldBe ClusterStatus.Running

        // startup script should have run again
        startScriptOutputs <- deps.storage
          .listBlobsWithPrefix(
            runtime.stagingBucket.get,
            "startscript_output",
            true
          )
          .evalMap(blob =>
            deps.storage
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
  }
}

final case class RuntimeGceSpecDependencies(httpClient: Client[IO], storage: GoogleStorageService[IO])
