package org.broadinstitute.dsde.workbench.leonardo
package monitor

import java.time.Instant
import java.util.concurrent.TimeUnit

import cats.effect.IO
import cats.mtl.Ask
import com.google.cloud.compute.v1._
import org.broadinstitute.dsde.workbench.google2.mock.{FakeGoogleComputeService, MockComputePollOperation}
import org.broadinstitute.dsde.workbench.google2.{
  ComputePollOperation,
  GoogleComputeService,
  InstanceName,
  OperationName,
  ZoneName
}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao.{MockToolDAO, ToolDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterErrorQuery, clusterQuery, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.http.{dbioToIO, userScriptStartupOutputUriMetadataKey}
import org.broadinstitute.dsde.workbench.leonardo.monitor.RuntimeMonitor._
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.model
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsObjectName, GcsPath, GoogleProject}
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class GceRuntimeMonitorSpec
    extends AnyFlatSpec
    with Matchers
    with TestComponent
    with LeonardoTestSuite
    with EitherValues {
  val readyInstance = Instance
    .newBuilder()
    .setStatus("Running")
    .setMetadata(
      Metadata
        .newBuilder()
        .addItems(
          Items.newBuilder
            .setKey(userScriptStartupOutputUriMetadataKey)
            .setValue("gs://success/object")
            .build()
        )
        .build()
    )
    .addNetworkInterfaces(
      NetworkInterface
        .newBuilder()
        .addAccessConfigs(AccessConfig.newBuilder().setNatIP("fakeIP").build())
        .build()
    )
    .build()

  "validateUserScript" should "validate user script properly" in {
    val monitor = gceRuntimeMonitor()
    val sucessUserScript = GcsPath(GcsBucketName("success"), GcsObjectName("object_output"))
    val failureUserScript = GcsPath(GcsBucketName("failure"), GcsObjectName("object_output"))
    val nonExistentUserScript = GcsPath(GcsBucketName("nonExistent"), GcsObjectName("object_output"))
    val res = for {
      ctx <- appContext.ask[AppContext]
      res1 <- monitor.validateUserScript(None, None)
      res2 <- monitor.validateUserScript(Some(sucessUserScript), None)
      res3 <- monitor.validateUserScript(
        Some(sucessUserScript),
        Some(UserScriptPath.Gcs(sucessUserScript.copy(objectName = GcsObjectName("userscript"))))
      )
      res4 <- monitor.validateUserScript(
        Some(failureUserScript),
        Some(UserScriptPath.Gcs(sucessUserScript.copy(objectName = GcsObjectName("userscript"))))
      )
      res5 <- monitor.validateUserScript(
        Some(nonExistentUserScript),
        Some(UserScriptPath.Gcs(sucessUserScript.copy(objectName = GcsObjectName("userscript"))))
      )
      res6 <- monitor
        .validateUserScript(None,
                            Some(UserScriptPath.Gcs(sucessUserScript.copy(objectName = GcsObjectName("userscript")))))
        .attempt
    } yield {
      res1 shouldBe UserScriptsValidationResult.Success
      res2 shouldBe UserScriptsValidationResult.Success
      res3 shouldBe UserScriptsValidationResult.Success
      res4 shouldBe (UserScriptsValidationResult.Error(
        "User script failed. See output in gs://failure/object_output"
      ))
      res5 shouldBe (UserScriptsValidationResult.CheckAgain(
        "User script hasn't finished yet. See output in gs://nonExistent/object_output"
      ))
      res6.left.value.getMessage shouldBe (s"${ctx} | staging bucket field hasn't been updated properly before monitoring started")
    }

    res.unsafeRunSync
  }

  "validateUserStartupScript" should "validate user startup script properly" in {
    val monitor = gceRuntimeMonitor()
    val sucessUserScript = GcsPath(GcsBucketName("success"), GcsObjectName("object_output"))
    val failureUserScript = GcsPath(GcsBucketName("failure"), GcsObjectName("object_output"))
    val nonExistentUserScript = GcsPath(GcsBucketName("nonExistent"), GcsObjectName("object_output"))
    val res = for {
      ctx <- appContext.ask[AppContext]
      res1 <- monitor.validateUserStartupScript(None, None)
      res2 <- monitor.validateUserStartupScript(Some(sucessUserScript), None)
      res3 <- monitor.validateUserStartupScript(
        Some(sucessUserScript),
        Some(UserScriptPath.Gcs(sucessUserScript.copy(objectName = GcsObjectName("userscript"))))
      )
      res4 <- monitor.validateUserStartupScript(
        Some(failureUserScript),
        Some(UserScriptPath.Gcs(sucessUserScript.copy(objectName = GcsObjectName("userscript"))))
      )
      res5 <- monitor.validateUserStartupScript(
        Some(nonExistentUserScript),
        Some(UserScriptPath.Gcs(sucessUserScript.copy(objectName = GcsObjectName("userscript"))))
      )
      res6 <- monitor.validateUserStartupScript(
        None,
        Some(UserScriptPath.Gcs(sucessUserScript.copy(objectName = GcsObjectName("userscript"))))
      )
    } yield {
      res1 shouldBe UserScriptsValidationResult.Success
      res2 shouldBe UserScriptsValidationResult.Success
      res3 shouldBe UserScriptsValidationResult.Success
      res4 shouldBe (UserScriptsValidationResult.Error(
        "User startup script failed. See output in gs://failure/object_output"
      ))
      res5 shouldBe (UserScriptsValidationResult.CheckAgain(
        "User startup script hasn't finished yet. See output in gs://nonExistent/object_output"
      ))
      res6 shouldBe (UserScriptsValidationResult.CheckAgain(s"${ctx} | Instance is not ready yet"))
    }

    res.unsafeRunSync
  }

  it should "check whether user script has failed correctly" in {
    val monitor = gceRuntimeMonitor()
    monitor
      .checkUserScriptsOutputFile(model.google.GcsPath(GcsBucketName("failure"), GcsObjectName("")))
      .unsafeRunSync() shouldBe (Some(false))
    monitor
      .checkUserScriptsOutputFile(model.google.GcsPath(GcsBucketName("success"), GcsObjectName("")))
      .unsafeRunSync() shouldBe (Some(true))
    monitor
      .checkUserScriptsOutputFile(model.google.GcsPath(GcsBucketName("nonExistent"), GcsObjectName("")))
      .unsafeRunSync() shouldBe (None)
  }

  it should "retrieve user script from instance metadata properly" in {
    getUserScript(readyInstance) shouldBe Some(GcsPath(GcsBucketName("success"), GcsObjectName("object")))
  }

  // process, Creating
  "process" should "fail Creating if user script failed" in isolatedDbTest {
    val runtime = makeCluster(1).copy(
      serviceAccount = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1).copy(stagingBucket = GcsBucketName("failure"))),
      userScriptUri =
        Some(UserScriptPath.Gcs(GcsPath(GcsBucketName("failure"), GcsObjectName("userscript_output.txt")))),
      status = RuntimeStatus.Creating
    )

    val computeService: GoogleComputeService[IO] = new FakeGoogleComputeService {
      override def getInstance(project: GoogleProject, zone: ZoneName, instanceName: InstanceName)(
        implicit ev: Ask[IO, TraceId]
      ): IO[Option[Instance]] = IO.pure(Some(readyInstance))
    }

    val res = for {
      start <- nowInstant[IO]
      monitor = gceRuntimeMonitor(googleComputeService = computeService)
      savedRuntime <- IO(runtime.saveWithRuntimeConfig(gceRuntimeConfig))
      _ <- monitor.process(savedRuntime.id, RuntimeStatus.Creating).compile.drain //start monitoring process
      afterMonitor <- nowInstant
      status <- clusterQuery.getClusterStatus(savedRuntime.id).transaction
      error <- clusterErrorQuery.get(savedRuntime.id).transaction
    } yield {
      (afterMonitor.toEpochMilli - start.toEpochMilli < 5000) shouldBe true // initial delay in tests is 2 seconds and 1 second polling interval, the stream should terminate after a few more checks
      status shouldBe Some(RuntimeStatus.Error)
      error.head.errorMessage shouldBe s"User script failed. See output in gs://failure/userscript_output.txt"
    }

    res.unsafeRunSync
  }

  // process, Creating
  it should "fail Creating if user startup script failed" in isolatedDbTest {
    val runtime = makeCluster(1).copy(
      serviceAccount = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1).copy(stagingBucket = GcsBucketName("staging_bucket"))),
      startUserScriptUri = Some(
        UserScriptPath
          .Gcs(GcsPath(GcsBucketName("staging_bucket"), GcsObjectName("failed_userstartupscript_output.txt")))
      ),
      status = RuntimeStatus.Creating
    )

    val computeService: GoogleComputeService[IO] = new FakeGoogleComputeService {
      override def getInstance(project: GoogleProject, zone: ZoneName, instanceName: InstanceName)(
        implicit ev: Ask[IO, TraceId]
      ): IO[Option[Instance]] = {
        val runningInstance = Instance
          .newBuilder()
          .setStatus("Running")
          .setMetadata(
            Metadata
              .newBuilder()
              .addItems(
                Items.newBuilder
                  .setKey(userScriptStartupOutputUriMetadataKey)
                  .setValue("gs://staging_bucket/failed_userstartupscript_output.txt")
                  .build()
              )
              .build()
          )
          .build()

        IO.pure(Some(runningInstance))
      }
    }

    val res = for {
      start <- nowInstant[IO]
      monitor = gceRuntimeMonitor(googleComputeService = computeService)
      savedRuntime <- IO(runtime.saveWithRuntimeConfig(gceRuntimeConfig))
      _ <- monitor.process(savedRuntime.id, RuntimeStatus.Creating).compile.drain //start monitoring process
      afterMonitor <- nowInstant
      status <- clusterQuery.getClusterStatus(savedRuntime.id).transaction
      error <- clusterErrorQuery.get(savedRuntime.id).transaction
    } yield {
      (afterMonitor.toEpochMilli - start.toEpochMilli < 5000) shouldBe true // initial delay in tests is 2 seconds and 1 second polling interval, the stream should terminate after a few more checks
      status shouldBe Some(RuntimeStatus.Error)
      error.head.errorMessage shouldBe s"User startup script failed. See output in gs://staging_bucket/failed_userstartupscript_output.txt"
    }

    res.unsafeRunSync
  }

  // process, Creating
  it should "will check again if instance still exists when trying to Creating one" in isolatedDbTest {
    val runtime = makeCluster(1).copy(
      serviceAccount = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Creating
    )

    def computeService(start: Long): GoogleComputeService[IO] = new FakeGoogleComputeService {
      override def getInstance(project: GoogleProject, zone: ZoneName, instanceName: InstanceName)(
        implicit ev: Ask[IO, TraceId]
      ): IO[Option[Instance]] = {
        val beforeInstance = None
        val runningInstance = readyInstance

        for {
          now <- testTimer.clock.realTime(TimeUnit.MILLISECONDS)
          res <- if (now - start < 5000)
            IO.pure(beforeInstance)
          else IO.pure(Some(runningInstance))
        } yield res
      }
    }

    val res = for {
      start <- nowInstant[IO]
      monitor = gceRuntimeMonitor(googleComputeService = computeService(start.toEpochMilli))
      savedRuntime <- IO(runtime.saveWithRuntimeConfig(gceRuntimeConfig))
      _ <- monitor.process(savedRuntime.id, RuntimeStatus.Creating).compile.drain //start monitoring process
      afterMonitor <- nowInstant
      status <- clusterQuery.getClusterStatus(savedRuntime.id).transaction
    } yield {
      (afterMonitor.toEpochMilli - start.toEpochMilli > 5000) shouldBe true // For 5 seconds, google is returning terminated no instance found
      status shouldBe Some(RuntimeStatus.Running)
    }

    res.unsafeRunSync()
  }

  // process, Starting
  it should "will check again if instance still exists when trying to Starting one" in isolatedDbTest {
    val runtime = makeCluster(1).copy(
      serviceAccount = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Starting
    )

    def computeService(start: Long): GoogleComputeService[IO] = new FakeGoogleComputeService {
      override def getInstance(project: GoogleProject, zone: ZoneName, instanceName: InstanceName)(
        implicit ev: Ask[IO, TraceId]
      ): IO[Option[Instance]] = {
        val beforeInstance = Instance.newBuilder().setStatus("Stopping").build()

        for {
          now <- testTimer.clock.realTime(TimeUnit.MILLISECONDS)
          res <- if (now - start < 5000)
            IO.pure(Some(beforeInstance))
          else IO.pure(Some(readyInstance))
        } yield res
      }
    }

    val res = for {
      start <- nowInstant[IO]
      monitor = gceRuntimeMonitor(googleComputeService = computeService(start.toEpochMilli))
      savedRuntime <- IO(runtime.saveWithRuntimeConfig(gceRuntimeConfig))
      _ <- monitor.process(savedRuntime.id, RuntimeStatus.Starting).compile.drain //start monitoring process
      afterMonitor <- nowInstant
      status <- clusterQuery.getClusterStatus(savedRuntime.id).transaction
    } yield {
      (afterMonitor.toEpochMilli - start.toEpochMilli > 5000) shouldBe true // For 5 seconds, google is returning terminated no instance found
      status shouldBe Some(RuntimeStatus.Running)
    }

    res.unsafeRunSync()
  }

  it should "transition gce runtime to Stopping if Starting times out" in isolatedDbTest {
    def computeService(start: Long): GoogleComputeService[IO] = new FakeGoogleComputeService {
      override def getInstance(project: GoogleProject, zone: ZoneName, instanceName: InstanceName)(
        implicit ev: Ask[IO, TraceId]
      ): IO[Option[Instance]] = {
        val beforeInstance = Instance.newBuilder().setStatus("Provisioning").build()
        val afterInstance = Instance.newBuilder().setStatus("Stopped").build()

        for {
          now <- testTimer.clock.realTime(TimeUnit.MILLISECONDS)
          res <- if (now - start < 5000)
            IO.pure(Some(beforeInstance))
          else IO.pure(Some(afterInstance))
        } yield res
      }
    }

    val res = for {
      start <- nowInstant[IO]
      monitor = gceRuntimeMonitor(googleComputeService = computeService(start.toEpochMilli),
                                  monitorStatusTimeouts = Some(Map(RuntimeStatus.Starting -> 2.seconds)))
      runtime <- IO(makeCluster(0).copy(status = RuntimeStatus.Starting).saveWithRuntimeConfig(gceRuntimeConfig))
      assersions = for {
        status <- clusterQuery.getClusterStatus(runtime.id).transaction
      } yield status.get shouldBe RuntimeStatus.Stopped
      _ <- withInfiniteStream(monitor.process(runtime.id, RuntimeStatus.Starting), assersions)
    } yield ()
    res.unsafeRunSync()
  }

  it should "terminate if instance is terminated after 5 seconds when trying to Starting one" in isolatedDbTest {
    val runtime = makeCluster(1).copy(
      serviceAccount = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Starting
    )

    def computeService(start: Long): GoogleComputeService[IO] = new FakeGoogleComputeService {
      override def getInstance(project: GoogleProject, zone: ZoneName, instanceName: InstanceName)(
        implicit ev: Ask[IO, TraceId]
      ): IO[Option[Instance]] = {
        val beforeInstance = Instance.newBuilder().setStatus("STOPPING").build()
        val terminatedInstance = Instance.newBuilder().setStatus("TERMINATED").build()

        for {
          now <- testTimer.clock.realTime(TimeUnit.MILLISECONDS)
          res <- if (now - start < 4000)
            IO.pure(Some(beforeInstance))
          else IO.pure(Some(terminatedInstance))
        } yield res
      }
    }

    val res = for {
      start <- nowInstant[IO]
      monitor = gceRuntimeMonitor(googleComputeService = computeService(start.toEpochMilli))
      savedRuntime <- IO(runtime.saveWithRuntimeConfig(gceRuntimeConfig))
      _ <- monitor.process(savedRuntime.id, RuntimeStatus.Starting).compile.drain //start monitoring process
      afterMonitor <- nowInstant
      status <- clusterQuery.getClusterStatus(savedRuntime.id).transaction
    } yield {
      val delay = afterMonitor.toEpochMilli - start.toEpochMilli
      (delay > 5000) shouldBe true // For 5 seconds, google is returning terminated no instance found
      (delay < 10000) shouldBe true
      status shouldBe Some(RuntimeStatus.Stopped)
    }

    res.unsafeRunSync()
  }

  it should "fail Starting if user startup script failed" in isolatedDbTest {
    val runtime = makeCluster(1).copy(
      serviceAccount = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1).copy(stagingBucket = GcsBucketName("staging_bucket"))),
      startUserScriptUri = Some(
        UserScriptPath
          .Gcs(GcsPath(GcsBucketName("staging_bucket"), GcsObjectName("failed_userstartupscript_output.txt")))
      ),
      status = RuntimeStatus.Starting
    )

    val computeService: GoogleComputeService[IO] = new FakeGoogleComputeService {
      override def getInstance(project: GoogleProject, zone: ZoneName, instanceName: InstanceName)(
        implicit ev: Ask[IO, TraceId]
      ): IO[Option[Instance]] = {
        val runningInstance = Instance
          .newBuilder()
          .setStatus("Running")
          .setMetadata(
            Metadata
              .newBuilder()
              .addItems(
                Items.newBuilder
                  .setKey(userScriptStartupOutputUriMetadataKey)
                  .setValue("gs://staging_bucket/failed_userstartupscript_output.txt")
                  .build()
              )
              .build()
          )
          .build()

        IO.pure(Some(runningInstance))
      }
    }

    val res = for {
      start <- nowInstant[IO]
      monitor = gceRuntimeMonitor(googleComputeService = computeService)
      savedRuntime <- IO(runtime.saveWithRuntimeConfig(gceRuntimeConfig))
      _ <- monitor.process(savedRuntime.id, RuntimeStatus.Starting).compile.drain //start monitoring process
      afterMonitor <- nowInstant
      status <- clusterQuery.getClusterStatus(savedRuntime.id).transaction
      error <- clusterErrorQuery.get(savedRuntime.id).transaction
    } yield {
      (afterMonitor.toEpochMilli - start.toEpochMilli < 5000) shouldBe true // initial delay in tests is 2 seconds and 1 second polling interval, the stream should terminate after a few more checks
      status shouldBe Some(RuntimeStatus.Error)
      error.head.errorMessage shouldBe s"User startup script failed. See output in gs://staging_bucket/failed_userstartupscript_output.txt"
    }

    res.unsafeRunSync
  }

  // process
  it should "exit monitor if status is not monitored" in isolatedDbTest {
    val runtime = makeCluster(1).copy(
      serviceAccount = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Stopped
    )

    val monitor = gceRuntimeMonitor()
    val savedRuntime = runtime.saveWithRuntimeConfig(gceRuntimeConfig)
    val res = for {
      now <- nowInstant[IO]
      _ <- monitor.process(savedRuntime.id, RuntimeStatus.Creating).compile.drain //start monitoring process
      afterMonitor <- nowInstant
      status <- clusterQuery.getClusterStatus(savedRuntime.id).transaction
    } yield {
      (afterMonitor.toEpochMilli - now.toEpochMilli < 6000) shouldBe true // initial delay in tests is 2 seconds and 1 second polling interval, the stream should terminate after initial check
      status shouldBe (Some(RuntimeStatus.Stopped))
    }

    res.unsafeRunSync()
  }

  // process, Stopping
  it should "error when trying to Stop an instance that doesn't exist in GCP" in isolatedDbTest {
    val runtime = makeCluster(1).copy(
      serviceAccount = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Stopping
    )

    val monitor = gceRuntimeMonitor()
    val savedRuntime = runtime.saveWithRuntimeConfig(gceRuntimeConfig)
    val res = for {
      now <- nowInstant[IO]
      _ <- monitor.process(savedRuntime.id, RuntimeStatus.Stopping).compile.drain //start monitoring process
      afterMonitor <- nowInstant
      status <- clusterQuery.getClusterStatus(savedRuntime.id).transaction
    } yield {
      (afterMonitor.toEpochMilli - now.toEpochMilli < 5000) shouldBe true // initial delay in tests is 2 seconds and 1 second polling interval, the stream should terminate after initial check
      status shouldBe (Some(RuntimeStatus.Stopping))
    }

    res.unsafeRunSync()
  }

  // process, Stopping
  it should "update runtime status appropriately when successfully stopped an instance" in isolatedDbTest {
    val runtime = makeCluster(1).copy(
      serviceAccount = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Stopping
    )

    val computeService = new FakeGoogleComputeService {
      override def getInstance(project: GoogleProject, zone: ZoneName, instanceName: InstanceName)(
        implicit ev: Ask[IO, TraceId]
      ): IO[Option[Instance]] = {
        val instance = Instance.newBuilder().setStatus("Terminated").build()
        IO.pure(Some(instance))
      }
    }
    val monitor = gceRuntimeMonitor(googleComputeService = computeService)
    val savedRuntime = runtime.saveWithRuntimeConfig(gceRuntimeConfig)
    val res = for {
      now <- nowInstant[IO]
      _ <- monitor.process(savedRuntime.id, RuntimeStatus.Stopping).compile.drain //start monitoring process
      afterMonitor <- nowInstant
      status <- clusterQuery.getClusterStatus(savedRuntime.id).transaction
    } yield {
      (afterMonitor.toEpochMilli - now.toEpochMilli < 5000) shouldBe true // initial delay in tests is 2 seconds and 1 second polling interval, the stream should terminate after initial check
      status shouldBe (Some(RuntimeStatus.Stopped))
    }

    res.unsafeRunSync()
  }

  // process, Stopping
  it should "will check again if instance is not terminated yet when trying to stop one" in isolatedDbTest {
    val runtime = makeCluster(1).copy(
      serviceAccount = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Stopping
    )

    val res = for {
      start <- nowInstant[IO]
      monitor = gceRuntimeMonitor(
        googleComputeService =
          computeService(start.toEpochMilli, Some(GceInstanceStatus.Running), Some(GceInstanceStatus.Terminated))
      )
      savedRuntime <- IO(runtime.saveWithRuntimeConfig(gceRuntimeConfig))
      _ <- monitor.process(savedRuntime.id, RuntimeStatus.Stopping).compile.drain //start monitoring process
      afterMonitor <- nowInstant
      status <- clusterQuery.getClusterStatus(savedRuntime.id).transaction
    } yield {
      (afterMonitor.toEpochMilli - start.toEpochMilli > 5000) shouldBe true // initial delay in tests is 2 seconds and 1 second polling interval, the stream should terminate after a few more checks
      status shouldBe Some(RuntimeStatus.Stopped)
    }

    res.unsafeRunSync()
  }

  // process, Deleting
  it should "delete runtime successfully when instance doesn't exist in GCP" in isolatedDbTest {
    val runtime = makeCluster(1).copy(
      serviceAccount = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Deleting
    )

    val monitor = gceRuntimeMonitor()
    val savedRuntime = runtime.saveWithRuntimeConfig(gceRuntimeConfig)
    val res = for {
      now <- nowInstant[IO]
      _ <- monitor.process(savedRuntime.id, RuntimeStatus.Deleting).compile.drain //start monitoring process
      afterMonitor <- nowInstant
      status <- clusterQuery.getClusterStatus(savedRuntime.id).transaction
    } yield {
      (afterMonitor.toEpochMilli - now.toEpochMilli < 5000) shouldBe true // initial delay in tests is 2 seconds and 1 second polling interval, the stream should terminate after initial check
      status shouldBe (Some(RuntimeStatus.Deleted))
    }

    res.unsafeRunSync()
  }

  // process, Deleting
  it should "will check again if instance still exists when trying to delete one" in isolatedDbTest {
    val runtime = makeCluster(1).copy(
      serviceAccount = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Deleting
    )

    def computeService(start: Long): GoogleComputeService[IO] = new FakeGoogleComputeService {
      override def getInstance(project: GoogleProject, zone: ZoneName, instanceName: InstanceName)(
        implicit ev: Ask[IO, TraceId]
      ): IO[Option[Instance]] = {
        val runningInstance = Instance.newBuilder().setStatus("Running").build()

        for {
          now <- testTimer.clock.realTime(TimeUnit.MILLISECONDS)
          res <- if (now - start < 5000)
            IO.pure(Some(runningInstance))
          else IO.pure(None)
        } yield res
      }
    }

    val res = for {
      start <- nowInstant[IO]
      monitor = gceRuntimeMonitor(googleComputeService = computeService(start.toEpochMilli))
      savedRuntime <- IO(runtime.saveWithRuntimeConfig(gceRuntimeConfig))
      _ <- monitor.process(savedRuntime.id, RuntimeStatus.Deleting).compile.drain //start monitoring process
      afterMonitor <- nowInstant

      status <- clusterQuery.getClusterStatus(savedRuntime.id).transaction
    } yield {
      (afterMonitor.toEpochMilli - start.toEpochMilli > 5000) shouldBe true // initial delay in tests is 2 seconds and 1 second polling interval, the stream should terminate after a few more checks
      status shouldBe Some(RuntimeStatus.Deleted)
    }

    res.unsafeRunSync()
  }

  //pollCheck, Deleting
  "pollCheck" should "raise error if we get invalid monitoring status" in {
    val runtime = makeCluster(1).copy(
      serviceAccount = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Deleted
    )

    val op = com.google.cloud.compute.v1.Operation.newBuilder().build()
    val monitor = gceRuntimeMonitor()
    val res = for {
      _ <- IO(runtime.saveWithRuntimeConfig(gceRuntimeConfig))
      r <- monitor
        .pollCheck(
          runtime.googleProject,
          RuntimeAndRuntimeConfig(runtime, CommonTestData.defaultDataprocRuntimeConfig),
          op,
          RuntimeStatus.Deleted
        )
        .attempt
    } yield {
      r.left.value.getMessage shouldBe "Monitoring Deleted with pollOperation is not supported"
    }

    res.unsafeRunSync()
  }

  it should "set runtime to PreDeleting if Stopping is interrupted by Deleting" in isolatedDbTest {
    val op = Operation.newBuilder().setId("op").setName("opName").setTargetId("target").setStatus("PENDING").build()
    val res = for {
      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Stopping).saveWithRuntimeConfig(gceRuntimeConfig))
      pollOperation = new MockComputePollOperation {
        // In the first operation call, we set runtime status to Deleting, this should cause the original `Stopping` process to cancel and we'll
        // enqueue a delete message instead
        override def getGlobalOperation(project: GoogleProject, operationName: OperationName)(
          implicit ev: Ask[IO, TraceId]
        ): IO[Operation] =
          clusterQuery
            .updateClusterStatus(runtime.id, RuntimeStatus.Deleting, Instant.now())
            .transaction
            .as(op)
      }
      monitor = gceRuntimeMonitor(computePollOperation = pollOperation)

      _ <- monitor.pollCheck(runtime.googleProject,
                             RuntimeAndRuntimeConfig(runtime, gceRuntimeConfig),
                             op,
                             RuntimeStatus.Stopping)
      status <- clusterQuery.getClusterStatus(runtime.id).transaction
    } yield {
      status shouldBe (Some(RuntimeStatus.PreDeleting))
    }

    res.unsafeRunSync()
  }

  it should "monitor Deleting successfully" in {
    val runtime = makeCluster(2).copy(
      serviceAccount = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(2)),
      status = RuntimeStatus.Deleting
    )

    val initialOp = com.google.cloud.compute.v1.Operation.newBuilder().setStatus("PENDING").build()

    def computePollOperation(start: Long): ComputePollOperation[IO] = new MockComputePollOperation {
      override def getGlobalOperation(project: GoogleProject, operationName: OperationName)(
        implicit ev: Ask[IO, TraceId]
      ): IO[Operation] = {
        val pendingOp = com.google.cloud.compute.v1.Operation.newBuilder().setStatus("PENDING").build()
        val afterOperation = com.google.cloud.compute.v1.Operation.newBuilder().setStatus("DONE").build()

        for {
          now <- testTimer.clock.realTime(TimeUnit.MILLISECONDS)
          res <- if (now - start < 4000)
            IO.pure(pendingOp)
          else IO.pure(afterOperation)
        } yield res
      }
    }

    val res = for {
      start <- nowInstant[IO]
      monitor = gceRuntimeMonitor(computePollOperation = computePollOperation(start.toEpochMilli))
      savedRuntime <- IO(runtime.saveWithRuntimeConfig(gceRuntimeConfig))
      _ <- monitor.pollCheck(
        savedRuntime.googleProject,
        RuntimeAndRuntimeConfig(savedRuntime, CommonTestData.defaultDataprocRuntimeConfig),
        initialOp,
        RuntimeStatus.Deleting
      ) //start monitoring process
      afterMonitor <- nowInstant
      status <- clusterQuery.getClusterStatus(savedRuntime.id).transaction
    } yield {
      (afterMonitor.toEpochMilli - start.toEpochMilli > 4000) shouldBe true // initial delay in tests is 2 seconds and 1 second polling interval, the stream should terminate after a few more checks
      status shouldBe Some(RuntimeStatus.Deleted)
    }

    res.unsafeRunSync()
  }

  //pollCheck Deleting
  it should "fail if reaches pollCheckMaxAttempts" in isolatedDbTest {
    val runtime = makeCluster(2).copy(
      serviceAccount = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(2)),
      status = RuntimeStatus.Deleting
    )

    val op = com.google.cloud.compute.v1.Operation.newBuilder().setStatus("PENDING").build()

    val pollOperation: ComputePollOperation[IO] = new MockComputePollOperation {
      override def getGlobalOperation(project: GoogleProject, operationName: OperationName)(
        implicit ev: Ask[IO, TraceId]
      ): IO[Operation] = IO.pure(op)
    }

    val res = for {
      start <- nowInstant[IO]
      monitor = gceRuntimeMonitor(computePollOperation = pollOperation)
      savedRuntime <- IO(runtime.saveWithRuntimeConfig(gceRuntimeConfig))
      _ <- monitor.pollCheck(
        runtime.googleProject,
        RuntimeAndRuntimeConfig(savedRuntime, CommonTestData.defaultDataprocRuntimeConfig),
        op,
        RuntimeStatus.Deleting
      ) //start monitoring process
      afterMonitor <- nowInstant
      status <- clusterQuery.getClusterStatus(savedRuntime.id).transaction
      error <- clusterErrorQuery.get(savedRuntime.id).transaction
    } yield {
      (afterMonitor.toEpochMilli - start.toEpochMilli > 6000) shouldBe true // max 5 retries, and each poll interval is 1 second
      status shouldBe Some(RuntimeStatus.Error)
      error.head.errorMessage shouldBe s"Deleting dsp-leo-test/clustername2 fail to complete in a timely manner"
    }

    res.unsafeRunSync()
  }

  implicit val toolDao: RuntimeContainerServiceType => ToolDAO[IO, RuntimeContainerServiceType] = _ => MockToolDAO(true)

  def gceRuntimeMonitor(
    googleComputeService: GoogleComputeService[IO] = FakeGoogleComputeService,
    computePollOperation: ComputePollOperation[IO] = new MockComputePollOperation,
    publisherQueue: fs2.concurrent.Queue[IO, LeoPubsubMessage] = QueueFactory.makePublisherQueue(),
    monitorStatusTimeouts: Option[Map[RuntimeStatus, FiniteDuration]] = None
  ): GceRuntimeMonitor[IO] = {
    val config =
      Config.gceMonitorConfig.copy(initialDelay = 2 seconds, pollingInterval = 1 seconds, pollCheckMaxAttempts = 5)
    val configWithCustomTimeouts =
      monitorStatusTimeouts.fold(config)(timeouts => config.copy(monitorStatusTimeouts = timeouts))
    new GceRuntimeMonitor[IO](
      configWithCustomTimeouts,
      googleComputeService,
      computePollOperation,
      MockAuthProvider,
      FakeGoogleStorageService,
      publisherQueue,
      GceInterp
    )
  }

  def computeService(start: Long,
                     beforeStatus: Option[GceInstanceStatus],
                     afterStatus: Option[GceInstanceStatus]): GoogleComputeService[IO] = new FakeGoogleComputeService {
    override def getInstance(project: GoogleProject, zone: ZoneName, instanceName: InstanceName)(
      implicit ev: Ask[IO, TraceId]
    ): IO[Option[Instance]] = {
      val beforeInstance = beforeStatus.map(s => Instance.newBuilder().setStatus(s.toString).build())
      val afterInstance = afterStatus.map(s => Instance.newBuilder().setStatus(s.toString).build())

      for {
        now <- testTimer.clock.realTime(TimeUnit.MILLISECONDS)
        res <- if (now - start < 5000)
          IO.pure(beforeInstance)
        else IO.pure(afterInstance)
      } yield res
    }
  }
}

class BaseFakeGceInterp extends RuntimeAlgebra[IO] {
  override def createRuntime(params: CreateRuntimeParams)(
    implicit ev: Ask[IO, AppContext]
  ): IO[Option[CreateGoogleRuntimeResponse]] = ???

  override def deleteRuntime(params: DeleteRuntimeParams)(
    implicit ev: Ask[IO, AppContext]
  ): IO[Option[Operation]] = IO.pure(None)

  override def finalizeDelete(params: FinalizeDeleteParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] =
    IO.unit

  override def stopRuntime(
    params: StopRuntimeParams
  )(implicit ev: Ask[IO, AppContext]): IO[Option[Operation]] =
    IO.pure(None)

  override def startRuntime(params: StartRuntimeParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] = ???

  override def updateMachineType(params: UpdateMachineTypeParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] =
    ???

  override def updateDiskSize(params: UpdateDiskSizeParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] = ???

  override def resizeCluster(params: ResizeClusterParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] = ???
}

object GceInterp extends BaseFakeGceInterp
