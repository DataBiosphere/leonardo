package org.broadinstitute.dsde.workbench.leonardo
package monitor

import java.util.concurrent.TimeUnit

import cats.effect.IO
import com.google.cloud.compute.v1.{AccessConfig, Items, Metadata, NetworkInterface, Operation}
import org.broadinstitute.dsde.workbench.DoneCheckable
import org.broadinstitute.dsde.workbench.google2.streamFUntilDone
import org.broadinstitute.dsde.workbench.leonardo.db.clusterErrorQuery
import org.broadinstitute.dsde.workbench.leonardo.http.userScriptStartupOutputUriMetadataKey
import org.broadinstitute.dsde.workbench.model.google.GcsPath
import org.scalatest.EitherValues
import cats.mtl.ApplicativeAsk
import com.google.cloud.compute.v1.Instance
import org.broadinstitute.dsde.workbench.google2.{GoogleComputeService, InstanceName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao.{MockToolDAO, ToolDAO}
import org.broadinstitute.dsde.workbench.leonardo.dao.google.MockGoogleComputeService
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.model.{
  LeoAuthProvider,
  NotebookClusterActions,
  ProjectActions,
  ServiceAccountProvider
}
import org.broadinstitute.dsde.workbench.model
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsObjectName, GoogleProject}
import org.scalatest.{FlatSpec, Matchers}
import org.broadinstitute.dsde.workbench.leonardo.util.QueueFactory.makePublisherQueue
import org.broadinstitute.dsde.workbench.leonardo.util.{
  CreateRuntimeParams,
  CreateRuntimeResponse,
  DeleteRuntimeParams,
  FinalizeDeleteParams,
  GetRuntimeStatusParams,
  ResizeClusterParams,
  RuntimeAlgebra,
  StartRuntimeParams,
  StopRuntimeParams,
  UpdateDiskSizeParams,
  UpdateMachineTypeParams
}
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO

import scala.concurrent.ExecutionContext.Implicits.global
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.http.nowInstant

import scala.concurrent.duration._
import GceRuntimeMonitorInterp._

class GceRuntimeMonitorSpec extends FlatSpec with Matchers with TestComponent with LeonardoTestSuite with EitherValues {
  implicit val appContext = ApplicativeAsk.const[IO, AppContext](AppContext.generate[IO].unsafeRunSync())
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
      ctx <- appContext.ask
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
      res4 shouldBe (UserScriptsValidationResult.Error("user script gs://failure/object_output failed"))
      res5 shouldBe (UserScriptsValidationResult.CheckAgain(
        "user script gs://nonExistent/object_output hasn't finished yet"
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
      ctx <- appContext.ask
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
      res4 shouldBe (UserScriptsValidationResult.Error("user startup script gs://failure/object_output failed"))
      res5 shouldBe (UserScriptsValidationResult.CheckAgain(
        "user startup script gs://nonExistent/object_output hasn't finished yet"
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
      serviceAccountInfo = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1).copy(stagingBucket = GcsBucketName("failure"))),
      jupyterUserScriptUri =
        Some(UserScriptPath.Gcs(GcsPath(GcsBucketName("failure"), GcsObjectName("userscript_output.txt")))),
      status = RuntimeStatus.Creating
    )

    val computeService: GoogleComputeService[IO] = new MockGoogleComputeService {
      override def getInstance(project: GoogleProject, zone: ZoneName, instanceName: InstanceName)(
        implicit ev: ApplicativeAsk[IO, TraceId]
      ): IO[Option[Instance]] = IO.pure(Some(readyInstance))
    }

    val res = for {
      start <- nowInstant[IO]
      monitor = gceRuntimeMonitor(googleComputeService = computeService)
      savedRuntime <- IO(runtime.save())
      _ <- monitor.process(savedRuntime.id).compile.drain //start monitoring process
      afterMonitor <- nowInstant
      status <- clusterQuery.getClusterStatus(savedRuntime.id).transaction
      error <- clusterErrorQuery.get(savedRuntime.id).transaction
    } yield {
      (afterMonitor.toEpochMilli - start.toEpochMilli < 5000) shouldBe true // initial delay in tests is 2 seconds and 1 second polling interval, the stream should terminate after a few more checks
      status shouldBe Some(RuntimeStatus.Error)
      error.head.errorMessage shouldBe s"user script gs://failure/userscript_output.txt failed"
    }

    res.unsafeRunSync
  }

  // process, Creating
  it should "fail Creating if user startup script failed" in isolatedDbTest {
    val runtime = makeCluster(1).copy(
      serviceAccountInfo = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1).copy(stagingBucket = GcsBucketName("staging_bucket"))),
      jupyterStartUserScriptUri = Some(
        UserScriptPath
          .Gcs(GcsPath(GcsBucketName("staging_bucket"), GcsObjectName("failed_userstartupscript_output.txt")))
      ),
      status = RuntimeStatus.Creating
    )

    val computeService: GoogleComputeService[IO] = new MockGoogleComputeService {
      override def getInstance(project: GoogleProject, zone: ZoneName, instanceName: InstanceName)(
        implicit ev: ApplicativeAsk[IO, TraceId]
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
      savedRuntime <- IO(runtime.save())
      _ <- monitor.process(savedRuntime.id).compile.drain //start monitoring process
      afterMonitor <- nowInstant
      status <- clusterQuery.getClusterStatus(savedRuntime.id).transaction
      error <- clusterErrorQuery.get(savedRuntime.id).transaction
    } yield {
      (afterMonitor.toEpochMilli - start.toEpochMilli < 5000) shouldBe true // initial delay in tests is 2 seconds and 1 second polling interval, the stream should terminate after a few more checks
      status shouldBe Some(RuntimeStatus.Error)
      error.head.errorMessage shouldBe s"user startup script gs://staging_bucket/failed_userstartupscript_output.txt failed"
    }

    res.unsafeRunSync
  }

  // process, Creating
  it should "will check again if instance still exists when trying to Creating one" in isolatedDbTest {
    val runtime = makeCluster(1).copy(
      serviceAccountInfo = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Creating
    )

    def computeService(start: Long): GoogleComputeService[IO] = new MockGoogleComputeService {
      override def getInstance(project: GoogleProject, zone: ZoneName, instanceName: InstanceName)(
        implicit ev: ApplicativeAsk[IO, TraceId]
      ): IO[Option[Instance]] = {
        val beforeInstance = None
        val runningInstance = readyInstance

        for {
          now <- timer.clock.realTime(TimeUnit.MILLISECONDS)
          res <- if (now - start < 5000)
            IO.pure(beforeInstance)
          else IO.pure(Some(runningInstance))
        } yield res
      }
    }

    val res = for {
      start <- nowInstant[IO]
      monitor = gceRuntimeMonitor(googleComputeService = computeService(start.toEpochMilli))
      savedRuntime <- IO(runtime.save())
      _ <- monitor.process(savedRuntime.id).compile.drain //start monitoring process
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
      serviceAccountInfo = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Starting
    )

    def computeService(start: Long): GoogleComputeService[IO] = new MockGoogleComputeService {
      override def getInstance(project: GoogleProject, zone: ZoneName, instanceName: InstanceName)(
        implicit ev: ApplicativeAsk[IO, TraceId]
      ): IO[Option[Instance]] = {
        val beforeInstance = Instance.newBuilder().setStatus("TERMINATED").build()

        for {
          now <- timer.clock.realTime(TimeUnit.MILLISECONDS)
          res <- if (now - start < 5000)
            IO.pure(Some(beforeInstance))
          else IO.pure(Some(readyInstance))
        } yield res
      }
    }

    val res = for {
      start <- nowInstant[IO]
      monitor = gceRuntimeMonitor(googleComputeService = computeService(start.toEpochMilli))
      savedRuntime <- IO(runtime.save())
      _ <- monitor.process(savedRuntime.id).compile.drain //start monitoring process
      afterMonitor <- nowInstant
      status <- clusterQuery.getClusterStatus(savedRuntime.id).transaction
    } yield {
      (afterMonitor.toEpochMilli - start.toEpochMilli > 5000) shouldBe true // For 5 seconds, google is returning terminated no instance found
      status shouldBe Some(RuntimeStatus.Running)
    }

    res.unsafeRunSync()
  }

  it should "fail Starting if user startup script failed" in isolatedDbTest {
    val runtime = makeCluster(1).copy(
      serviceAccountInfo = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1).copy(stagingBucket = GcsBucketName("staging_bucket"))),
      jupyterStartUserScriptUri = Some(
        UserScriptPath
          .Gcs(GcsPath(GcsBucketName("staging_bucket"), GcsObjectName("failed_userstartupscript_output.txt")))
      ),
      status = RuntimeStatus.Starting
    )

    val computeService: GoogleComputeService[IO] = new MockGoogleComputeService {
      override def getInstance(project: GoogleProject, zone: ZoneName, instanceName: InstanceName)(
        implicit ev: ApplicativeAsk[IO, TraceId]
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
      savedRuntime <- IO(runtime.save())
      _ <- monitor.process(savedRuntime.id).compile.drain //start monitoring process
      afterMonitor <- nowInstant
      status <- clusterQuery.getClusterStatus(savedRuntime.id).transaction
      error <- clusterErrorQuery.get(savedRuntime.id).transaction
    } yield {
      (afterMonitor.toEpochMilli - start.toEpochMilli < 5000) shouldBe true // initial delay in tests is 2 seconds and 1 second polling interval, the stream should terminate after a few more checks
      status shouldBe Some(RuntimeStatus.Error)
      error.head.errorMessage shouldBe s"user startup script gs://staging_bucket/failed_userstartupscript_output.txt failed"
    }

    res.unsafeRunSync
  }

  // process
  it should "exit monitor if status is not monitored" in isolatedDbTest {
    val runtime = makeCluster(1).copy(
      serviceAccountInfo = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Stopped
    )

    val monitor = gceRuntimeMonitor()
    val savedRuntime = runtime.save()
    val res = for {
      now <- nowInstant[IO]
      _ <- monitor.process(savedRuntime.id).compile.drain //start monitoring process
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
      serviceAccountInfo = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Stopping
    )

    val monitor = gceRuntimeMonitor()
    val savedRuntime = runtime.save()
    val res = for {
      now <- nowInstant[IO]
      _ <- monitor.process(savedRuntime.id).compile.drain //start monitoring process
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
      serviceAccountInfo = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Stopping
    )

    val computeService = new MockGoogleComputeService {
      override def getInstance(project: GoogleProject, zone: ZoneName, instanceName: InstanceName)(
        implicit ev: ApplicativeAsk[IO, TraceId]
      ): IO[Option[Instance]] = {
        val instance = Instance.newBuilder().setStatus("Terminated").build()
        IO.pure(Some(instance))
      }
    }
    val queue = makePublisherQueue()
    val monitor = gceRuntimeMonitor(queue = queue, googleComputeService = computeService)
    val savedRuntime = runtime.save()
    val res = for {
      now <- nowInstant[IO]
      _ <- monitor.process(savedRuntime.id).compile.drain //start monitoring process
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
      serviceAccountInfo = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Stopping
    )

    val res = for {
      start <- nowInstant[IO]
      monitor = gceRuntimeMonitor(
        googleComputeService =
          computeService(start.toEpochMilli, Some(GceInstanceStatus.Running), Some(GceInstanceStatus.Terminated))
      )
      savedRuntime <- IO(runtime.save())
      _ <- monitor.process(savedRuntime.id).compile.drain //start monitoring process
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
      serviceAccountInfo = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Deleting
    )

    val monitor = gceRuntimeMonitor()
    val savedRuntime = runtime.save()
    val res = for {
      now <- nowInstant[IO]
      _ <- monitor.process(savedRuntime.id).compile.drain //start monitoring process
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
      serviceAccountInfo = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Deleting
    )

    def computeService(start: Long): GoogleComputeService[IO] = new MockGoogleComputeService {
      override def getInstance(project: GoogleProject, zone: ZoneName, instanceName: InstanceName)(
        implicit ev: ApplicativeAsk[IO, TraceId]
      ): IO[Option[Instance]] = {
        val runningInstance = Instance.newBuilder().setStatus("Running").build()

        for {
          now <- timer.clock.realTime(TimeUnit.MILLISECONDS)
          res <- if (now - start < 5000)
            IO.pure(Some(runningInstance))
          else IO.pure(None)
        } yield res
      }
    }

    val res = for {
      start <- nowInstant[IO]
      monitor = gceRuntimeMonitor(googleComputeService = computeService(start.toEpochMilli))
      savedRuntime <- IO(runtime.save())
      _ <- monitor.process(savedRuntime.id).compile.drain //start monitoring process
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
      serviceAccountInfo = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Deleted
    )

    val op = com.google.cloud.compute.v1.Operation.newBuilder().build()
    val monitor = gceRuntimeMonitor()
    val res = for {
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

  it should "monitor Deleting successfully" in {
    val runtime = makeCluster(2).copy(
      serviceAccountInfo = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(2)),
      status = RuntimeStatus.Deleting
    )

    val initialOp = com.google.cloud.compute.v1.Operation.newBuilder().setStatus("PENDING").build()

    def computeService(start: Long): GoogleComputeService[IO] = new MockGoogleComputeService {
      override def pollOperation(project: GoogleProject, operation: Operation, delay: FiniteDuration, maxAttempts: Int)(
        implicit ev: ApplicativeAsk[IO, TraceId],
        doneEv: DoneCheckable[Operation]
      ): fs2.Stream[IO, Operation] = {
        val afterOperation = com.google.cloud.compute.v1.Operation.newBuilder().setStatus("DONE").build()

        val res = for {
          now <- timer.clock.realTime(TimeUnit.MILLISECONDS)
          res <- if (now - start < 4000)
            IO.pure(operation)
          else IO.pure(afterOperation)
        } yield res

        streamFUntilDone(res, maxAttempts, delay)
      }
    }

    val res = for {
      start <- nowInstant[IO]
      monitor = gceRuntimeMonitor(googleComputeService = computeService(start.toEpochMilli))
      savedRuntime <- IO(runtime.save())
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
      serviceAccountInfo = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(2)),
      status = RuntimeStatus.Deleting
    )

    val computeService: GoogleComputeService[IO] = new MockGoogleComputeService {
      override def pollOperation(project: GoogleProject, operation: Operation, delay: FiniteDuration, maxAttempts: Int)(
        implicit ev: ApplicativeAsk[IO, TraceId],
        doneEv: DoneCheckable[Operation]
      ): fs2.Stream[IO, Operation] =
        streamFUntilDone(IO(operation), maxAttempts, delay)
    }

    val op = com.google.cloud.compute.v1.Operation.newBuilder().setStatus("PENDING").build()

    val res = for {
      start <- nowInstant[IO]
      monitor = gceRuntimeMonitor(googleComputeService = computeService)
      savedRuntime <- IO(runtime.save())
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
    queue: fs2.concurrent.InspectableQueue[IO, LeoPubsubMessage] = makePublisherQueue(),
    googleComputeService: GoogleComputeService[IO] = MockGoogleComputeService
  ): GceRuntimeMonitorInterp[IO] = {
    val config =
      Config.gceMonitorConfig.copy(initialDelay = 2 seconds, pollingInterval = 1 seconds, pollCheckMaxAttempts = 5)
    new GceRuntimeMonitorInterp[IO](
      config,
      googleComputeService,
      MockAuthProvider,
      FakeGoogleStorageService,
      GceInterp
    )
  }

  def computeService(start: Long,
                     beforeStatus: Option[GceInstanceStatus],
                     afterStatus: Option[GceInstanceStatus]): GoogleComputeService[IO] = new MockGoogleComputeService {
    override def getInstance(project: GoogleProject, zone: ZoneName, instanceName: InstanceName)(
      implicit ev: ApplicativeAsk[IO, TraceId]
    ): IO[Option[Instance]] = {
      val beforeInstance = beforeStatus.map(s => Instance.newBuilder().setStatus(s.toString).build())
      val afterInstance = afterStatus.map(s => Instance.newBuilder().setStatus(s.toString).build())

      for {
        now <- timer.clock.realTime(TimeUnit.MILLISECONDS)
        res <- if (now - start < 5000)
          IO.pure(beforeInstance)
        else IO.pure(afterInstance)
      } yield res
    }
  }
}

object MockAuthProvider extends LeoAuthProvider[IO] {
  override def serviceAccountProvider: ServiceAccountProvider[IO] = ???
  override def hasProjectPermission(
    userInfo: UserInfo,
    action: ProjectActions.ProjectAction,
    googleProject: GoogleProject
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Boolean] = ???
  override def hasNotebookClusterPermission(
    internalId: RuntimeInternalId,
    userInfo: UserInfo,
    action: NotebookClusterActions.NotebookClusterAction,
    googleProject: GoogleProject,
    runtimeName: RuntimeName
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Boolean] = ???
  override def filterUserVisibleClusters(userInfo: UserInfo, clusters: List[(GoogleProject, RuntimeInternalId)])(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[List[(GoogleProject, RuntimeInternalId)]] = ???
  override def notifyClusterCreated(internalId: RuntimeInternalId,
                                    creatorEmail: WorkbenchEmail,
                                    googleProject: GoogleProject,
                                    runtimeName: RuntimeName)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] = ???
  override def notifyClusterDeleted(internalId: RuntimeInternalId,
                                    userEmail: WorkbenchEmail,
                                    creatorEmail: WorkbenchEmail,
                                    googleProject: GoogleProject,
                                    runtimeName: RuntimeName)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    IO.unit
}

object GceInterp extends RuntimeAlgebra[IO] {
  override def createRuntime(params: CreateRuntimeParams)(
    implicit ev: ApplicativeAsk[IO, AppContext]
  ): IO[CreateRuntimeResponse] = ???

  override def getRuntimeStatus(params: GetRuntimeStatusParams)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[RuntimeStatus] = ???

  override def deleteRuntime(params: DeleteRuntimeParams)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Option[Operation]] = IO.pure(None)

  override def finalizeDelete(params: FinalizeDeleteParams)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    IO.unit

  override def stopRuntime(params: StopRuntimeParams)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Option[Operation]] =
    IO.pure(None)

  override def startRuntime(params: StartRuntimeParams)(implicit ev: ApplicativeAsk[IO, AppContext]): IO[Unit] = ???

  override def updateMachineType(params: UpdateMachineTypeParams)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    ???

  override def updateDiskSize(params: UpdateDiskSizeParams)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] = ???

  override def resizeCluster(params: ResizeClusterParams)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] = ???
}
