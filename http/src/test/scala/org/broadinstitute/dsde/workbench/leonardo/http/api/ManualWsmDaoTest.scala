package org.broadinstitute.dsde.workbench.leonardo.http.api

import java.time.Instant

import cats.mtl.Ask
import cats.effect.{IO, Resource}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.dsde.workbench.model.TraceId
import java.util.UUID

import cats.effect.std.Semaphore
import com.azure.core.management.Region
import org.broadinstitute.dsde.workbench.leonardo.{WorkspaceId, AppContext}
import org.broadinstitute.dsde.workbench.leonardo.config.HttpWsmDaoConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.{CreateIpRequest, ControlledResourceCommonFields, CreateIpRequestData, CloningInstructions, AccessScope, ControlledResourceName, ManagedBy, AzureIpName, HttpWsmDao, ControlledResourceDescription}
import org.broadinstitute.dsde.workbench.openTelemetry.FakeOpenTelemetryMetricsInterpreter
import org.broadinstitute.dsde.workbench.util2.ExecutionContexts
import org.http4s.Uri
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.client.Client
import org.http4s.client.middleware.{Retry, Logger, RetryPolicy}

import scala.concurrent.duration._

class ManualWsmDaoTest(region: Region = Region.fromName("westcentralus"),
                       workspaceId: WorkspaceId = WorkspaceId("jc-ws1")
                      ) {

  implicit val traceId = Ask.const[IO, AppContext](AppContext(TraceId(UUID.randomUUID()), Instant.now()))

  implicit def logger = Slf4jLogger.getLogger[IO]
  implicit val metrics = FakeOpenTelemetryMetricsInterpreter

  val config = HttpWsmDaoConfig(Uri.unsafeFromString("http://localhost:8080"))
  val client: Resource[IO, Client[IO]] =
    for {
      blockingEc <- ExecutionContexts.cachedThreadPool[IO]
      retryPolicy = RetryPolicy[IO](RetryPolicy.exponentialBackoff(30 seconds, 5))
      client <- BlazeClientBuilder[IO](blockingEc).resource.map(c => Retry(retryPolicy)(c))
    } yield Logger[IO](logHeaders = true, logBody = true)(client)


  def getCommonFields(prefix: String): ControlledResourceCommonFields = {
    ControlledResourceCommonFields(
      ControlledResourceName(uniqueName(prefix)),
      ControlledResourceDescription(prefix),
      CloningInstructions.Nothing,
      AccessScope.PrivateAccess,
      ManagedBy.User, //TODO: Application
      None
    )
  }

  def callCreateIp(): Unit = {
    client.use { http =>
      val httpWsmDao = new HttpWsmDao(http, config)

      val createIpReq = CreateIpRequest(workspaceId,
        getCommonFields("testip"),
        CreateIpRequestData(AzureIpName(uniqueName("ip")),
          region))

      httpWsmDao.createIp(createIpReq)
    }
  }

  def uniqueName(prefix: String): String = prefix + "-" + UUID.randomUUID.toString

}
