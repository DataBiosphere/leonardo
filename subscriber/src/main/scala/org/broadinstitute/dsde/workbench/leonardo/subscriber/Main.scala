package org.broadinstitute.dsde.workbench.leonardo
package subscriber

import akka.actor.ActorSystem
import cats.Parallel
import cats.effect.concurrent.Semaphore
import cats.effect.{Blocker, Concurrent, ConcurrentEffect, ContextShift, ExitCode, IO, IOApp, Resource, Timer}
import com.google.api.services.compute.ComputeScopes
import com.google.api.services.container.ContainerScopes
import fs2.concurrent.InspectableQueue
import io.chrisdavenport.log4cats.StructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.dsde.workbench.google.GoogleCredentialModes.Json
import org.broadinstitute.dsde.workbench.google.{HttpGoogleIamDAO, HttpGoogleProjectDAO}
import org.broadinstitute.dsde.workbench.google2.GKEModels.KubernetesClusterId
import org.broadinstitute.dsde.workbench.google2.util.RetryPredicates
import org.broadinstitute.dsde.workbench.google2.{
  credentialResource,
  ComputePollOperation,
  Event,
  GKEService,
  GoogleComputeService,
  GooglePublisher,
  GoogleResourceService,
  GoogleSubscriber
}
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.Task
import org.broadinstitute.dsde.workbench.leonardo.algebra._
import org.broadinstitute.dsde.workbench.leonardo.db.{DbReference, DbReferenceInitializer}
import org.broadinstitute.dsde.workbench.leonardo.dns.KubernetesDnsCache
import org.broadinstitute.dsde.workbench.leonardo.subscriber.NonLeoMessageSubscriber.nonLeoMessageDecoder
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.broadinstitute.dsde.workbench.util2.ExecutionContexts
import org.broadinstitute.dsp.HelmInterpreter
import org.http4s.client.blaze
import org.http4s.client.middleware.{Retry, RetryPolicy, Logger => Http4sLogger}

import java.nio.file.Paths
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

object Main extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
    implicit val logger = Slf4jLogger.getLogger[IO]
    implicit val system = ActorSystem("leonardo")

    initDependencies("path").use(deps => deps.nonLeoMessageSubscriber.process.compile.drain.as(ExitCode.Success))
  }

  private def initDependencies[F[_]: StructuredLogger: Parallel: Concurrent: ContextShift: Timer](
    pathToCredentialJson: String
  )(implicit as: ActorSystem, F: ConcurrentEffect[F]): Resource[F, AppDependencies[F]] =
    for {
      config <- Resource.liftF(F.fromEither(Config.appConfig))

      blockingEc <- ExecutionContexts.cachedThreadPool[F]
      semaphore <- Resource.liftF(Semaphore[F](255L))
      blocker = Blocker.liftExecutionContext(blockingEc)

      implicit0(openTelemetry: OpenTelemetryMetrics[F]) <- OpenTelemetryMetrics
        .resource[F](config.application.serviceAccountFile, config.application.appName, blocker)

      concurrentDbAccessPermits <- Resource.liftF(Semaphore[F](config.mysql.concurrency))
      implicit0(dbRef: DbReference[F]) <- new DbReferenceInitializer[F]
        .init(config.mysql.liquibase, Config.configForSlick, concurrentDbAccessPermits, blocker)

      sslContext <- Resource.liftF(SslContextReader.getSSLContext())
      httpClientWithCustomSSL <- blaze.BlazeClientBuilder[F](blockingEc, Some(sslContext)).resource
      retryPolicy = RetryPolicy[F](RetryPolicy.exponentialBackoff(30 seconds, 5))
      clientWithRetryWithCustomSSL = Retry(retryPolicy)(httpClientWithCustomSSL)
      clientWithRetryAndLogging = Http4sLogger[F](logHeaders = true, logBody = false)(clientWithRetryWithCustomSSL)

      samDao = HttpSamDAO[F](clientWithRetryWithCustomSSL, config.authProviderConfig, blocker)

      credential <- credentialResource(pathToCredentialJson)
      scopedCredential = credential.createScoped(Seq(ComputeScopes.COMPUTE).asJava)
      googleComputeRetryPolicy = RetryPredicates.retryConfigWithPredicates(
        RetryPredicates.standardRetryPredicate,
        RetryPredicates.whenStatusCode(400)
      )
      googleComputeService <- GoogleComputeService.fromCredential(scopedCredential,
                                                                  blocker,
                                                                  semaphore,
                                                                  googleComputeRetryPolicy)

      credentialJson <- Resource.liftF(
        readFileToString(config.application.serviceAccountFile, blocker)
      )
      computePollOperation <- ComputePollOperation.resourceFromCredential(scopedCredential, blocker, semaphore)

      json = Json(credentialJson)
      googleProjectDAO = new HttpGoogleProjectDAO(config.application.appName, json, "google")
      googleIamDAO = new HttpGoogleIamDAO(config.application.appName, json, "google")

      asyncTasksQueue <- Resource.liftF(InspectableQueue.bounded[F, Task[F]](config.asyncTaskProcessor.queueBound))

      cryptoMiningUserPublisher <- GooglePublisher.resource[F](config.cryptominingPublisherConfig)

      gkeService <- GKEService.resource(config.application.serviceAccountFile, blocker, semaphore)
      kubeService <- org.broadinstitute.dsde.workbench.google2.KubernetesService
        .resource(config.application.serviceAccountFile, gkeService, blocker, semaphore)
      googleResourceService <- GoogleResourceService.resource[F](Paths.get(pathToCredentialJson), blocker, semaphore)

      kubernetesDnsCache = new KubernetesDnsCache(config.proxy, dbRef, config.dnsCache.kubernetes, blocker)

      helmClient = new HelmInterpreter[F](blocker, semaphore)
      galaxyDAO = new HttpGalaxyDAO(kubernetesDnsCache, clientWithRetryAndLogging)

      nodepoolLock <- Resource.liftF(
        KeyLock[F, KubernetesClusterId](config.gke.cluster.nodepoolLockCacheExpiryTime,
                                        config.gke.cluster.nodepoolLockCacheMaxSize,
                                        blocker)
      )

      vpcInterp = new VPCInterpreter(VPCInterpreterConfig(config.vpc),
                                     googleResourceService,
                                     googleComputeService,
                                     computePollOperation)

      gkeInterpConfig = GKEInterpreterConfig(
        config.securityFiles,
        config.gke.ingress,
        config.gke.galaxyApp,
        config.monitor.kubernetes,
        config.gke.cluster,
        config.proxy,
        config.gke.galaxyDisk,
        config.vpc
      )
      gkeAlg = new GKEInterpreter[F](
        gkeInterpConfig,
        vpcInterp,
        gkeService,
        kubeService,
        helmClient,
        galaxyDAO,
        credential.createScoped(Seq(ContainerScopes.CLOUD_PLATFORM).asJava),
        googleIamDAO,
        blocker,
        nodepoolLock
      )

      nonLeoMessageSubscriberQueue <- Resource.liftF(
        InspectableQueue.bounded[F, Event[NonLeoMessage]](200)
      )
      nonLeoMessageSubscriber <- GoogleSubscriber.resource(config.nonLeonardoMessageSubscriber,
                                                           nonLeoMessageSubscriberQueue)

      subscriber = new NonLeoMessageSubscriber[F](gkeAlg,
                                                  googleComputeService,
                                                  samDao,
                                                  nonLeoMessageSubscriber,
                                                  cryptoMiningUserPublisher,
                                                  asyncTasksQueue)
    } yield AppDependencies(subscriber)

}

final case class AppDependencies[F[_]](
  nonLeoMessageSubscriber: NonLeoMessageSubscriber[F]
)
