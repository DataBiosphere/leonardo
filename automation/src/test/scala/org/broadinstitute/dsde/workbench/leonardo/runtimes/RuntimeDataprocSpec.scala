package org.broadinstitute.dsde.workbench.leonardo
package runtimes

import java.util.UUID

import cats.effect.IO
import cats.mtl.Ask
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.google2.DataprocRole.{Master, SecondaryWorker, Worker}
import org.broadinstitute.dsde.workbench.google2.{
  DataprocClusterName,
  GoogleDataprocInterpreter,
  GoogleDataprocService,
  MachineTypeName,
  RegionName
}
import org.broadinstitute.dsde.workbench.leonardo.LeonardoApiClient.defaultCreateRuntime2Request
import org.broadinstitute.dsde.workbench.leonardo.http.RuntimeConfigRequest
import org.broadinstitute.dsde.workbench.leonardo.notebooks.{NotebookTestUtils, Python3}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.http4s.client.Client
import org.http4s.headers.Authorization
import org.http4s.{AuthScheme, Credentials}
import org.scalatest.{DoNotDiscover, ParallelTestExecution}

@DoNotDiscover
class RuntimeDataprocSpec
    extends GPAllocFixtureSpec
    with ParallelTestExecution
    with LeonardoTestUtils
    with NotebookTestUtils {
  implicit val authTokenForOldApiClient = ronAuthToken
  implicit val auth: Authorization = Authorization(Credentials.Token(AuthScheme.Bearer, ronCreds.makeAuthToken().value))
  implicit val traceId = Ask.const[IO, TraceId](TraceId(UUID.randomUUID()))

  val dependencies = for {
    dataprocService <- googleDataprocService
    httpClient <- LeonardoApiClient.client
  } yield RuntimeDataprocSpecDependencies(httpClient, dataprocService)

  "should create a Dataproc cluster with workers and preemptible workers" in { project =>
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
        _ <- verifyDataproc(project, runtime.clusterName, dep.dataproc, 2, 5)

        // check output of yarn node -list command
        _ <- IO(
          withWebDriver { implicit driver =>
            withNewNotebook(runtime, Python3) { notebookPage =>
              val output = notebookPage.executeCell("""!yarn node -list""")
              output.get should include("Total Nodes:7")
            }
          }
        )

        _ <- LeonardoApiClient.deleteRuntime(project, runtimeName)
      } yield ()
    }

    res.unsafeRunSync()
  }

  "should stop/start a Dataproc cluster with workers and preemptible workers" in { project =>
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
        _ <- verifyDataproc(project, runtime.clusterName, dep.dataproc, 2, 5)

        // stop the cluster
        _ <- IO(stopAndMonitorRuntime(runtime.googleProject, runtime.clusterName))

        // preemptibles should be removed in Dataproc
        _ <- verifyDataproc(project, runtime.clusterName, dep.dataproc, 2, 0)

        // start the cluster
        _ <- IO(startAndMonitorRuntime(runtime.googleProject, runtime.clusterName))

        // preemptibles should be added in Dataproc
        _ <- verifyDataproc(project, runtime.clusterName, dep.dataproc, 2, 5)

        // check output of yarn node -list command
        _ <- IO(
          withWebDriver { implicit driver =>
            withNewNotebook(runtime, Python3) { notebookPage =>
              val output = notebookPage.executeCell("""!yarn node -list""")
              output.get should include("Total Nodes:7")
            }
          }
        )

        _ <- LeonardoApiClient.deleteRuntime(project, runtimeName)
      } yield ()
    }

    res.unsafeRunSync()
  }

  private def verifyDataproc(project: GoogleProject,
                             runtimeName: RuntimeName,
                             dataproc: GoogleDataprocService[IO],
                             expectedNumWorkers: Int,
                             expectedPreemptibles: Int): IO[Unit] =
    for {
      // check cluster status in Dataproc
      clusterOpt <- dataproc.getCluster(project, RegionName("us-central1"), DataprocClusterName(runtimeName.asString))
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

final case class RuntimeDataprocSpecDependencies(httpClient: Client[IO], dataproc: GoogleDataprocService[IO])
