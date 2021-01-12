package org.broadinstitute.dsde.workbench.leonardo.runtimes

import java.util.UUID

import cats.effect.IO
import cats.implicits._
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.google2.DataprocRole.{Master, SecondaryWorker, Worker}
import org.broadinstitute.dsde.workbench.google2.{
  DataprocClusterName,
  GoogleDataprocInterpreter,
  MachineTypeName,
  RegionName
}
import org.broadinstitute.dsde.workbench.leonardo.LeonardoApiClient.defaultCreateRuntime2Request
import org.broadinstitute.dsde.workbench.leonardo.http.RuntimeConfigRequest
import org.broadinstitute.dsde.workbench.leonardo.notebooks.{NotebookTestUtils, Python3}
import org.broadinstitute.dsde.workbench.leonardo.{
  DataprocClusterStatus,
  DiskSize,
  GPAllocFixtureSpec,
  LeonardoTestUtils
}
import org.broadinstitute.dsde.workbench.model.TraceId
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

  "Workers and preemptible workers should be created" in { project =>
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
      )
    )

    val res = googleDataprocService.use { dataproc =>
      withNewRuntime(project, runtimeName, createRuntimeRequest) { runtime =>
        for {
          // check cluster status in Dataproc
          clusterOpt <- dataproc.getCluster(project,
                                            RegionName("us-central1"),
                                            DataprocClusterName(runtime.clusterName.asString))
          cluster <- IO.fromOption(clusterOpt)(
            fail(s"Cluster not found in dataproc: ${project.value}/${runtime.clusterName.asString}")
          )
          status <- IO.fromOption(DataprocClusterStatus.withNameInsensitiveOption(cluster.getStatus.getState.name))(
            fail(s"Unknown Dataproc status ${cluster.getStatus.getState.name}")
          )
          _ <- IO(status shouldBe DataprocClusterStatus.Running)

          // check cluster instances in Dataproc
          instances = GoogleDataprocInterpreter.getAllInstanceNames(cluster)
          _ <- IO(instances.size shouldBe 3)
          _ <- instances.toList.traverse {
            case (k, v) =>
              IO(
                k.role match {
                  case Master => v.size shouldBe 1
                  case Worker => v.size shouldBe 2
                  case SecondaryWorker =>
                    v.size shouldBe 5
                    k.isPreemptible shouldBe true
                }
              )
          }

          // check output of yarn node -list command
          _ <- IO(
            withWebDriver { implicit driver =>
              withNewNotebook(runtime, Python3) { notebookPage =>
                val output = notebookPage.executeCell("""!yarn node -list | grep Total""")
                output.get should include("Total Nodes:7")
              }
            }
          )
        } yield ()
      }
    }

    res.unsafeRunSync()
  }

}
