package org.broadinstitute.dsde.workbench.leonardo.dao

import java.util.UUID

import cats.effect.IO
import cats.mtl.ApplicativeAsk
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterTool.{Jupyter, RStudio}
import org.broadinstitute.dsde.workbench.leonardo.model.ContainerImage.{DockerHub, GCR}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.client.middleware.Logger
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.ExecutionContext.global

class HttpDockerDAOSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  implicit val cs = IO.contextShift(global)
  implicit def unsafeLogger = Slf4jLogger.getLogger[IO]
  implicit val traceId = ApplicativeAsk.const[IO, TraceId](TraceId(UUID.randomUUID()))

  val testImages = Map(
    Some(Jupyter) -> List(
      // dockerhub no tag
      DockerHub("broadinstitute/leonardo-notebooks"),
      // dockerhub with tag
      DockerHub("broadinstitute/leonardo-notebooks:dev"),
      // dockerhub with sha TODO
      DockerHub(
        "broadinstitute/leonardo-notebooks@sha256:bb959cf74f31d2a10f7bb8ee0f0754138d7c90f7ed8a92c3697ac994ff8b40b7"
      ),
      // gcr with tag
      GCR("us.gcr.io/broad-dsp-gcr-public/leonardo-jupyter:dev"),
      // gcr with sha TODO
      GCR(
        "us.gcr.io/broad-dsp-gcr-public/leonardo-jupyter@sha256:fa11b7c528304726985b4ad4cb4cb4d8b9a2fbf7c5547671ef495f414564727c"
      )
    ),
    Some(RStudio) -> List(
      // gcr with tag
      GCR("us.gcr.io/anvil-gcr-public/anvil-rstudio-base:0.0.1"),
      // gcr with sha TODO
      GCR(
        "us.gcr.io/anvil-gcr-public/anvil-rstudio-base@sha256:98ed9ed3072ab20633f5212ddc7201c0df369db28fd669a509987e0744bcef2c"
      )
    ),
    None -> List(
      // dockerhub
      DockerHub("library/nginx:latest"),
      // public gcr
      GCR("us.gcr.io/broad-dsp-gcr-public/welder-server:latest"),
      // private gcr TODO
      GCR("gcr.io/broad-dsp-gcr-public/sam:dev"),
      // gcr no tag
      GCR("us.gcr.io/anvil-gcr-public/anvil-rstudio-base")
    )
  )

  def withDockerDAO(testCode: HttpDockerDAO[IO] => Any): Unit = {
    val dockerDAOResource = for {
      client <- BlazeClientBuilder[IO](global).resource
      clientWithLogging = Logger[IO](logHeaders = true, logBody = false)(client)
      dockerDAO = HttpDockerDAO[IO](clientWithLogging)
    } yield dockerDAO

    dockerDAOResource.use(dao => IO(testCode(dao))).unsafeRunSync()
  }

  testImages.foreach {
    case (tool, images) =>
      images.foreach { image =>
        it should s"detect tool=$tool for image $image" in withDockerDAO { dockerDAO =>
          val response = dockerDAO.detectTool(image).attempt.unsafeRunSync().toOption.flatten
          response shouldBe tool
        }
      }
  }
}
