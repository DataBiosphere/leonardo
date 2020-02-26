package org.broadinstitute.dsde.workbench.leonardo.dao

import java.util.UUID

import cats.effect.IO
import cats.mtl.ApplicativeAsk
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.dsde.workbench.leonardo.ContainerImage.{DockerHub, GCR}
import org.broadinstitute.dsde.workbench.leonardo.RuntimeImageType.{Jupyter, RStudio}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.client.middleware.Logger
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.ExecutionContext.global

class HttpDockerDAOSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  implicit val cs = IO.contextShift(global)
  implicit def unsafeLogger = Slf4jLogger.getLogger[IO]
  implicit val traceId = ApplicativeAsk.const[IO, TraceId](TraceId(UUID.randomUUID()))

  val jupyterImages = List(
    // dockerhub no tag
    DockerHub("broadinstitute/leonardo-notebooks"),
    // dockerhub with tag
    DockerHub("broadinstitute/leonardo-notebooks:dev"),
    // dockerhub with sha
    // TODO: shas are currently not working
//    DockerHub(
//      "broadinstitute/leonardo-notebooks@sha256:bb959cf74f31d2a10f7bb8ee0f0754138d7c90f7ed8a92c3697ac994ff8b40b7"
//    ),
    // gcr with tag
    GCR("us.gcr.io/broad-dsp-gcr-public/leonardo-jupyter:dev"),
    GCR("us.gcr.io/broad-dsp-gcr-public/terra-jupyter-python:0.0.4"),
    GCR("us.gcr.io/broad-dsp-gcr-public/terra-jupyter-r:0.0.5"),
    GCR("us.gcr.io/broad-dsp-gcr-public/terra-jupyter-gatk:0.0.4")
    // gcr with sha
    // TODO shas are currently not working
//    GCR(
//      "us.gcr.io/broad-dsp-gcr-public/leonardo-jupyter@sha256:fa11b7c528304726985b4ad4cb4cb4d8b9a2fbf7c5547671ef495f414564727c"
//    )
  )

  val rstudioImages = List(
    // dockerhub no tag
    DockerHub("rtitle/anvil-rstudio-base"),
    // dockerhub with tag
    DockerHub("rtitle/anvil-rstudio-base:0.0.1"),
    // dockerhub with sha
    // TODO: shas are currently not working
//    DockerHub(
//      "rocker/rstudio@sha256:5aea617714eb38a97a21de652ab667c6d7bb486d7468a4ab6b4d515154fec383"
//    ),
    // gcr with tag
    GCR("us.gcr.io/anvil-gcr-public/anvil-rstudio-base:0.0.1")
    // gcr with sha
    // TODO shas are currently not working
//    GCR(
//      "us.gcr.io/anvil-gcr-public/anvil-rstudio-base@sha256:98ed9ed3072ab20633f5212ddc7201c0df369db28fd669a509987e0744bcef2c"
//    )
  )

  val unknownImages = List(
    // not a supported tool
    DockerHub("library/nginx:latest"),
    // not a supported tool
    GCR("us.gcr.io/broad-dsp-gcr-public/welder-server:latest"),
    // non existent tag
    GCR("us.gcr.io/anvil-gcr-public/anvil-rstudio-base")
  )

  def withDockerDAO(testCode: HttpDockerDAO[IO] => Any): Unit = {
    val dockerDAOResource = for {
      client <- BlazeClientBuilder[IO](global).resource
      clientWithLogging = Logger[IO](logHeaders = true, logBody = false)(client)
      dockerDAO = HttpDockerDAO[IO](clientWithLogging)
    } yield dockerDAO

    dockerDAOResource.use(dao => IO(testCode(dao))).unsafeRunSync()
  }

  Map(Some(Jupyter) -> jupyterImages, Some(RStudio) -> rstudioImages, None -> unknownImages).foreach {
    case (tool, images) =>
      images.foreach { image =>
        it should s"detect tool=$tool for image $image" in withDockerDAO { dockerDAO =>
          val response = dockerDAO.detectTool(image).attempt.unsafeRunSync().toOption.flatten
          response shouldBe tool
        }
      }
  }
}
