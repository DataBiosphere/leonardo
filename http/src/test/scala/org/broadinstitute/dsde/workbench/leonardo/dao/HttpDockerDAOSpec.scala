package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.IO
import org.broadinstitute.dsde.workbench.leonardo.ContainerImage.{DockerHub, GCR}
import org.broadinstitute.dsde.workbench.leonardo.LeonardoTestSuite
import org.broadinstitute.dsde.workbench.leonardo.RuntimeImageType.{Jupyter, RStudio}
import org.broadinstitute.dsde.workbench.leonardo.http.service.InvalidImage
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.client.middleware.Logger
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.ExecutionContext.global

class HttpDockerDAOSpec extends FlatSpec with Matchers with BeforeAndAfterAll with LeonardoTestSuite {
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

  def withDockerDAO(testCode: HttpDockerDAO[IO] => Any): Unit = {
    val dockerDAOResource = for {
      client <- BlazeClientBuilder[IO](global).resource
      clientWithLogging = Logger[IO](logHeaders = true, logBody = false)(client)
      dockerDAO = HttpDockerDAO[IO](clientWithLogging)
    } yield dockerDAO

    dockerDAOResource.use(dao => IO(testCode(dao))).unsafeRunSync()
  }

  Map(Jupyter -> jupyterImages, RStudio -> rstudioImages).foreach {
    case (tool, images) =>
      images.foreach { image =>
        it should s"detect tool=$tool for image $image" in withDockerDAO { dockerDAO =>
          val response = dockerDAO.detectTool(image).unsafeRunSync()
          response shouldBe tool
        }
      }
  }

  it should s"detect ImageParseException" in withDockerDAO { dockerDAO =>
    val image = GCR("us.gcr.io/anvil-gcr-public/anvil-rstudio-base") // non existent tag
    val res = for {
      tid <- traceId.ask
      response <- dockerDAO.detectTool(image).attempt
    } yield {
      response shouldBe Left(ImageParseException(tid, image))
    }
    res.unsafeRunSync()
  }

  it should s"detect invalid GCR image if image doesn't have proper environment variables set" in withDockerDAO {
    dockerDAO =>
      val image = GCR("us.gcr.io/broad-dsp-gcr-public/welder-server:latest") // not a supported tool
      val res = for {
        tid <- traceId.ask
        response <- dockerDAO.detectTool(image).attempt
      } yield {
        response shouldBe Left(InvalidImage(tid, image))
      }
      res.unsafeRunSync()
  }

  it should s"detect invalid dockerhub image if image doesn't have proper environment variables set" in withDockerDAO {
    dockerDAO =>
      val image = DockerHub("library/nginx:latest") // not a supported tool
      val res = for {
        tid <- traceId.ask
        response <- dockerDAO.detectTool(image).attempt
      } yield {
        response shouldBe Left(InvalidImage(tid, image))
      }
      res.unsafeRunSync()
  }
}
