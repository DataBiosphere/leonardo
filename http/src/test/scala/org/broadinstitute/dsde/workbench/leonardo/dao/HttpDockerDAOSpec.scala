package org.broadinstitute.dsde.workbench.leonardo
package dao

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.circe.CursorOp.DownField
import io.circe.DecodingFailure
import io.circe.parser.decode
import org.broadinstitute.dsde.workbench.leonardo.ContainerRegistry.{DockerHub, GCR, GHCR}
import org.broadinstitute.dsde.workbench.leonardo.RuntimeImageType.{Jupyter, RStudio}
import org.broadinstitute.dsde.workbench.leonardo.dao.HttpDockerDAO._
import org.broadinstitute.dsde.workbench.leonardo.model.InvalidImage
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.client.middleware.Logger
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.broadinstitute.dsde.workbench.leonardo.http.ctxConversion
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext

class HttpDockerDAOSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with LeonardoTestSuite {
  val jupyterImages = List(
    // TODO this will break if AoU moves off Dockerhub and we delete these images
    // dockerhub with tag
    ContainerImage("broadinstitute/terra-jupyter-aou:1.0.17", DockerHub),
    ContainerImage("kkimler/r4.0_tca:2020_11_09", DockerHub), // added after a bug fix (see ticket IA-2439)
    // dockerhub with sha
    // TODO: shas are currently not working
//    DockerHub(
//      "broadinstitute/leonardo-notebooks@sha256:bb959cf74f31d2a10f7bb8ee0f0754138d7c90f7ed8a92c3697ac994ff8b40b7"
//    ),
    // gcr with tag
    ContainerImage("us.gcr.io/broad-dsp-gcr-public/leonardo-jupyter:dev", GCR),
    ContainerImage("us.gcr.io/broad-dsp-gcr-public/terra-jupyter-python:0.0.4", GCR),
    ContainerImage("us.gcr.io/broad-dsp-gcr-public/terra-jupyter-r:0.0.5", GCR),
    ContainerImage("us.gcr.io/broad-dsp-gcr-public/terra-jupyter-gatk:0.0.4", GCR),
    // gcr with sha
    // TODO shas are currently not working
//    GCR(
//      "us.gcr.io/broad-dsp-gcr-public/leonardo-jupyter@sha256:fa11b7c528304726985b4ad4cb4cb4d8b9a2fbf7c5547671ef495f414564727c"
//    ),
    // ghcr with tag
    // Taken from workspace https://app.terra.bio/#workspaces/uk-biobank-sek/ml4h-toolkit-for-machine-learning-on-clinical-data
    ContainerImage("ghcr.io/broadinstitute/ml4h/ml4h_terra:20210104_224422", GHCR)
  )

  val rstudioImages = List(
    // dockerhub no tag
    ContainerImage("rtitle/anvil-rstudio-base", DockerHub),
    // dockerhub with tag
    ContainerImage("rtitle/anvil-rstudio-base:0.0.1", DockerHub),
    // dockerhub with sha
    // TODO: shas are currently not working
//    DockerHub(
//      "rocker/rstudio@sha256:5aea617714eb38a97a21de652ab667c6d7bb486d7468a4ab6b4d515154fec383"
//    ),
    // gcr with tag
    ContainerImage("us.gcr.io/anvil-gcr-public/anvil-rstudio-base:0.0.1", GCR)
    // gcr with sha
    // TODO shas are currently not working
//    GCR(
//      "us.gcr.io/anvil-gcr-public/anvil-rstudio-base@sha256:98ed9ed3072ab20633f5212ddc7201c0df369db28fd669a509987e0744bcef2c"
//    )
  )

  def withDockerDAO(testCode: HttpDockerDAO[IO] => Any): Unit = {
    val dockerDAOResource = for {
      client <- BlazeClientBuilder[IO](scala.concurrent.ExecutionContext.global).resource
      clientWithLogging = Logger[IO](logHeaders = true, logBody = false)(client)
      dockerDAO = HttpDockerDAO[IO](clientWithLogging)
    } yield dockerDAO

    dockerDAOResource.use(dao => IO(testCode(dao))).unsafeRunSync()
  }

  Map(Jupyter -> jupyterImages, RStudio -> rstudioImages).foreach { case (tool, images) =>
    images.foreach { image =>
      it should s"detect tool=$tool for image $image" in withDockerDAO { dockerDAO =>
        val response = IO.realTimeInstant.flatMap(n => dockerDAO.detectTool(image, None, n)).unsafeRunSync()
        response.imageType shouldBe tool
      }
    }
  }

  it should s"detect ImageParseException" in withDockerDAO { dockerDAO =>
    val image = ContainerImage("us.gcr.io/anvil-gcr-public/anvil-rstudio-base", GCR) // non existent tag
    val res = for {
      ctx <- appContext.ask[AppContext]
      response <- dockerDAO.detectTool(image, None, ctx.now).attempt
    } yield response shouldBe Left(ImageParseException(ctx.traceId, image))
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should s"detect invalid GCR image if image doesn't have proper environment variables set" in withDockerDAO {
    dockerDAO =>
      val image = ContainerImage("us.gcr.io/broad-dsp-gcr-public/welder-server:latest", GCR) // not a supported tool
      val res = for {
        ctx <- appContext.ask[AppContext]
        response <- dockerDAO.detectTool(image, None, ctx.now).attempt
      } yield response shouldBe Left(InvalidImage(ctx.traceId, image, None))
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should s"detect invalid dockerhub image if image doesn't have proper environment variables set" in withDockerDAO {
    dockerDAO =>
      val image = ContainerImage("library/nginx:latest", DockerHub) // not a supported tool
      val res = for {
        ctx <- appContext.ask[AppContext]
        response <- dockerDAO.detectTool(image, None, ctx.now).attempt
      } yield response shouldBe Left(InvalidImage(ctx.traceId, image, None))
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "detect invalid ghcr image if image doesn't have proper environment variables set" in withDockerDAO {
    dockerDAO =>
      val image = ContainerImage("ghcr.io/github/super-linter:latest", GHCR) // not a supported tool
      val res = for {
        ctx <- appContext.ask[AppContext]
        response <- dockerDAO.detectTool(image, None, ctx.now).attempt
      } yield response.isLeft shouldBe true
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "correctly decode 'container_config' field from Docker API response if it exists" in {
    val jsonString =
      """
        |{
        |  "container_config": {
        |     "Env": ["key1=value1", "key2=value2"]
        |  },
        |  "config": {
        |     "Env": ["key3=value3", "key4=value4"]
        |  }
        |}
        |""".stripMargin
    val expectedResult = ContainerConfigResponse(ContainerConfig(List(Env("key1", "value1"), Env("key2", "value2"))))
    decode[ContainerConfigResponse](jsonString) shouldBe Right(expectedResult)
  }

  it should "correctly decode 'config' field from Docker API response if container_config does not exist" in {
    val jsonString =
      """
        |{
        |  "config": {
        |     "Env": ["keyX=valueX", "keyY=valueY"]
        |  }
        |}
        |""".stripMargin
    val expectedResult = ContainerConfigResponse(ContainerConfig(List(Env("keyX", "valueX"), Env("keyY", "valueY"))))
    decode[ContainerConfigResponse](jsonString) shouldBe Right(expectedResult)
  }

  it should "fail to decode Docker API response if it does not contain 'container_config' or 'config' fields" in {
    val jsonString =
      """
        |{
        |  "unexpected_field": {
        |     "Env": ["keyA=valueA", "keyB=valueB"]
        |  }
        |}
        |""".stripMargin
    val expectedResult =
      // TODO [IA-4419] once we can upgrade Circe past 14.2/0.15.0-M1, restore human-readable "missing required field" expect.
      // See https://github.com/pbyrne84/scala-circe-error-rendering/blob/035972dc9407506d1de084421531668526ddff26/src/main/scala/com/github/pbyrne84/circe/rendering/CirceErrorRendering.scala#L40

      // DecodingFailure("Missing required field", List(DownField("config")))
      DecodingFailure("Attempt to decode value on failed cursor", List(DownField("config")))
    decode[ContainerConfigResponse](jsonString).swap.toOption.get.getMessage shouldBe expectedResult.leftSideValue
      .getMessage()
  }

  it should "fail to decode Docker API response if it does not contain 'Env' field" in {
    val jsonString =
      """
        |{
        |  "container_config": {
        |     "no_Env": ["keyFoo=valueFoo", "keyBaz=valueBaz"]
        |  }
        |}
        |""".stripMargin
    val expectedResult =
      // TODO [IA-4419] once we can upgrade Circe past 14.2/0.15.0-M1, restore human-readable "missing required field" expect.
      // See https://github.com/pbyrne84/scala-circe-error-rendering/blob/035972dc9407506d1de084421531668526ddff26/src/main/scala/com/github/pbyrne84/circe/rendering/CirceErrorRendering.scala#L40

      // DecodingFailure("Missing required field", List(DownField("Env")))
      DecodingFailure("Attempt to decode value on failed cursor", List(DownField("Env")))

    decode[ContainerConfigResponse](jsonString).swap.toOption.get.getMessage shouldBe expectedResult.leftSideValue
      .getMessage()
  }

  it should s"detect tool RStudio for image rtitle/anvil-rstudio-base:0.0.1" in withDockerDAO { dockerDAO =>
    val image = ContainerImage("rtitle/anvil-rstudio-base:0.0.1", DockerHub)
    val response = IO.realTimeInstant.flatMap(n => dockerDAO.detectTool(image, None, n)).unsafeRunSync()
    response.imageType shouldBe RStudio
  }
}
