package org.broadinstitute.dsde.workbench.leonardo
package dao

import cats.effect.IO
import org.http4s.HttpApp
import org.http4s.client.middleware.Logger
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.http4s.client.Client
import org.http4s._
import org.http4s.blaze.client.BlazeClientBuilder
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import scala.concurrent.ExecutionContext.global

class HTTPAppDescriptorDAOSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with LeonardoTestSuite {
  // NOTE: If this is not a valid link, tests will fail
  val appYamlURI: Uri = Uri.uri("https://raw.githubusercontent.com/DataBiosphere/terra-app/main/apps/rstudio/app.yaml")
  // NOTE: no tests here actually use the http response from this URI
  val dummyRequestURI: Uri =
    Uri.uri("https://raw.githubusercontent.com/DataBiosphere/terra-app/main/apps/rstudio/app.yaml")
  val rstudioRespYaml =
    """name: rstudio
      |author: workbench-interactive-analysis@broadinstitute.org
      |description: |
      |  RStudio
      |version: 0.0.10
      |services:
      |  rstudio:
      |    image: "us.gcr.io/anvil-gcr-public/anvil-rstudio-bioconductor:0.0.10"
      |    port: 8001
      |    # inject a sed command to remove the www-address config used by Leo
      |    command:  ["/bin/sh", "-c"]
      |    args: ["sed -i 's/^www-address.*$//' $RSTUDIO_HOME/rserver.conf && /init"]
      |    pdMountPath: "/data"
      |    pdAccessMode: "ReadWriteOnce"
      |    environment:
      |      WORKSPACE_NAME: "my-ws"
      |      WORKSPACE_NAMESPACE: "my-proj"
      |      # needed to disable auth
      |      USER: "rstudio"
      |""".stripMargin

  "HttpAppDescriptorDAO" should "properly parse an app.yaml with a stubbed client response" in {

    withStubbedAppDescriptorDAO(
      rstudioRespYaml,
      { dao =>
        val descriptor = dao.getDescriptor(dummyRequestURI).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
        descriptor.author shouldBe "workbench-interactive-analysis@broadinstitute.org"
        descriptor.description should include("RStudio")
        descriptor.name shouldBe "rstudio"
        descriptor.version shouldBe "0.0.10"
        descriptor.services.size shouldBe 1
        descriptor.services.get("rstudio") shouldBe Some(
          CustomAppService(
            ContainerImage.fromImageUrl("us.gcr.io/anvil-gcr-public/anvil-rstudio-bioconductor:0.0.10").get,
            8001,
            "/",
            List("/bin/sh", "-c"),
            List("sed -i 's/^www-address.*$//' $RSTUDIO_HOME/rserver.conf && /init"),
            "/data",
            "ReadWriteOnce",
            Map(
              "WORKSPACE_NAME" -> "my-ws",
              "WORKSPACE_NAMESPACE" -> "my-proj",
              "USER" -> "rstudio"
            )
          )
        )
      }
    )
  }

  // allows you to specify the response of requests
  def withStubbedAppDescriptorDAO(response: String, testCode: HttpAppDescriptorDAO[IO] => Any): Unit = {
    val fixedRespClient = Client.fromHttpApp[IO](
      HttpApp(_ => IO(Response(status = Status.Ok).withEntity(response)))
    )
    val clientWithLogging = Logger[IO](logHeaders = true, logBody = false)(fixedRespClient)

    val appDAOResource = new HttpAppDescriptorDAO[IO](clientWithLogging)

    IO(testCode(appDAOResource)).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  // allows http retrieval
  def withAppDescriptorDAO(testCode: HttpAppDescriptorDAO[IO] => Any): Unit = {
    val daoResource = for {
      client <- BlazeClientBuilder[IO](global).resource
      clientWithLogging = Logger[IO](logHeaders = true, logBody = false)(client)
    } yield new HttpAppDescriptorDAO[IO](clientWithLogging)

    daoResource.use(dao => IO(testCode(dao))).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }
}
