package org.broadinstitute.dsde.workbench.leonardo

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.api.LeoRoutes
import org.broadinstitute.dsde.workbench.leonardo.config.{DataprocConfig, SwaggerConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.GoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.service.LeonardoService

import scala.concurrent.{ExecutionContext, Future}

object Boot extends App with LazyLogging {

  private def startup(): Unit = {

    val config = ConfigFactory.parseResources("leonardo.conf").withFallback(ConfigFactory.load())
    val dataprocConfig = config.as[DataprocConfig]("dataproc")

    // we need an ActorSystem to host our application in
    implicit val system = ActorSystem("leonardo")
    implicit val materializer = ActorMaterializer()
    import scala.concurrent.ExecutionContext.Implicits.global

    val ggDAO = new GoogleDataprocDAO(dataprocConfig)
    val leonardoService = new LeonardoService(ggDAO)

    val leoRoutes = new LeoRoutes(leonardoService, config.as[SwaggerConfig]("swagger"))

      Http().bindAndHandle(leoRoutes.route, "0.0.0.0", 8080)
        .recover {
          case t: Throwable =>
            logger.error("FATAL - failure starting http server", t)
            throw t
        }
  }

  startup()
}
