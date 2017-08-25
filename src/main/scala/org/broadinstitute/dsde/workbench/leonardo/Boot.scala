package org.broadinstitute.dsde.workbench.leonardo

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.api.LeoRoutes
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.config.{DataprocConfig, ProxyConfig, SwaggerConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.GoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.service.{LeonardoService, ProxyService}

object Boot extends App with LazyLogging {
  private def startup(): Unit = {

    System.setProperty("sun.net.spi.nameservice.provider.1", "dns,Jupyter")
    System.setProperty("sun.net.spi.nameservice.provider.2", "dns,sun")

    val config = ConfigFactory.parseResources("leonardo.conf").withFallback(ConfigFactory.load())
    val dataprocConfig = config.as[DataprocConfig]("dataproc")
    val proxyConfig = config.as[ProxyConfig]("proxy")

    // we need an ActorSystem to host our application in
    implicit val system = ActorSystem("leonardo")
    implicit val materializer = ActorMaterializer()
    import system.dispatcher

    val dbRef = DbReference.init(config)
    system.registerOnTermination {
      dbRef.database.close()
    }

    val gdDAO = new GoogleDataprocDAO(dataprocConfig)
    val leonardoService = new LeonardoService(gdDAO, dbRef)
    val proxyService = new ProxyService(proxyConfig, dbRef)

    val leoRoutes = new LeoRoutes(leonardoService, proxyService, config.as[SwaggerConfig]("swagger"))

    Http().bindAndHandle(leoRoutes.route, "0.0.0.0", 8080)
      .recover {
        case t: Throwable =>
          logger.error("FATAL - failure starting http server", t)
          throw t
      }
  }

  startup()
}
