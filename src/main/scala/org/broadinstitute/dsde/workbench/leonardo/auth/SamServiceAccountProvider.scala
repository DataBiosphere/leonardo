package org.broadinstitute.dsde.workbench.leonardo.auth

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.config.SamConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.HttpSamDAO
import org.broadinstitute.dsde.workbench.leonardo.model.ServiceAccountProvider
import scala.concurrent.duration.FiniteDuration

/**
  * Created by rtitle on 12/5/17.
  */
abstract class SamServiceAccountProvider(config: Config) extends ServiceAccountProvider(config) {
  // Need to specify a new ActorSystem for Sam
  implicit val system = ActorSystem("SamServiceAccountProvider")
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher


  protected lazy val samConfig = config.as[SamConfig]("sam")
  protected lazy val cacheExpiryTime = config.getConfig("serviceAccounts.config").getConfig("sam").as[FiniteDuration]("cacheExpiryTime")
  protected lazy val cacheMaxSize = config.getConfig("serviceAccounts.config").getConfig("sam").as[Int]("cacheMaxSize")
  protected lazy val (leoEmail, leoPemFile) = getLeoServiceAccountAndKey
  protected lazy val samDAO = new HttpSamDAO(samConfig.server)
  protected lazy val samClient = new SwaggerSamClient(samConfig.server, cacheExpiryTime, cacheMaxSize, leoEmail, leoPemFile)

}
