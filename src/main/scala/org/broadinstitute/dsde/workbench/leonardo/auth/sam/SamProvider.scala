package org.broadinstitute.dsde.workbench.leonardo.auth.sam

import java.io.File

import akka.actor.ActorSystem
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.util.Retry

import scala.concurrent.duration._

/**
  * Common trait for Leo providers which need a SwaggerSamClient.
  */
trait SamProvider extends Retry with LazyLogging {
  val config: Config

  protected def getLeoServiceAccountAndKey: (WorkbenchEmail, File)

  protected lazy val samServer = config.as[String]("samServer")
  protected lazy val cacheEnabled = config.getBoolean("petTokenCacheEnabled")
  protected lazy val cacheExpiryTime = config.getAs[FiniteDuration]("petTokenCacheExpiryTime").getOrElse(60 minutes)
  protected lazy val cacheMaxSize = config.getAs[Int]("petTokenCacheMaxSize").getOrElse(1000)
  protected lazy val (leoEmail, leoPemFile) = getLeoServiceAccountAndKey
  protected lazy val samClient = new SwaggerSamClient(samServer, cacheEnabled, cacheExpiryTime, cacheMaxSize, leoEmail, leoPemFile)
  override val system = ActorSystem("leonardo-sam-provider")
}
