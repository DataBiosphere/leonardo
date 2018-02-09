package org.broadinstitute.dsde.workbench.leonardo.auth

import java.io.File
import java.time.Duration

import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.util.toScalaDuration

/**
  * Common trait for Leo providers which need a SwaggerSamClient.
  */
trait SamProvider {
  val config: Config

  protected def getLeoServiceAccountAndKey: (WorkbenchEmail, File)

  protected lazy val samServer = config.as[String]("samServer")
  protected lazy val cacheExpiryTime = toScalaDuration(Option(config.getDuration("samCacheExpiryTime")).getOrElse(Duration.ofMinutes(60)))
  protected lazy val cacheMaxSize = Option(config.as[Int]("samCacheMaxSize")).getOrElse(1000)
  protected lazy val (leoEmail, leoPemFile) = getLeoServiceAccountAndKey
  protected lazy val samClient = new SwaggerSamClient(samServer, cacheExpiryTime, cacheMaxSize, leoEmail, leoPemFile)
}
