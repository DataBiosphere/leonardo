package org.broadinstitute.dsde.workbench.leonardo
package http

import akka.actor.ActorSystem
import cats.effect.Sync
import com.typesafe.sslconfig.akka.util.AkkaLoggerFactory
import com.typesafe.sslconfig.ssl.{
  ConfigSSLContextBuilder,
  DefaultKeyManagerFactoryWrapper,
  DefaultTrustManagerFactoryWrapper,
  SSLConfigFactory
}
import org.slf4j.LoggerFactory

import javax.net.ssl.SSLContext

object SslContextReader {
  private val logger = LoggerFactory.getLogger(getClass)

  def getSSLContext[F[_]: Sync]()(implicit as: ActorSystem): F[SSLContext] = Sync[F].delay {
    val akkaOverrides = as.settings.config.getConfig("akka.ssl-config")

    val defaults = as.settings.config.getConfig("ssl-config")
    val sslConfigSettings = SSLConfigFactory.parse(akkaOverrides.withFallback(defaults))

    // Log certificate information
    logCertificateInfo(sslConfigSettings)

    val keyManagerAlgorithm = new DefaultKeyManagerFactoryWrapper(sslConfigSettings.keyManagerConfig.algorithm)
    val trustManagerAlgorithm = new DefaultTrustManagerFactoryWrapper(sslConfigSettings.trustManagerConfig.algorithm)

    new ConfigSSLContextBuilder(new AkkaLoggerFactory(as),
                                sslConfigSettings,
                                keyManagerAlgorithm,
                                trustManagerAlgorithm
    ).build()
  }

  private def logCertificateInfo(sslConfigSettings: com.typesafe.sslconfig.ssl.SSLConfigSettings): Unit = {
    logger.info("=== SSL Certificate Configuration ===")

    // Log certificate secret metadata if available
    sys.env.get("LEO_CERT_SECRET_NAME").foreach { secretName =>
      logger.info(s"Certificate Secret Name: $secretName")
    }

    sys.env.get("LEO_CERT_SECRET_PROJECT").foreach { secretProject =>
      logger.info(s"Certificate Secret Project: $secretProject")
    }

    sys.env.get("LEO_CERT_GENERATION_DATE").foreach { genDate =>
      logger.info(s"Certificate Generation Date: $genDate")
    }

    // Log environment information
    val googleProject = sys.env.get("GOOGLE_PROJECT").orElse(sys.env.get("GCP_PROJECT"))
    googleProject.foreach(project => logger.info(s"Google Project: $project"))
  }
}
