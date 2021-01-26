package org.broadinstitute.dsde.workbench.leonardo
package http

import cats.syntax.all._
import org.broadinstitute.dsde.workbench.google2.{PublisherConfig, SubscriberConfig}
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import pureconfig.ConfigSource
import _root_.pureconfig.generic.auto._
import org.broadinstitute.dsde.workbench.leonardo.ConfigImplicits._

object NewConfig {
  val appConfig = ConfigSource
    .fromConfig(Config.config)
    .load[HttpAppConfig]
    .leftMap(failures => new RuntimeException(failures.toList.map(_.description).mkString("\n")))
}

final case class HttpAppConfig(cryptominingPublisherConfig: PublisherConfig,
                               nonLeonardoMessageSubscriber: SubscriberConfig)
