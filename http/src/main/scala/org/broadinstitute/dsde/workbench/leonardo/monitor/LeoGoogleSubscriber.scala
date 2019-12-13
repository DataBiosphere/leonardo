package org.broadinstitute.dsde.workbench.leonardo.monitor

import cats.effect.Concurrent
import io.chrisdavenport.log4cats.Logger//TODO: verify?
import org.broadinstitute.dsde.workbench.leonardo.config.PubsubConfig

class LeoGoogleSubscriber[F[_]: Logger: Concurrent](pubsubConfig: PubsubConfig) {

}
