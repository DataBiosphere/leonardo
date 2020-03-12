package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.google2.ZoneName

import scala.concurrent.duration.FiniteDuration

case class ZombieClusterConfig(enableZombieClusterDetection: Boolean,
                               zombieCheckPeriod: FiniteDuration,
                               creationHangTolerance: FiniteDuration,
                               concurrency: Int,
                               gceZoneName: ZoneName)
