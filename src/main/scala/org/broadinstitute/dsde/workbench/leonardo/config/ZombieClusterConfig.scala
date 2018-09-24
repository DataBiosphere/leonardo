package org.broadinstitute.dsde.workbench.leonardo.config

import scala.concurrent.duration.FiniteDuration

case class ZombieClusterConfig(enableZombieClusterDetection: Boolean,
                               zombieCheckPeriod: FiniteDuration)
