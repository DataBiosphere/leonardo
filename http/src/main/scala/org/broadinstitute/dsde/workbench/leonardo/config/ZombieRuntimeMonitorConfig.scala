package org.broadinstitute.dsde.workbench.leonardo
package config

import scala.concurrent.duration.FiniteDuration

case class ZombieRuntimeMonitorConfig(enableZombieRuntimeDetection: Boolean,
                                      zombieCheckPeriod: FiniteDuration,
                                      deletionConfirmationLabelKey: String,
                                      creationHangTolerance: FiniteDuration,
                                      concurrency: Int
)
