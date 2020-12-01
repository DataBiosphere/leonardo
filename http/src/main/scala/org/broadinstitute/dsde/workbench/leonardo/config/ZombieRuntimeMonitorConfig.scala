package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.google2.{RegionName, ZoneName}

import scala.concurrent.duration.FiniteDuration

case class ZombieRuntimeMonitorConfig(enableZombieRuntimeDetection: Boolean,
                                      zombieCheckPeriod: FiniteDuration,
                                      deletionConfirmationLabelKey: String,
                                      creationHangTolerance: FiniteDuration,
                                      concurrency: Int,
                                      gceZoneName: ZoneName,
                                      dataprocRegion: RegionName
)
