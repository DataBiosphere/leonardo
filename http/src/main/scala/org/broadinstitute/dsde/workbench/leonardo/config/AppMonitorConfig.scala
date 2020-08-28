package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.leonardo.monitor.PollMonitorConfig

case class AppMonitorConfig(nodepoolCreate: PollMonitorConfig,
                            clusterCreate: PollMonitorConfig,
                            nodepoolDelete: PollMonitorConfig,
                            clusterDelete: PollMonitorConfig,
                            createIngress: PollMonitorConfig,
                            createApp: PollMonitorConfig)
