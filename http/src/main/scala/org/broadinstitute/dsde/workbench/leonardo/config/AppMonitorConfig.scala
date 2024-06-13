package org.broadinstitute.dsde.workbench.leonardo
package config

import org.broadinstitute.dsde.workbench.leonardo.monitor.{InterruptablePollMonitorConfig, PollMonitorConfig}

case class AppMonitorConfig(nodepoolCreate: PollMonitorConfig,
                            clusterCreate: PollMonitorConfig,
                            nodepoolDelete: PollMonitorConfig,
                            clusterDelete: PollMonitorConfig,
                            createIngress: PollMonitorConfig,
                            createApp: InterruptablePollMonitorConfig,
                            deleteApp: PollMonitorConfig,
                            scalingUpNodepool: PollMonitorConfig,
                            scalingDownNodepool: PollMonitorConfig,
                            startApp: InterruptablePollMonitorConfig,
                            updateApp: InterruptablePollMonitorConfig,
                            appLiveness: PollMonitorConfig
)
