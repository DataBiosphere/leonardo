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
                            scaleNodepool: PollMonitorConfig,
                            setNodepoolAutoscaling: PollMonitorConfig,
                            startApp: InterruptablePollMonitorConfig
)
