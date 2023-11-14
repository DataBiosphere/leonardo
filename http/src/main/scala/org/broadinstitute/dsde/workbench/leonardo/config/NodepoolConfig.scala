package org.broadinstitute.dsde.workbench.leonardo
package config
case class NodepoolConfig(defaultNodepoolConfig: DefaultNodepoolConfig,
                          defaultAutoScalingNodepoolConfig: DefaultNodepoolConfig,
                          galaxyNodepoolConfig: GalaxyNodepoolConfig
)
