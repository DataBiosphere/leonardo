package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.{AutoscalingConfig, NumNodes}

case class GalaxyNodepoolConfig(machineType: MachineTypeName,
                                numNodes: NumNodes,
                                autoscalingEnabled: Boolean,
                                autoscalingConfig: AutoscalingConfig)
