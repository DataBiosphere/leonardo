package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.NumNodes

case class DefaultNodepoolConfig(machineType: MachineTypeName, numNodes: NumNodes, autoscalingEnabled: Boolean)
