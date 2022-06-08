package org.broadinstitute.dsde.workbench.leonardo
import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.NumNodes

//this is the maximum number of nodepools that can run on a cluster the given default nodepool machine type, used to determine how many nodes the default nodepool needs
final case class MaxNodepoolsPerDefaultNode(value: Int) extends AnyVal
case class DefaultNodepoolConfig(machineType: MachineTypeName,
                                 numNodes: NumNodes,
                                 autoscalingEnabled: Boolean,
                                 maxNodepoolsPerDefaultNode: MaxNodepoolsPerDefaultNode
)
