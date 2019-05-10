package org.broadinstitute.dsde.workbench.leonardo.config

case class ClusterDefaultsConfig(
                                   numberOfWorkers: Int,
                                   masterMachineType: String,
                                   masterDiskSize: Int,
                                   workerMachineType: String,
                                   workerDiskSize: Int,
                                   numberOfWorkerLocalSSDs: Int,
                                   numberOfPreemptibleWorkers: Int
                                 )