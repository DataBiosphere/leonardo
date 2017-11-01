package org.broadinstitute.dsde.workbench.leonardo.config

case class ClusterDefaultsConfig(
                                   numberOfWorkers: Int,
                                   masterMachineType: String,
                                   masterDiskSize: Int,
                                   masterDiskSizeMinimum: Int,
                                   workerMachineType: String,
                                   workerDiskSize: Int,
                                   numberOfWorkerLocalSSDs: Int,
                                   numberOfPreemptibleWorkers: Int
                                 )