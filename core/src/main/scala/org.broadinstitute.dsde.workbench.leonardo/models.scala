package org.broadinstitute.dsde.workbench.leonardo

import java.time.Instant

import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

final case class MachineConfig(numberOfWorkers: Option[Int] = None,
                               masterMachineType: Option[String] = None,
                               masterDiskSize: Option[Int] = None, //min 10
                               workerMachineType: Option[String] = None,
                               workerDiskSize: Option[Int] = None, //min 10
                               numberOfWorkerLocalSSDs: Option[Int] = None, //min 0 max 8
                               numberOfPreemptibleWorkers: Option[Int] = None)

// Information about service accounts used by the cluster
final case class ServiceAccountInfo(clusterServiceAccount: Option[WorkbenchEmail],
                                    notebookServiceAccount: Option[WorkbenchEmail])

final case class ClusterError(errorMessage: String, errorCode: Int, timestamp: Instant)
