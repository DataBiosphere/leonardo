package org.broadinstitute.dsde.workbench.leonardo
package cluster

import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.scalatest.time.{Minutes, Seconds, Span}
import org.scalatest.DoNotDiscover
import org.broadinstitute.dsde.workbench.service.util.Tags

@DoNotDiscover
class ClusterPatchSpec extends ClusterFixtureSpec with LeonardoTestUtils {

  //this is an end to end test of the pub/sub infrastructure
  "Patch endpoint should perform a stop/start tranition" taggedAs Tags.SmokeTest in { clusterFixture =>
    val newMasterMachineType = MachineTypeName("n1-standard-2")
    val machineConfig =
      RuntimeConfig.DataprocConfig(
        0,
        masterMachineType = newMasterMachineType,
        DiskSize(500),
        workerMachineType = None,
        workerDiskSize = None,
        numberOfWorkerLocalSSDs = None,
        numberOfPreemptibleWorkers = None,
        properties = Map.empty
      )

    val originalCluster = Leonardo.cluster.get(clusterFixture.cluster.googleProject, clusterFixture.cluster.clusterName)
    originalCluster.status shouldBe ClusterStatus.Running

    val originalMachineConfig = originalCluster.machineConfig

    Leonardo.cluster.update(
      clusterFixture.cluster.googleProject,
      clusterFixture.cluster.clusterName,
      clusterRequest = defaultClusterRequest.copy(allowStop = true,
                                                  machineConfig =
                                                    Some(DataprocConfigCopy.fromDataprocConfig(machineConfig)))
    )

    eventually(timeout(Span(1, Minutes)), interval(Span(10, Seconds))) {
      val getCluster: ClusterCopy =
        Leonardo.cluster.get(clusterFixture.cluster.googleProject, clusterFixture.cluster.clusterName)
      getCluster.status shouldBe ClusterStatus.Stopping
    }

    eventually(timeout(Span(10, Minutes)), interval(Span(30, Seconds))) {
      val getCluster: ClusterCopy =
        Leonardo.cluster.get(clusterFixture.cluster.googleProject, clusterFixture.cluster.clusterName)
      getCluster.status shouldBe ClusterStatus.Running
      getCluster.machineConfig shouldBe originalMachineConfig.copy(masterMachineType = newMasterMachineType)
    }
  }

}
