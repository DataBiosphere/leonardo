package org.broadinstitute.dsde.workbench.leonardo
package cluster

import org.scalatest.time.{Minutes, Seconds, Span}
import org.scalatest.DoNotDiscover
import org.broadinstitute.dsde.workbench.service.util.Tags

@DoNotDiscover
class ClusterPatchSpec extends ClusterFixtureSpec with LeonardoTestUtils {

  //this is an end to end test of the pub/sub infrastructure
  "Patch endpoint should perform a stop/start tranition" taggedAs Tags.SmokeTest  in { clusterFixture =>
    val newMasterMachineType = "n1-standard-2"
    val machineConfig = Some(
      RuntimeConfig.DataprocConfig(
        0,
        masterMachineType = newMasterMachineType,
        500
      )
    )

    val originalCluster = Leonardo.cluster.get(clusterFixture.cluster.googleProject, clusterFixture.cluster.clusterName)
    originalCluster.status shouldBe ClusterStatus.Running

    val originalMachineConfig = originalCluster.machineConfig

    Leonardo.cluster.update(
      clusterFixture.cluster.googleProject,
      clusterFixture.cluster.clusterName,
      clusterRequest = defaultClusterRequest.copy(allowStop = true, machineConfig = machineConfig)
    )

    eventually(timeout(Span(1, Minutes)), interval(Span(10, Seconds))) {
      val getCluster: Cluster =
        Leonardo.cluster.get(clusterFixture.cluster.googleProject, clusterFixture.cluster.clusterName)
      getCluster.status shouldBe ClusterStatus.Stopping
    }

    eventually(timeout(Span(10, Minutes)), interval(Span(30, Seconds))) {
      val getCluster: Cluster =
        Leonardo.cluster.get(clusterFixture.cluster.googleProject, clusterFixture.cluster.clusterName)
      getCluster.status shouldBe ClusterStatus.Running
      getCluster.machineConfig shouldBe originalMachineConfig.copy(masterMachineType = newMasterMachineType)
    }
  }

}
