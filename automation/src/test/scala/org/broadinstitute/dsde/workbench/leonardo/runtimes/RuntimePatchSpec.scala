package org.broadinstitute.dsde.workbench.leonardo
package runtimes

import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.scalatest.time.{Minutes, Seconds, Span}
import org.scalatest.DoNotDiscover
import org.broadinstitute.dsde.workbench.service.util.Tags

//@DoNotDiscover
class RuntimePatchSpec extends RuntimeFixtureSpec with LeonardoTestUtils {

  //this is an end to end test of the pub/sub infrastructure
  "Patch endpoint should perform a stop/start tranition" taggedAs Tags.SmokeTest in { clusterFixture =>
    val newMasterMachineType = "n1-standard-2"
    val runtimeConfig = UpdateRuntimeConfigRequestCopy.GceConfig(
      //TODO See if this fails to go lower
      Some(newMasterMachineType),
      None
    )


    val originalCluster = Leonardo.cluster.getRuntime(clusterFixture.cluster.googleProject, clusterFixture.cluster.clusterName)
    originalCluster.status shouldBe ClusterStatus.Running

    val originalMachineConfig = originalCluster.runtimeConfig

    Leonardo.cluster.updateRuntime(
      clusterFixture.cluster.googleProject,
      clusterFixture.cluster.clusterName,
      request = UpdateRuntimeRequestCopy(Some(runtimeConfig), true, None, None )
    )

    eventually(timeout(Span(10, Minutes)), interval(Span(10, Seconds))) {
      val getRuntime =
        Leonardo.cluster.getRuntime(clusterFixture.cluster.googleProject, clusterFixture.cluster.clusterName)
      logger.info(s"GET RUNTIME STATUS ${getRuntime.status}")
      getRuntime.status shouldBe ClusterStatus.Stopping
    }

    eventually(timeout(Span(10, Minutes)), interval(Span(30, Seconds))) {
      val getRuntime =
        Leonardo.cluster.getRuntime(clusterFixture.cluster.googleProject, clusterFixture.cluster.clusterName)
      getRuntime.status shouldBe ClusterStatus.Running
      getRuntime.runtimeConfig shouldBe originalMachineConfig.asInstanceOf[RuntimeConfig.GceConfig].copy(machineType = MachineTypeName(newMasterMachineType))
    }
  }

}
