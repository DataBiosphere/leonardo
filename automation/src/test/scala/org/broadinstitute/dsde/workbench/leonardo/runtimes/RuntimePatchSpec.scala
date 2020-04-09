package org.broadinstitute.dsde.workbench.leonardo
package runtimes

//import org.broadinstitute.dsde.workbench.google2.MachineTypeName
//import org.scalatest.time.{Minutes, Seconds, Span}
import org.scalatest.DoNotDiscover
//import org.broadinstitute.dsde.workbench.service.util.Tags

@DoNotDiscover
class RuntimePatchSpec extends RuntimeFixtureSpec with LeonardoTestUtils {

  //TODO Reenable this once the bug with Patch is fixed
  //this is an end to end test of the pub/sub infrastructure
//  "Patch endpoint should perform a stop/start tranition" taggedAs Tags.SmokeTest in { runtimeFixture =>
//    val newMasterMachineType = "n1-standard-2"
//    val runtimeConfig = UpdateRuntimeConfigRequestCopy.GceConfig(
//      Some(newMasterMachineType),
//      None
//    )
//
//    val originalCluster =
//      Leonardo.cluster.getRuntime(runtimeFixture.runtime.googleProject, runtimeFixture.runtime.clusterName)
//    originalCluster.status shouldBe ClusterStatus.Running
//
//    val originalMachineConfig = originalCluster.runtimeConfig
//
//    Leonardo.cluster.updateRuntime(
//      runtimeFixture.runtime.googleProject,
//      runtimeFixture.runtime.clusterName,
//      request = UpdateRuntimeRequestCopy(Some(runtimeConfig), true, None, None)
//    )
//
//    eventually(timeout(Span(10, Minutes)), interval(Span(10, Seconds))) {
//      val getRuntime =
//        Leonardo.cluster.getRuntime(runtimeFixture.runtime.googleProject, runtimeFixture.runtime.clusterName)
//      getRuntime.status shouldBe ClusterStatus.Stopping
//    }
//
//    eventually(timeout(Span(10, Minutes)), interval(Span(30, Seconds))) {
//      val getRuntime =
//        Leonardo.cluster.getRuntime(runtimeFixture.runtime.googleProject, runtimeFixture.runtime.clusterName)
//      getRuntime.status shouldBe ClusterStatus.Running
//      getRuntime.runtimeConfig shouldBe originalMachineConfig
//        .asInstanceOf[RuntimeConfig.GceConfig]
//        .copy(machineType = MachineTypeName(newMasterMachineType))
//    }
//  }

}
