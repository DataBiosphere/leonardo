package org.broadinstitute.dsde.workbench.leonardo
package runtimes

import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.GPAllocFixtureSpec.gpallocProjectKey
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.service.util.Tags
import org.scalatest.{fixture, DoNotDiscover, Outcome}
import org.scalatest.time.{Minutes, Seconds, Span}
import RuntimeFixtureSpec._
import org.broadinstitute.dsde.workbench.auth.AuthToken

// extending fixture.Freespec just so we can use `taggedAs`. Not really need fixture for anything
@DoNotDiscover
class RuntimePatchSpec extends fixture.FreeSpec with LeonardoTestUtils {
  implicit val ronToken: AuthToken = ronAuthToken

  //this is an end to end test of the pub/sub infrastructure
  "Patch endpoint should perform a stop/start tranition for GCE VM" taggedAs Tags.SmokeTest in { _ =>
    val billingProjectString =
      sys.props.get(gpallocProjectKey).getOrElse(throw new Exception("Billing project is not set"))
    val googleProject = GoogleProject(billingProjectString)
    // create a new GCE runtime
    val runtime = createNewRuntime(googleProject, request = getRuntimeRequest(CloudService.GCE, None))(ronAuthToken)

    val newMasterMachineType = "n1-standard-2"
    val runtimeConfig = UpdateRuntimeConfigRequestCopy.GceConfig(
      Some(newMasterMachineType),
      None
    )

    val originalCluster =
      Leonardo.cluster.getRuntime(googleProject, runtime.clusterName)
    originalCluster.status shouldBe ClusterStatus.Running

    val originalMachineConfig = originalCluster.runtimeConfig

    Leonardo.cluster.updateRuntime(
      runtime.googleProject,
      runtime.clusterName,
      request = UpdateRuntimeRequestCopy(Some(runtimeConfig), true, None, None)
    )

    eventually(timeout(Span(10, Minutes)), interval(Span(10, Seconds))) {
      val getRuntime =
        Leonardo.cluster.getRuntime(runtime.googleProject, runtime.clusterName)
      getRuntime.status shouldBe ClusterStatus.Stopping
    }

    eventually(timeout(Span(10, Minutes)), interval(Span(30, Seconds))) {
      val getRuntime =
        Leonardo.cluster.getRuntime(runtime.googleProject, runtime.clusterName)
      getRuntime.status shouldBe ClusterStatus.Running
      getRuntime.runtimeConfig shouldBe originalMachineConfig
        .asInstanceOf[RuntimeConfig.GceConfig]
        .copy(machineType = MachineTypeName(newMasterMachineType))
    }
  }

  "Patch endpoint should perform a stop/start tranition for Dataproc cluster" taggedAs Tags.SmokeTest in { _ =>
    val billingProjectString =
      sys.props.get(gpallocProjectKey).getOrElse(throw new Exception("Billing project is not set"))
    val googleProject = GoogleProject(billingProjectString)
    // create a new GCE runtime
    val runtime =
      createNewRuntime(googleProject, request = getRuntimeRequest(CloudService.Dataproc, None))(ronAuthToken)

    val newMasterMachineType = "n1-standard-2"
    val runtimeConfig = UpdateRuntimeConfigRequestCopy.GceConfig(
      Some(newMasterMachineType),
      None
    )

    val originalCluster =
      Leonardo.cluster.getRuntime(googleProject, runtime.clusterName)
    originalCluster.status shouldBe ClusterStatus.Running

    val originalMachineConfig = originalCluster.runtimeConfig

    Leonardo.cluster.updateRuntime(
      runtime.googleProject,
      runtime.clusterName,
      request = UpdateRuntimeRequestCopy(Some(runtimeConfig), true, None, None)
    )

    eventually(timeout(Span(10, Minutes)), interval(Span(10, Seconds))) {
      val getRuntime =
        Leonardo.cluster.getRuntime(runtime.googleProject, runtime.clusterName)
      getRuntime.status shouldBe ClusterStatus.Stopping
    }

    eventually(timeout(Span(10, Minutes)), interval(Span(30, Seconds))) {
      val getRuntime =
        Leonardo.cluster.getRuntime(runtime.googleProject, runtime.clusterName)
      getRuntime.status shouldBe ClusterStatus.Running
      getRuntime.runtimeConfig shouldBe originalMachineConfig
        .asInstanceOf[RuntimeConfig.DataprocConfig]
        .copy(masterMachineType = MachineTypeName(newMasterMachineType))
    }
  }

  override protected def withFixture(test: OneArgTest): Outcome = super.withFixture(test.toNoArgTest(()))

  override type FixtureParam = Unit
}
