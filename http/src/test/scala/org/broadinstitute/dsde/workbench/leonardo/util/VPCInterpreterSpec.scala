package org.broadinstitute.dsde.workbench.leonardo
package util

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.mtl.Ask
import com.google.cloud.compute.v1.{Firewall, Network, Operation}
import org.broadinstitute.dsde.workbench.google2.mock.{
  FakeGoogleComputeService,
  FakeGoogleResourceService,
  MockComputePollOperation
}
import org.broadinstitute.dsde.workbench.google2.{FirewallRuleName, NetworkName, RegionName, SubnetworkName}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.jdk.CollectionConverters._

class VPCInterpreterSpec extends AnyFlatSpecLike with LeonardoTestSuite {

  "VPCInterpreter" should "get a subnet from a project label" in {
    val test = new VPCInterpreter(Config.vpcInterpreterConfig,
                                  stubResourceService(
                                    Map(vpcConfig.highSecurityProjectNetworkLabel.value -> "my_network",
                                        vpcConfig.highSecurityProjectSubnetworkLabel.value -> "my_subnet")
                                  ),
                                  FakeGoogleComputeService,
                                  new MockComputePollOperation)

    test
      .setUpProjectNetwork(SetUpProjectNetworkParams(project, RegionName("us-central1")))
      .unsafeRunSync() shouldBe (NetworkName("my_network"), SubnetworkName("my_subnet"))
  }

  it should "fail if both labels are not present" in {
    val test = new VPCInterpreter(Config.vpcInterpreterConfig,
                                  stubResourceService(
                                    Map(vpcConfig.highSecurityProjectSubnetworkLabel.value -> "my_network")
                                  ),
                                  FakeGoogleComputeService,
                                  new MockComputePollOperation)

    test
      .setUpProjectNetwork(SetUpProjectNetworkParams(project, RegionName("us-central1")))
      .attempt
      .unsafeRunSync() shouldBe Left(
      InvalidVPCSetupException(project)
    )

    val test2 = new VPCInterpreter(Config.vpcInterpreterConfig,
                                   stubResourceService(
                                     Map(vpcConfig.highSecurityProjectSubnetworkLabel.value -> "my_subnet")
                                   ),
                                   FakeGoogleComputeService,
                                   new MockComputePollOperation)

    test2
      .setUpProjectNetwork(SetUpProjectNetworkParams(project, RegionName("us-central1")))
      .attempt
      .unsafeRunSync() shouldBe Left(
      InvalidVPCSetupException(project)
    )
  }

  it should "create a new subnet if there are no project labels" in {
    val test = new VPCInterpreter(Config.vpcInterpreterConfig,
                                  stubResourceService(Map.empty),
                                  FakeGoogleComputeService,
                                  new MockComputePollOperation)

    test
      .setUpProjectNetwork(SetUpProjectNetworkParams(project, RegionName("us-central1")))
      .unsafeRunSync() shouldBe (vpcConfig.networkName, vpcConfig.subnetworkName)
  }

  it should "create firewall rules in the project network" in {
    val computeService = new MockGoogleComputeServiceWithFirewalls()
    val test = new VPCInterpreter(Config.vpcInterpreterConfig,
                                  stubResourceService(Map.empty),
                                  computeService,
                                  new MockComputePollOperation)

    test
      .setUpProjectFirewalls(SetUpProjectFirewallsParams(project, vpcConfig.networkName, RegionName("us-central1")))
      .unsafeRunSync()
    computeService.firewallMap.size shouldBe 3
    vpcConfig.firewallsToAdd.foreach { fwConfig =>
      val fw = computeService.firewallMap.get(FirewallRuleName(s"${fwConfig.namePrefix}-us-central1"))
      fw shouldBe defined
      fw.get.getNetwork shouldBe s"projects/${project.value}/global/networks/${vpcConfig.networkName.value}"
      fw.get.getTargetTagsList.asScala shouldBe List(vpcConfig.networkTag.value)
      fw.get.getAllowedList.asScala.map(_.getIPProtocol).toSet shouldBe fwConfig.allowed.map(_.protocol).toSet
      fw.get.getAllowedList.asScala.flatMap(_.getPortsList.asScala).toSet shouldBe fwConfig.allowed
        .flatMap(_.port)
        .toSet
    }
  }

  it should "remove firewall rules in the default network" in {
    val computeService = new MockGoogleComputeServiceWithFirewalls()
    vpcConfig.firewallsToRemove.foreach { fw =>
      computeService.firewallMap.putIfAbsent(fw, Firewall.newBuilder().setName(fw.value).build)
    }
    val test = new VPCInterpreter(Config.vpcInterpreterConfig,
                                  stubResourceService(Map.empty),
                                  computeService,
                                  new MockComputePollOperation)
    test
      .setUpProjectFirewalls(SetUpProjectFirewallsParams(project, vpcConfig.networkName, RegionName("us-central1")))
      .unsafeRunSync()
    vpcConfig.firewallsToRemove.foreach(fw => computeService.firewallMap should not contain key(fw))
  }

  private def stubResourceService(labels: Map[String, String]): FakeGoogleResourceService =
    new FakeGoogleResourceService {
      override def getLabels(project: GoogleProject)(
        implicit ev: Ask[IO, TraceId]
      ): IO[Option[Map[String, String]]] = IO(Some(labels))
    }

  class MockGoogleComputeServiceWithFirewalls extends FakeGoogleComputeService {
    val firewallMap = scala.collection.concurrent.TrieMap.empty[FirewallRuleName, Firewall]

    override def addFirewallRule(project: GoogleProject, firewall: Firewall)(
      implicit ev: Ask[IO, TraceId]
    ): IO[Operation] =
      IO(firewallMap.putIfAbsent(FirewallRuleName(firewall.getName), firewall)) >> super
        .addFirewallRule(project, firewall)(ev)

    override def deleteFirewallRule(project: GoogleProject, firewallRuleName: FirewallRuleName)(
      implicit ev: Ask[IO, TraceId]
    ): IO[Unit] = IO(firewallMap.remove(firewallRuleName)).void

    override def getNetwork(project: GoogleProject, networkName: NetworkName)(
      implicit ev: Ask[IO, TraceId]
    ): IO[Option[Network]] =
      if (networkName.value == "default")
        IO.pure(Some(Network.newBuilder().setName("default").build))
      else IO.pure(None)
  }

}
