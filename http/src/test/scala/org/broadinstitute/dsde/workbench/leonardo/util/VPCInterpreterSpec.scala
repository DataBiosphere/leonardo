package org.broadinstitute.dsde.workbench.leonardo
package util

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.mtl.Ask
import com.google.api.gax.longrunning.OperationFuture
import com.google.cloud.compute.v1.{Firewall, Network, Operation}
import org.broadinstitute.dsde.workbench.google2.mock.{
  FakeComputeOperationFuture,
  FakeGoogleComputeService,
  FakeGoogleResourceService
}
import org.broadinstitute.dsde.workbench.google2.{FirewallRuleName, NetworkName, RegionName, SubnetworkName}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.config.{Allowed, Config, FirewallRuleConfig}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.scalatest.flatspec.AnyFlatSpecLike
import org.broadinstitute.dsde.workbench.leonardo.http.ctxConversion
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext

import scala.jdk.CollectionConverters._

class VPCInterpreterSpec extends AnyFlatSpecLike with LeonardoTestSuite {

  "VPCInterpreter" should "get a subnet from a project label" in {
    val test = new VPCInterpreter(
      Config.vpcInterpreterConfig,
      stubResourceService(
        Map(vpcConfig.highSecurityProjectNetworkLabel.value -> "my_network",
            vpcConfig.highSecurityProjectSubnetworkLabel.value -> "my_subnet"
        )
      ),
      FakeGoogleComputeService
    )

    test
      .setUpProjectNetworkAndFirewalls(SetUpProjectNetworkParams(project, RegionName("us-central1")))
      .unsafeRunSync() shouldBe (NetworkName("my_network"), SubnetworkName("my_subnet"))
  }

  it should "fail if both labels are not present" in {
    val test = new VPCInterpreter(Config.vpcInterpreterConfig,
                                  stubResourceService(
                                    Map(vpcConfig.highSecurityProjectSubnetworkLabel.value -> "my_network")
                                  ),
                                  FakeGoogleComputeService
    )

    test
      .setUpProjectNetworkAndFirewalls(SetUpProjectNetworkParams(project, RegionName("us-central1")))
      .attempt
      .unsafeRunSync() shouldBe Left(
      InvalidVPCSetupException(project)
    )

    val test2 = new VPCInterpreter(Config.vpcInterpreterConfig,
                                   stubResourceService(
                                     Map(vpcConfig.highSecurityProjectSubnetworkLabel.value -> "my_subnet")
                                   ),
                                   FakeGoogleComputeService
    )

    test2
      .setUpProjectNetworkAndFirewalls(SetUpProjectNetworkParams(project, RegionName("us-central1")))
      .attempt
      .unsafeRunSync() shouldBe Left(
      InvalidVPCSetupException(project)
    )
  }

  it should "create a new subnet if there are no project labels" in {
    val test = new VPCInterpreter(Config.vpcInterpreterConfig, stubResourceService(Map.empty), FakeGoogleComputeService)

    test
      .setUpProjectNetworkAndFirewalls(SetUpProjectNetworkParams(project, RegionName("us-central1")))
      .unsafeRunSync() shouldBe (vpcConfig.networkName, vpcConfig.subnetworkName)
  }

  it should "create firewall rules in the project network" in {
    val computeService = new MockGoogleComputeServiceWithFirewalls()
    val test = new VPCInterpreter(Config.vpcInterpreterConfig, stubResourceService(Map.empty), computeService)

    test
      .setUpProjectFirewalls(
        SetUpProjectFirewallsParams(project, vpcConfig.networkName, RegionName("us-central1"), Map.empty)
      )
      .unsafeRunSync()
    computeService.firewallMap.size shouldBe 4
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
    val test = new VPCInterpreter(Config.vpcInterpreterConfig, stubResourceService(Map.empty), computeService)
    test
      .setUpProjectFirewalls(
        SetUpProjectFirewallsParams(project, vpcConfig.networkName, RegionName("us-central1"), Map.empty)
      )
      .unsafeRunSync()
    vpcConfig.firewallsToRemove.foreach(fw => computeService.firewallMap should not contain key(fw))
  }

  it should "create only firewallrules if necessary" in {
    val computeService = new MockGoogleComputeServiceWithFirewalls()
    val expectedSshFirewallRules = FirewallRuleConfig(
      "leonardo-allow-broad-ssh",
      None,
      allSupportedRegions
        .map(r =>
          r -> List(
            IpRange("69.173.127.0/25"),
            IpRange("69.173.124.0/23"),
            IpRange("69.173.126.0/24"),
            IpRange("69.173.127.230/31"),
            IpRange("69.173.64.0/19"),
            IpRange("69.173.127.224/30"),
            IpRange("69.173.127.192/27"),
            IpRange("69.173.120.0/22"),
            IpRange("69.173.127.228/32"),
            IpRange("69.173.127.232/29"),
            IpRange("69.173.127.128/26"),
            IpRange("69.173.96.0/20"),
            IpRange("69.173.127.240/28"),
            IpRange("69.173.112.0/21")
          )
        )
        .toMap,
      List(Allowed("tcp", Some("22")))
    )
    val expectedIapFirewallRules = FirewallRuleConfig(
      "leonardo-allow-iap-ssh",
      None,
      iapRegions
        .map(r =>
          r -> List(
            IpRange("35.235.240.0/20")
          )
        )
        .toMap,
      List(Allowed("tcp", Some("22")))
    )
    val test = new VPCInterpreter(Config.vpcInterpreterConfig, stubResourceService(Map.empty), computeService)

    test
      .firewallRulesToAdd(
        Map(
          "leonardo-allow-internal-firewall-name" -> "leonardo-allow-internal",
          "leonardo-allow-https-firewall-name" -> "leonardo-ssl"
        )
      )
      .toSet shouldBe Set(expectedSshFirewallRules, expectedIapFirewallRules)
  }

  private def stubResourceService(labels: Map[String, String]): FakeGoogleResourceService =
    new FakeGoogleResourceService {
      override def getLabels(project: GoogleProject)(implicit
        ev: Ask[IO, TraceId]
      ): IO[Option[Map[String, String]]] = IO(Some(labels))
    }

  class MockGoogleComputeServiceWithFirewalls extends FakeGoogleComputeService {
    val firewallMap = scala.collection.concurrent.TrieMap.empty[FirewallRuleName, Firewall]

    override def addFirewallRule(project: GoogleProject, firewall: Firewall)(implicit
      ev: Ask[IO, TraceId]
    ): IO[OperationFuture[Operation, Operation]] =
      IO(firewallMap.putIfAbsent(FirewallRuleName(firewall.getName), firewall))
        .as(new FakeComputeOperationFuture)

    override def deleteFirewallRule(project: GoogleProject, firewallRuleName: FirewallRuleName)(implicit
      ev: Ask[IO, TraceId]
    ): IO[Option[OperationFuture[Operation, Operation]]] =
      IO(firewallMap.remove(firewallRuleName)).as(Some(new FakeComputeOperationFuture))

    override def getNetwork(project: GoogleProject, networkName: NetworkName)(implicit
      ev: Ask[IO, TraceId]
    ): IO[Option[Network]] =
      if (networkName.value == "default")
        IO.pure(Some(Network.newBuilder().setName("default").build))
      else IO.pure(None)
  }

}
