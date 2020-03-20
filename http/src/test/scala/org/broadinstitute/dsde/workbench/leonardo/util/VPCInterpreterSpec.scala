package org.broadinstitute.dsde.workbench.leonardo.util

import org.broadinstitute.dsde.workbench.leonardo.LeonardoTestSuite
import org.scalatest.FlatSpecLike

class VPCInterpreterSpec extends FlatSpecLike with LeonardoTestSuite {

  // TODO rewrite

//  "VPCHelper" should "get a subnet from a project label" in {
//    val test = new VPCHelper(CommonTestData.vpcHelperConfig,
//                             stubProjectDAO(
//                               Map(CommonTestData.vpcHelperConfig.projectVPCSubnetLabelName -> "my_subnet",
//                                   CommonTestData.vpcHelperConfig.projectVPCNetworkLabelName -> "my_network")
//                             ),
//                             MockGoogleComputeService)
//
//    // subnet should take precedence
//    test.getOrCreateVPCSettings(CommonTestData.project).unsafeRunSync() shouldBe VPCSubnet("my_subnet")
//  }
//
//  it should "get a network from a project label" in {
//    val test = new VPCHelper(CommonTestData.vpcHelperConfig,
//                             stubProjectDAO(
//                               Map(CommonTestData.vpcHelperConfig.projectVPCNetworkLabelName -> "ny_network")
//                             ),
//                             MockGoogleComputeService)
//
//    test.getOrCreateVPCSettings(CommonTestData.project).unsafeRunSync() shouldBe VPCNetwork("ny_network")
//  }
//
//  it should "create a new subnet if there are no project labels" in {
//    val test =
//      new VPCHelper(CommonTestData.vpcHelperConfig, stubProjectDAO(Map.empty), MockGoogleComputeService)
//
//    test.getOrCreateVPCSettings(CommonTestData.project).unsafeRunSync() shouldBe VPCNetwork("default")
//  }
//
//  it should "create a firewall rule with no project labels" in {
//    val computeService = new MockGoogleComputeServiceWithFirewalls()
//    val test = new VPCHelper(CommonTestData.vpcHelperConfig, stubProjectDAO(Map.empty), computeService)
//
//    test
//      .getOrCreateFirewallRule(CommonTestData.project)
//      .unsafeRunSync()
//    val createdFirewall = computeService.firewallMap.get(FirewallRuleName(CommonTestData.proxyConfig.firewallRuleName))
//    createdFirewall shouldBe 'defined
//    createdFirewall.get.getName shouldBe CommonTestData.proxyConfig.firewallRuleName
//    createdFirewall.get.getNetwork shouldBe s"projects/${CommonTestData.project.value}/global/networks/default"
//    createdFirewall.get.getTargetTagsList.asScala shouldBe List(CommonTestData.proxyConfig.networkTag)
//    createdFirewall.get.getAllowedList.asScala.flatMap(_.getPortsList.asScala) shouldBe List(
//      CommonTestData.proxyConfig.proxyPort.toString
//    )
//  }
//
//  it should "create a firewall rule with project labels" in {
//    val computeService = new MockGoogleComputeServiceWithFirewalls()
//    val test = new VPCHelper(CommonTestData.vpcHelperConfig,
//                             stubProjectDAO(
//                               Map(CommonTestData.vpcHelperConfig.projectVPCSubnetLabelName -> "my_subnet",
//                                   CommonTestData.vpcHelperConfig.projectVPCNetworkLabelName -> "my_network")
//                             ),
//                             computeService)
//
//    test
//      .getOrCreateFirewallRule(CommonTestData.project)
//      .unsafeRunSync()
//    val createdFirewall = computeService.firewallMap.get(FirewallRuleName(CommonTestData.proxyConfig.firewallRuleName))
//    createdFirewall shouldBe 'defined
//    createdFirewall.get.getName shouldBe CommonTestData.proxyConfig.firewallRuleName
//    // it should ignore the subnet label and use the network
//    createdFirewall.get.getNetwork shouldBe s"projects/${CommonTestData.project.value}/global/networks/my_network"
//    createdFirewall.get.getTargetTagsList.asScala shouldBe List(CommonTestData.proxyConfig.networkTag)
//    createdFirewall.get.getAllowedList.asScala.flatMap(_.getPortsList.asScala) shouldBe List(
//      CommonTestData.proxyConfig.proxyPort.toString
//    )
//  }
//
//  private def stubProjectDAO(labels: Map[String, String]): GoogleProjectDAO =
//    new MockGoogleProjectDAO {
//      override def getLabels(projectName: String): Future[Map[String, String]] = Future.successful(labels)
//    }
//
//  class MockGoogleComputeServiceWithFirewalls extends MockGoogleComputeService {
//    val firewallMap = scala.collection.mutable.Map.empty[FirewallRuleName, Firewall]
//
//    override def addFirewallRule(project: GoogleProject, firewall: Firewall)(
//      implicit ev: ApplicativeAsk[IO, TraceId]
//    ): IO[Unit] = IO(firewallMap.put(FirewallRuleName(firewall.getName), firewall)).void
//  }

}
