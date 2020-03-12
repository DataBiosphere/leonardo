package org.broadinstitute.dsde.workbench.leonardo.util

import cats.effect.IO
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.google.cloud.compute.v1.Firewall
import org.broadinstitute.dsde.workbench.google.GoogleProjectDAO
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleProjectDAO
import org.broadinstitute.dsde.workbench.google2.FirewallRuleName
import org.broadinstitute.dsde.workbench.leonardo.VPCConfig.{VPCNetwork, VPCSubnet}
import org.broadinstitute.dsde.workbench.leonardo.dao.google.MockGoogleComputeService
import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, LeonardoTestSuite}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.scalatest.FlatSpecLike

import scala.collection.JavaConverters._
import scala.concurrent.Future

class VPCHelperSpec extends FlatSpecLike with LeonardoTestSuite {

  "VPCHelper" should "get a subnet from a project label" in {
    val test = new VPCHelper(CommonTestData.vpcHelperConfig,
                             stubProjectDAO(
                               Map(CommonTestData.vpcHelperConfig.projectVPCSubnetLabelName -> "my_subnet",
                                   CommonTestData.vpcHelperConfig.projectVPCNetworkLabelName -> "my_network")
                             ),
                             MockGoogleComputeService,
                             blocker)

    // subnet should take precedence
    test.getOrCreateVPCSettings(CommonTestData.project).unsafeRunSync() shouldBe VPCSubnet("my_subnet")
  }

  it should "get a network from a project label" in {
    val test = new VPCHelper(CommonTestData.vpcHelperConfig,
                             stubProjectDAO(
                               Map(CommonTestData.vpcHelperConfig.projectVPCNetworkLabelName -> "ny_network")
                             ),
                             MockGoogleComputeService,
                             blocker)

    test.getOrCreateVPCSettings(CommonTestData.project).unsafeRunSync() shouldBe VPCNetwork("ny_network")
  }

  it should "create a new subnet if there are no project labels" in {
    val test =
      new VPCHelper(CommonTestData.vpcHelperConfig, stubProjectDAO(Map.empty), MockGoogleComputeService, blocker)

    test.getOrCreateVPCSettings(CommonTestData.project).unsafeRunSync() shouldBe VPCNetwork("default")
  }

  it should "create a firewall rule with no project labels" in {
    val computeService = new MockGoogleComputeServiceWithFirewalls()
    val test = new VPCHelper(CommonTestData.vpcHelperConfig, stubProjectDAO(Map.empty), computeService, blocker)

    test
      .getOrCreateFirewallRule(CommonTestData.project)
      .unsafeRunSync()
    val createdFirewall = computeService.firewallMap.get(FirewallRuleName(CommonTestData.proxyConfig.firewallRuleName))
    createdFirewall shouldBe 'defined
    createdFirewall.get.getName shouldBe CommonTestData.proxyConfig.firewallRuleName
    createdFirewall.get.getNetwork shouldBe s"projects/${CommonTestData.project.value}/global/networks/default"
    createdFirewall.get.getTargetTagsList.asScala shouldBe List(CommonTestData.proxyConfig.networkTag)
    createdFirewall.get.getAllowedList.asScala.flatMap(_.getPortsList.asScala) shouldBe List(
      CommonTestData.proxyConfig.proxyPort.toString
    )
  }

  it should "create a firewall rule with project labels" in {
    val computeService = new MockGoogleComputeServiceWithFirewalls()
    val test = new VPCHelper(CommonTestData.vpcHelperConfig,
                             stubProjectDAO(
                               Map(CommonTestData.vpcHelperConfig.projectVPCSubnetLabelName -> "my_subnet",
                                   CommonTestData.vpcHelperConfig.projectVPCNetworkLabelName -> "my_network")
                             ),
                             computeService)

    test
      .getOrCreateFirewallRule(CommonTestData.project)
      .unsafeRunSync()
    val createdFirewall = computeService.firewallMap.get(FirewallRuleName(CommonTestData.proxyConfig.firewallRuleName))
    createdFirewall shouldBe 'defined
    createdFirewall.get.getName shouldBe CommonTestData.proxyConfig.firewallRuleName
    // it should ignore the subnet label and use the network
    createdFirewall.get.getNetwork shouldBe s"projects/${CommonTestData.project.value}/global/networks/my_network"
    createdFirewall.get.getTargetTagsList.asScala shouldBe List(CommonTestData.proxyConfig.networkTag)
    createdFirewall.get.getAllowedList.asScala.flatMap(_.getPortsList.asScala) shouldBe List(
      CommonTestData.proxyConfig.proxyPort.toString
    )
  }

  private def stubProjectDAO(labels: Map[String, String]): GoogleProjectDAO =
    new MockGoogleProjectDAO {
      override def getLabels(projectName: String): Future[Map[String, String]] = Future.successful(labels)
    }

  class MockGoogleComputeServiceWithFirewalls extends MockGoogleComputeService {
    val firewallMap = scala.collection.mutable.Map.empty[FirewallRuleName, Firewall]

    override def addFirewallRule(project: GoogleProject, firewall: Firewall)(
      implicit ev: ApplicativeAsk[IO, TraceId]
    ): IO[Unit] = IO(firewallMap.put(FirewallRuleName(firewall.getName), firewall)).void
  }

}
