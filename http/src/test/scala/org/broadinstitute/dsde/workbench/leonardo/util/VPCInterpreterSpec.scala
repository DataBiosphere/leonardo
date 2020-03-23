package org.broadinstitute.dsde.workbench.leonardo.util

import cats.effect.IO
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.google.cloud.compute.v1.{Firewall, Network, Operation}
import org.broadinstitute.dsde.workbench.google.GoogleProjectDAO
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleProjectDAO
import org.broadinstitute.dsde.workbench.google2.{FirewallRuleName, NetworkName, SubnetworkName}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.LeonardoTestSuite
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao.google.MockGoogleComputeService
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.scalatest.FlatSpecLike

import scala.collection.JavaConverters._
import scala.concurrent.Future

class VPCInterpreterSpec extends FlatSpecLike with LeonardoTestSuite {

  "VPCInterpreter" should "get a subnet from a project label" in {
    val test = new VPCInterpreter(Config.vpcInterpreterConfig,
                                  stubProjectDAO(
                                    Map(vpcConfig.highSecurityProjectNetworkLabel.value -> "my_network",
                                        vpcConfig.highSecurityProjectSubnetworkLabel.value -> "my_subnet")
                                  ),
                                  MockGoogleComputeService)

    test
      .setUpProjectNetwork(SetUpProjectNetworkParams(project))
      .unsafeRunSync() shouldBe (NetworkName("my_network"), SubnetworkName("my_subnet"))
  }

  it should "fail if both labels are not present" in {
    val test = new VPCInterpreter(Config.vpcInterpreterConfig,
                                  stubProjectDAO(
                                    Map(vpcConfig.highSecurityProjectSubnetworkLabel.value -> "my_network")
                                  ),
                                  MockGoogleComputeService)

    test.setUpProjectNetwork(SetUpProjectNetworkParams(project)).attempt.unsafeRunSync() shouldBe Left(
      InvalidVPCSetupException(project)
    )

    val test2 = new VPCInterpreter(Config.vpcInterpreterConfig,
                                   stubProjectDAO(
                                     Map(vpcConfig.highSecurityProjectSubnetworkLabel.value -> "my_subnet")
                                   ),
                                   MockGoogleComputeService)

    test2.setUpProjectNetwork(SetUpProjectNetworkParams(project)).attempt.unsafeRunSync() shouldBe Left(
      InvalidVPCSetupException(project)
    )
  }

  it should "create a new subnet if there are no project labels" in {
    val test = new VPCInterpreter(Config.vpcInterpreterConfig, stubProjectDAO(Map.empty), MockGoogleComputeService)

    test
      .setUpProjectNetwork(SetUpProjectNetworkParams(project))
      .unsafeRunSync() shouldBe (vpcConfig.networkName, vpcConfig.subnetworkName)
  }

  it should "create firewall rules in the project network" in {
    val computeService = new MockGoogleComputeServiceWithFirewalls()
    val test = new VPCInterpreter(Config.vpcInterpreterConfig, stubProjectDAO(Map.empty), computeService)

    test.setUpProjectFirewalls(SetUpProjectFirewallsParams(project, vpcConfig.networkName)).unsafeRunSync()
    computeService.firewallMap.size shouldBe 3
    vpcConfig.firewallsToAdd.foreach { fwConfig =>
      val fw = computeService.firewallMap.get(fwConfig.name)
      fw shouldBe 'defined
      fw.get.getNetwork shouldBe s"projects/${project.value}/global/networks/${fwConfig.network.value}"
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
      computeService.firewallMap.put(fw, Firewall.newBuilder().setName(fw.value).build)
    }
    val test = new VPCInterpreter(Config.vpcInterpreterConfig, stubProjectDAO(Map.empty), computeService)
    test.setUpProjectFirewalls(SetUpProjectFirewallsParams(project, vpcConfig.networkName)).unsafeRunSync()
    vpcConfig.firewallsToRemove.foreach(fw => computeService.firewallMap should not contain key(fw))
  }

  private def stubProjectDAO(labels: Map[String, String]): GoogleProjectDAO =
    new MockGoogleProjectDAO {
      override def getLabels(projectName: String): Future[Map[String, String]] = Future.successful(labels)
    }

  class MockGoogleComputeServiceWithFirewalls extends MockGoogleComputeService {
    val firewallMap = scala.collection.mutable.Map.empty[FirewallRuleName, Firewall]

    override def addFirewallRule(project: GoogleProject, firewall: Firewall)(
      implicit ev: ApplicativeAsk[IO, TraceId]
    ): IO[Operation] =
      IO(firewallMap.put(FirewallRuleName(firewall.getName), firewall)) >> super.addFirewallRule(project, firewall)(ev)

    override def deleteFirewallRule(project: GoogleProject, firewallRuleName: FirewallRuleName)(
      implicit ev: ApplicativeAsk[IO, TraceId]
    ): IO[Unit] = IO(firewallMap.remove(firewallRuleName)).void

    override def getNetwork(project: GoogleProject, networkName: NetworkName)(
      implicit ev: ApplicativeAsk[IO, TraceId]
    ): IO[Option[Network]] =
      if (networkName.value == "default")
        IO.pure(Some(Network.newBuilder().setName("default").build))
      else IO.pure(None)
  }

}
