package org.broadinstitute.dsde.workbench.leonardo
package monitor

import _root_.io.circe.parser.decode
import _root_.io.circe.syntax._
import io.circe.Printer
import org.broadinstitute.dsde.workbench.azure._
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.NamespaceName
import org.broadinstitute.dsde.workbench.google2.{DiskName, MachineTypeName, NetworkName, SubnetworkName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.AppType.Galaxy
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubCodec._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.{
  CreateAppMessage,
  CreateAppV2Message,
  CreateAzureRuntimeMessage,
  CreateRuntimeMessage
}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import java.util.UUID

class LeoPubsubCodecSpec extends AnyFlatSpec with Matchers {
  val storageContainerResourceId = WsmControlledResourceId(UUID.randomUUID())

  it should "encode/decode CreateRuntimeMessage.GceConfig properly" in {
    val now = Instant.now()
    val originalMessage = CreateRuntimeMessage(
      1,
      RuntimeProjectAndName(CloudContext.Gcp(GoogleProject("project1")), RuntimeName("runtimeName1")),
      WorkbenchEmail("email1"),
      None,
      AuditInfo(WorkbenchEmail("email1"), now, None, now),
      None,
      None,
      None,
      None,
      Set.empty,
      Set.empty,
      false,
      Map.empty,
      RuntimeConfigInCreateRuntimeMessage.GceConfig(MachineTypeName("n1-standard-4"),
                                                    DiskSize(50),
                                                    bootDiskSize = DiskSize(50),
                                                    zone = ZoneName("us-central1-a"),
                                                    None
      ),
      None,
      Some(1)
    )

    val res = decode[CreateRuntimeMessage](originalMessage.asJson.printWith(Printer.noSpaces))

    res shouldBe Right(originalMessage)
  }

  it should "encode/decode CreateRuntimeMessage.GceWithPdConfig properly" in {
    val now = Instant.now()
    val originalMessage = CreateRuntimeMessage(
      1,
      RuntimeProjectAndName(CloudContext.Gcp(GoogleProject("project1")), RuntimeName("runtimeName1")),
      WorkbenchEmail("email1"),
      None,
      AuditInfo(WorkbenchEmail("email1"), now, None, now),
      None,
      None,
      None,
      None,
      Set.empty,
      Set.empty,
      false,
      Map.empty,
      RuntimeConfigInCreateRuntimeMessage.GceWithPdConfig(MachineTypeName("n1-standard-4"),
                                                          DiskId(2),
                                                          bootDiskSize = DiskSize(50),
                                                          zone = ZoneName("us-central1-a"),
                                                          None
      ),
      None,
      None
    )

    val res = decode[CreateRuntimeMessage](originalMessage.asJson.printWith(Printer.noSpaces))

    res shouldBe Right(originalMessage)
  }

  it should "encode/decode DiskUpdate.PdSizeUpdate properly" in {
    val diskUpdate = DiskUpdate.PdSizeUpdate(DiskId(1), DiskName("name"), DiskSize(10))
    val res = decode[DiskUpdate](diskUpdate.asJson.printWith(Printer.noSpaces))

    res shouldBe Right(diskUpdate)
  }

  it should "encode/decode DiskUpdate.NoPdSizeUpdate properly" in {
    val diskUpdate = DiskUpdate.NoPdSizeUpdate(DiskSize(10))
    val res = decode[DiskUpdate](diskUpdate.asJson.printWith(Printer.noSpaces))

    res shouldBe Right(diskUpdate)
  }

  it should "encode/decode CreateAppMessage properly" in {
    val traceId = TraceId(UUID.randomUUID().toString)
    val originalMessage = CreateAppMessage(
      GoogleProject("project1"),
      Some(
        ClusterNodepoolAction.CreateClusterAndNodepool(KubernetesClusterLeoId(1), NodepoolLeoId(1), NodepoolLeoId(2))
      ),
      AppId(1),
      AppName("app1"),
      Some(DiskId(1)),
      Map.empty,
      Galaxy,
      NamespaceName("ns"),
      None,
      Some(traceId),
      false,
      Some("bucketName")
    )

    val res = decode[CreateAppMessage](originalMessage.asJson.printWith(Printer.noSpaces))

    res shouldBe Right(originalMessage)
  }

  it should "encode/decode CreateAzureRuntimeMessage properly" in {
    val originalMessage =
      CreateAzureRuntimeMessage(1,
                                WorkspaceId(UUID.randomUUID()),
                                false,
                                None,
                                "WorkspaceName",
                                BillingProfileId("spend-profile")
      )

    val res = decode[CreateAzureRuntimeMessage](originalMessage.asJson.printWith(Printer.noSpaces))

    res shouldBe Right(originalMessage)
  }

  val landingZoneResources = LandingZoneResources(
    UUID.randomUUID(),
    AKSCluster("cluster-name", Map.empty[String, Boolean]),
    BatchAccountName("batch-account"),
    RelayNamespace("relay-ns"),
    StorageAccountName("storage-account"),
    NetworkName("vnet"),
    SubnetworkName("batch-subnet"),
    SubnetworkName("aks-subnet"),
    com.azure.core.management.Region.US_EAST,
    ApplicationInsightsName("lzappinsights"),
    Some(PostgresServer("postgres", false))
  )

  it should "encode/decode LandingZoneResources properly" in {
    val res = decode[LandingZoneResources](landingZoneResources.asJson.printWith(Printer.noSpaces))

    res shouldBe Right(landingZoneResources)
  }

  it should "encode/decode CreateAppV2Message properly" in {
    val originalMessage =
      CreateAppV2Message(
        AppId(1),
        AppName("test"),
        WorkspaceId(UUID.randomUUID()),
        CloudContext.Azure(
          AzureCloudContext(
            TenantId("id"),
            SubscriptionId("sub"),
            ManagedResourceGroupName("rg-name")
          )
        ),
        BillingProfileId("spend-profile"),
        None
      )

    val res = decode[CreateAppV2Message](originalMessage.asJson.printWith(Printer.noSpaces))

    res shouldBe Right(originalMessage)
  }
}
