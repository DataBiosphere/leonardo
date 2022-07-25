package org.broadinstitute.dsde.workbench.leonardo
package monitor

import java.time.Instant
import java.util.UUID
import _root_.io.circe.parser.decode
import _root_.io.circe.syntax._
import io.circe.Printer
import org.broadinstitute.dsde.workbench.azure.RelayNamespace
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.NamespaceName
import org.broadinstitute.dsde.workbench.google2.{DiskName, MachineTypeName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.AppType.Galaxy
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubCodec._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.{
  CreateAppMessage,
  CreateAzureRuntimeMessage,
  CreateRuntimeMessage
}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LeoPubsubCodecSpec extends AnyFlatSpec with Matchers {
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
      None
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
      Some(traceId)
    )

    val res = decode[CreateAppMessage](originalMessage.asJson.printWith(Printer.noSpaces))

    res shouldBe Right(originalMessage)
  }

  it should "encode/decode CreateAzureRuntimeMessage properly" in {
    val originalMessage =
      CreateAzureRuntimeMessage(1, WorkspaceId(UUID.randomUUID()), RelayNamespace("relay-ns"), None)

    val res = decode[CreateAzureRuntimeMessage](originalMessage.asJson.printWith(Printer.noSpaces))

    res shouldBe Right(originalMessage)
  }
}
