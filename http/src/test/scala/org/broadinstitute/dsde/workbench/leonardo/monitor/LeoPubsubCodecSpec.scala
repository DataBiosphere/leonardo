package org.broadinstitute.dsde.workbench.leonardo.monitor

import java.time.Instant

import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.{AuditInfo, DiskId, DiskSize, RuntimeName, RuntimeProjectAndName}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.CreateRuntimeMessage
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.scalatest.matchers.should.Matchers
import _root_.io.circe.syntax._
import _root_.io.circe.parser.decode
import LeoPubsubCodec._
import io.circe.Printer
import org.scalatest.flatspec.AnyFlatSpec

class LeoPubsubCodecSpec extends AnyFlatSpec with Matchers {
  it should "encode/decode CreateRuntimeMessage.GceConfig properly" in {
    val now = Instant.now()
    val originalMessage = CreateRuntimeMessage(
      1,
      RuntimeProjectAndName(GoogleProject("project1"), RuntimeName("runtimeName1")),
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
                                                    bootDiskSize = DiskSize(50)),
      false,
      None
    )

    val res = decode[CreateRuntimeMessage](originalMessage.asJson.printWith(Printer.noSpaces))

    res shouldBe Right(originalMessage)
  }

  it should "encode/decode CreateRuntimeMessage.GceWithPdConfig properly" in {
    val now = Instant.now()
    val originalMessage = CreateRuntimeMessage(
      1,
      RuntimeProjectAndName(GoogleProject("project1"), RuntimeName("runtimeName1")),
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
                                                          bootDiskSize = DiskSize(50)),
      false,
      None
    )

    val res = decode[CreateRuntimeMessage](originalMessage.asJson.printWith(Printer.noSpaces))

    res shouldBe Right(originalMessage)
  }
}
