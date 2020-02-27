package org.broadinstitute.dsde.workbench.leonardo.dao.google

import cats.effect.IO
import cats.mtl.ApplicativeAsk
import com.google.cloud.compute.v1._
import org.broadinstitute.dsde.workbench.google2.{
  DiskName,
  FirewallRuleName,
  GoogleComputeService,
  InstanceName,
  MachineTypeName,
  RegionName,
  ZoneName
}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

// TODO move to wb-libs
class MockGoogleComputeService extends GoogleComputeService[IO] {
  override def createInstance(project: GoogleProject, zone: ZoneName, instance: Instance)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Operation] = IO.pure(Operation.newBuilder().setId("op").build())

  override def deleteInstance(project: GoogleProject, zone: ZoneName, instanceName: InstanceName)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Operation] = IO.pure(Operation.newBuilder().setId("op").build())

  override def getInstance(project: GoogleProject, zone: ZoneName, instanceName: InstanceName)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Option[Instance]] = IO.pure(None)

  override def stopInstance(project: GoogleProject, zone: ZoneName, instanceName: InstanceName)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Operation] = IO.pure(Operation.newBuilder().setId("op").build())

  override def startInstance(project: GoogleProject, zone: ZoneName, instanceName: InstanceName)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Operation] = IO.pure(Operation.newBuilder().setId("op").build())

  override def addInstanceMetadata(project: GoogleProject,
                                   zone: ZoneName,
                                   instanceName: InstanceName,
                                   metadata: Map[String, String])(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    IO.unit

  override def addFirewallRule(project: GoogleProject, firewall: Firewall)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Unit] = IO.unit

  override def getFirewallRule(project: GoogleProject, firewallRuleName: FirewallRuleName)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Option[Firewall]] = IO.pure(None)

  override def setMachineType(project: GoogleProject,
                              zone: ZoneName,
                              instanceName: InstanceName,
                              machineType: MachineTypeName)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    IO.unit

  override def getMachineType(project: GoogleProject, zone: ZoneName, machineTypeName: MachineTypeName)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Option[MachineType]] = IO.pure(Some(MachineType.newBuilder().setMemoryMb(7680).build))

  override def resizeDisk(project: GoogleProject, zone: ZoneName, diskName: DiskName, newSizeGb: Int)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Unit] = IO.unit

  override def getZones(project: GoogleProject, regionName: RegionName)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[List[Zone]] = IO.pure(List(Zone.newBuilder.setName("us-central1-a").build))
}

object MockGoogleComputeService extends MockGoogleComputeService
