package org.broadinstitute.dsde.workbench.leonardo.dao.google

import cats.effect.IO
import cats.mtl.ApplicativeAsk
import com.google.cloud.compute.v1._
import org.broadinstitute.dsde.workbench.google2.{
  ComputePollOperation,
  DeviceName,
  DiskName,
  FirewallRuleName,
  GoogleComputeService,
  InstanceName,
  MachineTypeName,
  NetworkName,
  RegionName,
  SubnetworkName,
  ZoneName
}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

// TODO move to wb-libs
class MockGoogleComputeService extends GoogleComputeService[IO] {
  override def createInstance(project: GoogleProject, zone: ZoneName, instance: Instance)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Operation] = IO.pure(Operation.newBuilder().setId("op").setName("opName").setTargetId("target").build())

  override def deleteInstance(project: GoogleProject, zone: ZoneName, instanceName: InstanceName)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Operation] = IO.pure(Operation.newBuilder().setId("op").setName("opName").setTargetId("target").build())

  override def getInstance(project: GoogleProject, zone: ZoneName, instanceName: InstanceName)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Option[Instance]] = IO.pure(None)

  override def stopInstance(project: GoogleProject, zone: ZoneName, instanceName: InstanceName)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Operation] = IO.pure(Operation.newBuilder().setId("op").setName("opName").setTargetId("target").build())

  override def startInstance(project: GoogleProject, zone: ZoneName, instanceName: InstanceName)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Operation] = IO.pure(Operation.newBuilder().setId("op").setName("opName").setTargetId("target").build())

  override def modifyInstanceMetadata(
    project: GoogleProject,
    zone: ZoneName,
    instanceName: InstanceName,
    metadataToAdd: Map[String, String],
    metadataToRemove: Set[String]
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    IO.unit

  override def addFirewallRule(project: GoogleProject, firewall: Firewall)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Operation] = IO.pure(Operation.newBuilder().setId("op").setName("opName").setTargetId("target").build())

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

  override def getZones(project: GoogleProject, regionName: RegionName)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[List[Zone]] = IO.pure(List(Zone.newBuilder.setName("us-central1-a").build))

  override def deleteFirewallRule(project: GoogleProject, firewallRuleName: FirewallRuleName)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Unit] = IO.unit

  override def getNetwork(project: GoogleProject, networkName: NetworkName)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Option[Network]] = IO(None)

  override def createNetwork(project: GoogleProject, network: Network)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Operation] = IO.pure(Operation.newBuilder().setId("op").setName("opName").setTargetId("target").build())

  override def getSubnetwork(project: GoogleProject, region: RegionName, subnetwork: SubnetworkName)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Option[Subnetwork]] = IO(None)

  override def createSubnetwork(project: GoogleProject, region: RegionName, subnetwork: Subnetwork)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Operation] = IO.pure(Operation.newBuilder().setId("op").setName("opName").setTargetId("target").build())

  def detachDisk(project: GoogleProject, zone: ZoneName, instanceName: InstanceName, deviceName: DeviceName)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Operation] = ???

  override def deleteInstanceWithAutoDeleteDisk(
    project: GoogleProject,
    zone: ZoneName,
    instanceName: InstanceName,
    autoDeleteDisks: Set[DiskName]
  )(implicit ev: ApplicativeAsk[IO, TraceId], computePollOperation: ComputePollOperation[IO]): IO[Operation] =
    IO.pure(Operation.newBuilder().setId("op").setName("opName").setStatus("DONE").setTargetId("target").build())
}

object MockGoogleComputeService extends MockGoogleComputeService
