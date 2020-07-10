package org.broadinstitute.dsde.workbench.leonardo.service

import cats.effect.IO
import cats.mtl.ApplicativeAsk
import com.google.container.v1.{Cluster, NodePool, Operation}
import org.broadinstitute.dsde.workbench.DoneCheckable
import org.broadinstitute.dsde.workbench.google2.{GKEModels, GKEService}
import org.broadinstitute.dsde.workbench.model.TraceId

import scala.concurrent.duration.FiniteDuration

class MockGKEService extends GKEService[IO] {
  override def createCluster(request: GKEModels.KubernetesCreateClusterRequest)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Operation] = IO(Operation.newBuilder().setName("opName").build())

  override def deleteCluster(clusterId: GKEModels.KubernetesClusterId)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Operation] = IO(Operation.newBuilder().setName("opName").build())

  val testEndpoint = "0.0.0.0"
  override def getCluster(clusterId: GKEModels.KubernetesClusterId)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Option[Cluster]] = IO(Some(Cluster.newBuilder().setEndpoint(testEndpoint).build()))

  override def createNodepool(request: GKEModels.KubernetesCreateNodepoolRequest)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Operation] = IO(Operation.newBuilder().setName("opName").build())

  override def getNodepool(nodepoolId: GKEModels.NodepoolId)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Option[NodePool]] = IO(None)

  override def deleteNodepool(nodepoolId: GKEModels.NodepoolId)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Operation] = IO(Operation.newBuilder().setName("opName").build())

  override def pollOperation(operationId: GKEModels.KubernetesOperationId, delay: FiniteDuration, maxAttempts: Int)(
    implicit ev: ApplicativeAsk[IO, TraceId],
    doneEv: DoneCheckable[Operation]
  ): fs2.Stream[IO, Operation] =
    fs2.Stream(
      Operation
        .newBuilder()
        .setName("opName")
        .setStatus(Operation.Status.DONE)
        .build()
    )
}

object MockGKEService extends MockGKEService
