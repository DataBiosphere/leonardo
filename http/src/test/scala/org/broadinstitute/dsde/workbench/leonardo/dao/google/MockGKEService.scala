package org.broadinstitute.dsde.workbench.leonardo.dao.google

import cats.effect.IO
import cats.mtl.ApplicativeAsk
import com.google.container.v1.{Cluster, NodePool, Operation}
import org.broadinstitute.dsde.workbench.DoneCheckable
import org.broadinstitute.dsde.workbench.google2.{GKEModels, GKEService}
import org.broadinstitute.dsde.workbench.model.TraceId

import scala.concurrent.duration.FiniteDuration

class MockGKEService extends GKEService[IO] {
  val testOp = IO.pure(Operation.newBuilder().setName("opName").build())
  override def createCluster(request: GKEModels.KubernetesCreateClusterRequest)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Operation] = testOp

  override def deleteCluster(clusterId: GKEModels.KubernetesClusterId)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Operation] = testOp

  override def getCluster(clusterId: GKEModels.KubernetesClusterId)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Option[Cluster]] = IO(None)

  override def createNodepool(request: GKEModels.KubernetesCreateNodepoolRequest)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Operation] = testOp

  override def getNodepool(nodepoolId: GKEModels.NodepoolId)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[NodePool] = IO(
     null
  )

  override def deleteNodepool(nodepoolId: GKEModels.NodepoolId)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Operation] = testOp

  override def pollOperation(operationId: GKEModels.KubernetesOperationId, delay: FiniteDuration, maxAttempts: Int)(implicit ev: ApplicativeAsk[IO, TraceId], doneEv: DoneCheckable[Operation]): fs2.Stream[IO, Operation] = fs2.Stream[IO, Operation]()
}
