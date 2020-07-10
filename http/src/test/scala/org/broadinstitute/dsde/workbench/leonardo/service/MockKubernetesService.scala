package org.broadinstitute.dsde.workbench.leonardo.service
import cats.effect.IO
import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.google2.{GKEModels, KubernetesModels}
import org.broadinstitute.dsde.workbench.model.TraceId

class MockKubernetesService extends org.broadinstitute.dsde.workbench.google2.KubernetesService[IO] {
  override def createNamespace(
    clusterId: GKEModels.KubernetesClusterId,
    namespace: KubernetesModels.KubernetesNamespace
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] = IO.unit

  override def deleteNamespace(
    clusterId: GKEModels.KubernetesClusterId,
    namespace: KubernetesModels.KubernetesNamespace
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] = IO.unit

  override def createServiceAccount(
    clusterId: GKEModels.KubernetesClusterId,
    serviceAccount: KubernetesModels.KubernetesServiceAccount,
    namespaceName: KubernetesModels.KubernetesNamespace
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] = IO.unit

  override def createPod(
    clusterId: GKEModels.KubernetesClusterId,
    pod: KubernetesModels.KubernetesPod,
    namespace: KubernetesModels.KubernetesNamespace
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] = IO.unit

  override def createService(
    clusterId: GKEModels.KubernetesClusterId,
    service: KubernetesModels.KubernetesServiceKind,
    namespace: KubernetesModels.KubernetesNamespace
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] = IO.unit

  override def createRole(
    clusterId: GKEModels.KubernetesClusterId,
    role: KubernetesModels.KubernetesRole,
    namespace: KubernetesModels.KubernetesNamespace
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] = IO.unit

  override def createRoleBinding(
    clusterId: GKEModels.KubernetesClusterId,
    roleBinding: KubernetesModels.KubernetesRoleBinding,
    namespace: KubernetesModels.KubernetesNamespace
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] = IO.unit

  override def createSecret(
    clusterId: GKEModels.KubernetesClusterId,
    namespace: KubernetesModels.KubernetesNamespace,
    secret: KubernetesModels.KubernetesSecret
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] = IO.unit
}

object MockKubernetesService extends MockKubernetesService
