package org.broadinstitute.dsde.workbench.leonardo.util

import cats.mtl.Ask
import io.kubernetes.client.openapi.apis.CoreV1Api
import org.broadinstitute.dsde.workbench.azure.{AKSClusterName, AzureCloudContext}
import org.broadinstitute.dsde.workbench.google2.KubernetesModels.{KubernetesNamespace, PodStatus}
import org.broadinstitute.dsde.workbench.leonardo.AppContext

trait KubernetesAlgebra[F[_]] {

  /** Creates a k8s client given an Azure cloud context and cluster name. */
  def createAzureClient(cloudContext: AzureCloudContext, clusterName: AKSClusterName)(implicit
    ev: Ask[F, AppContext]
  ): F[CoreV1Api]

  /** Lists pods in a namespace. */
  def listPodStatus(clusterId: CoreV1Api, namespace: KubernetesNamespace)(implicit
    ev: Ask[F, AppContext]
  ): F[List[PodStatus]]

  /** Creates a namespace. */
  def createNamespace(client: CoreV1Api, namespace: KubernetesNamespace)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit]

  /** Deletes a namespace. */
  def deleteNamespace(client: CoreV1Api, namespace: KubernetesNamespace)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit]

  /** Checks whether a namespace exists. */
  def namespaceExists(client: CoreV1Api, namespace: KubernetesNamespace)(implicit
    ev: Ask[F, AppContext]
  ): F[Boolean]

}
