package org.broadinstitute.dsde.workbench.leonardo.util

import cats.mtl.Ask
import io.kubernetes.client.openapi.apis.CoreV1Api
import org.broadinstitute.dsde.workbench.azure.{AKSClusterName, AzureCloudContext}
import org.broadinstitute.dsde.workbench.google2.GKEModels.KubernetesClusterId
import org.broadinstitute.dsde.workbench.leonardo.AppContext

// TODO add more, use from AKS/GKE interpreter
trait KubernetesAlgebra[F[_]] {

  def createAzureClient(cloudContext: AzureCloudContext, clusterName: AKSClusterName)(implicit
    ev: Ask[F, AppContext]
  ): F[CoreV1Api]

  def createGcpClient(clusterId: KubernetesClusterId)(implicit ev: Ask[F, AppContext]): F[CoreV1Api]

}
