package org.broadinstitute.dsde.workbench.leonardo.util

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.azure.AzureCloudContext
import org.broadinstitute.dsde.workbench.leonardo.dao.StorageContainerResponse
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, AppId, AppName, LandingZoneResources, WorkspaceId}
import org.broadinstitute.dsp.ChartVersion

trait AKSAlgebra[F[_]] {

  /** Creates an app and polls it for completion */
  def createAndPollApp(params: CreateAKSAppParams)(implicit ev: Ask[F, AppContext]): F[Unit]

  def deleteApp(params: DeleteAKSAppParams)(implicit ev: Ask[F, AppContext]): F[Unit]

  def updateAndPollApp(params: UpdateAKSAppParams)(implicit ev: Ask[F, AppContext]): F[Unit]

}

final case class CreateAKSAppParams(appId: AppId,
                                    appName: AppName,
                                    workspaceId: WorkspaceId,
                                    cloudContext: AzureCloudContext,
                                    landingZoneResources: LandingZoneResources,
                                    storageContainer: Option[StorageContainerResponse]
)

final case class UpdateAKSAppParams(appId: AppId,
                                    appName: AppName,
                                    appChartVersion: ChartVersion,
                                    workspaceId: Option[WorkspaceId],
                                    cloudContext: AzureCloudContext
)

final case class DeleteAKSAppParams(
  appName: AppName,
  workspaceId: WorkspaceId,
  landingZoneResourcesOpt: LandingZoneResources,
  cloudContext: AzureCloudContext
)

/** Enumerates the possible identity modes for an AKS app. */
sealed trait IdentityType
object IdentityType {
  // Runs the app with aad-pod-identity.
  // Currently this is only done for single-user applications who don't provision a database.
  // The pet UAMI is linked to the app.
  // See https://broadworkbench.atlassian.net/browse/IA-3804 for tracking migration to AKS Workload Identity.
  case object PodIdentity extends IdentityType

  // Runs the app with Workload Identity.
  // Currently this is only done for apps who provision a database.
  // The WSM-managed identity is linked to the app
  case object WorkloadIdentity extends IdentityType

  // Runs the app with no identity.
  // This is done for multi-user applications who do _not_ provision a database.
  case object NoIdentity extends IdentityType
}
