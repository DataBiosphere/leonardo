package org.broadinstitute.dsde.workbench.leonardo.util

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.azure.AzureCloudContext
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceAccountName
import org.broadinstitute.dsde.workbench.leonardo.dao.StorageContainerResponse
import org.broadinstitute.dsde.workbench.leonardo.util.IdentityType.AppWorkloadIdentity
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, AppId, AppName, LandingZoneResources, WorkspaceId}
import org.broadinstitute.dsp.ChartVersion

import java.util.UUID

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
sealed trait IdentityType {
  def getResourceId: Option[UUID] = this match {
    case AppWorkloadIdentity(_, resourceId) => Some(resourceId)
    case _ => None
  }
  def getServiceAccountName: Option[ServiceAccountName] = this match {
    case AppWorkloadIdentity(identity, _) => Some(identity)
    case _ => None
  }
}

object IdentityType {
  // Trait representing the types which run the app with Workload Identity.
  // Currently this is done for apps who provision a database.
  sealed trait WorkloadIdentityType extends IdentityType

  // For apps whose workload identity (to access databases) is linked to an individual user rather than the app itself
  case object UserWorkloadIdentity extends WorkloadIdentityType

  // For apps whose workload identity (to access databases) is linked to the app itself rather than an individual user
  case class AppWorkloadIdentity(serviceAccountName: ServiceAccountName, resourceId: UUID) extends WorkloadIdentityType

  // Runs the app with aad-pod-identity.
  // Currently this is only done for single-user applications who don't provision a database.
  // The pet UAMI is linked to the app.
  // See https://broadworkbench.atlassian.net/browse/IA-3804 for tracking migration to AKS Workload Identity.
  case object PodIdentity extends IdentityType

  // Runs the app with no identity.
  // This is done for multi-user applications who do _not_ provision a database.
  case object NoIdentity extends IdentityType
}

case class AppIdentityAndDatabases(identityType: IdentityType, databases: CreatedDatabaseNames)
