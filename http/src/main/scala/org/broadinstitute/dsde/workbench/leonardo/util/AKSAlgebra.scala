package org.broadinstitute.dsde.workbench.leonardo.util

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.azure.AzureCloudContext
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, AppId, AppName, LandingZoneResources, WorkspaceId}

trait AKSAlgebra[F[_]] {

  /** Creates an app and polls it for completion */
  def createAndPollApp(params: CreateAKSAppParams)(implicit ev: Ask[F, AppContext]): F[Unit]

  def deleteApp(params: DeleteAKSAppParams)(implicit ev: Ask[F, AppContext]): F[Unit]

}

final case class CreateAKSAppParams(appId: AppId,
                                    appName: AppName,
                                    workspaceId: WorkspaceId,
                                    landingZoneResourcesOpt: Option[LandingZoneResources],
                                    cloudContext: AzureCloudContext
)

final case class DeleteAKSAppParams(
  appName: AppName,
  workspaceId: WorkspaceId,
  landingZoneResourcesOpt: Option[LandingZoneResources],
  cloudContext: AzureCloudContext,
  keepHistory: Boolean = false
)
