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
                                    workspaceId: WorkspaceId,
                                    cloudContext: AzureCloudContext
)

final case class DeleteAKSAppParams(
  appName: AppName,
  workspaceId: WorkspaceId,
  landingZoneResourcesOpt: LandingZoneResources,
  cloudContext: AzureCloudContext,
  keepHistory: Boolean = false
)
