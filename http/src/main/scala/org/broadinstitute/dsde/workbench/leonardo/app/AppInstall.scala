package org.broadinstitute.dsde.workbench.leonardo.app

import bio.terra.workspace.model.CloningInstructionsEnum
import bio.terra.workspace.model.CloningInstructionsEnum.NOTHING
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.azure.AzureCloudContext
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceAccountName
import org.broadinstitute.dsde.workbench.leonardo.dao.StorageContainerResponse
import org.broadinstitute.dsde.workbench.leonardo.util.AKSInterpreterConfig
import org.broadinstitute.dsde.workbench.leonardo.{
  App,
  AppContext,
  AppType,
  BillingProfileId,
  LandingZoneResources,
  ManagedIdentityName,
  WorkspaceId,
  WsmControlledDatabaseResource
}
import org.broadinstitute.dsp.Values
import org.http4s.Uri
import org.http4s.headers.Authorization

/**
 * Defines how to install a Kubernetes App.
 */
trait AppInstall[F[_]] {

  /** List of WSM-controlled databases the app requires. */
  def databases: List[Database]

  /** Builds helm values to be passed to the app. */
  def buildHelmOverrideValues(params: BuildHelmOverrideValuesParams)(implicit ev: Ask[F, AppContext]): F[Values]

  /** Checks status of the app. */
  def checkStatus(baseUri: Uri, authHeader: Authorization)(implicit ev: Ask[F, AppContext]): F[Boolean]

//  /** Checks status of the app. */
//  def checkStatus(cloudContext: CloudContext, runtimeName: RuntimeName)(implicit ev: Ask[F, AppContext]): F[Boolean]

}

object AppInstall {

  /** Maps AppType to AppInstall. */
  def appTypeToAppInstall[F[_]](wdsAppInstall: WdsAppInstall[F],
                                cromwellAppInstall: CromwellAppInstall[F],
                                workflowsAppInstall: WorkflowsAppInstall[F],
                                hailBatchAppInstall: HailBatchAppInstall[F],
                                cromwellRunnerAppInstall: CromwellRunnerAppInstall[F]
  ): AppType => AppInstall[F] = _ match {
    case AppType.Wds               => wdsAppInstall
    case AppType.Cromwell          => cromwellAppInstall
    case AppType.WorkflowsApp      => workflowsAppInstall
    case AppType.HailBatch         => hailBatchAppInstall
    case AppType.CromwellRunnerApp => cromwellRunnerAppInstall
    case e                         => throw new IllegalArgumentException(s"Unexpected app type: ${e}")
  }

  def getAzureDatabaseName(dbResources: List[WsmControlledDatabaseResource], dbPrefix: String): Option[String] =
    dbResources.collectFirst {
      case db if db.wsmDatabaseName.startsWith(dbPrefix) => db.azureDatabaseName
    }
}

sealed trait Database
object Database {

  /** A database attached to the lifecycle of app. */
  final case class ControlledDatabase(prefix: String,
                                      allowAccessForAllWorkspaceUsers: Boolean = false,
                                      cloningInstructions: CloningInstructionsEnum = NOTHING
  ) extends Database

  /** A database that should _not_ be created as part of app creation, but referenced in k8s namespace creation.
   * It is not tied to the lifecycle of app. */
  final case class ReferenceDatabase(name: String) extends Database
}

final case class BuildHelmOverrideValuesParams(app: App,
                                               workspaceId: WorkspaceId,
                                               workspaceName: String,
                                               cloudContext: AzureCloudContext,
                                               billingProfileId: BillingProfileId,
                                               landingZoneResources: LandingZoneResources,
                                               storageContainer: Option[StorageContainerResponse],
                                               relayPath: Uri,
                                               ksaName: ServiceAccountName,
                                               managedIdentityName: ManagedIdentityName,
                                               databaseNames: List[WsmControlledDatabaseResource],
                                               config: AKSInterpreterConfig
)
