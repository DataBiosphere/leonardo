package org.broadinstitute.dsde.workbench.leonardo.app

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.azure.AzureCloudContext
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceAccountName
import org.broadinstitute.dsde.workbench.leonardo.{App, AppContext, CloudContext, LandingZoneResources, WorkspaceId}
import org.broadinstitute.dsde.workbench.leonardo.app.AppInstall.Database
import org.broadinstitute.dsde.workbench.leonardo.dao.StorageContainerResponse
import org.broadinstitute.dsde.workbench.leonardo.util.{AKSInterpreterConfig, CreateAKSAppParams}
import org.broadinstitute.dsp.{Release, Values}
import org.http4s.Uri
import org.http4s.headers.Authorization

trait AppInstall[F[_]] {
  def databases: List[Database]

  def helmValues(params: CreateAKSAppParams,
                 config: AKSInterpreterConfig,
                 app: App,
                 relayPath: Uri,
                 ksaName: ServiceAccountName,
                 databaseNames: List[String]
  )(implicit ev: Ask[F, AppContext]): F[Values]

  def checkStatus(baseUri: Uri, authHeader: Authorization)(implicit ev: Ask[F, AppContext]): F[Boolean]
}

object AppInstall {
  case class Database(prefix: String, allowAccessForAllWorkspaceUsers: Boolean)
}
