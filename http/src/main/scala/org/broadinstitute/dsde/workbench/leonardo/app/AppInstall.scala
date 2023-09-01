package org.broadinstitute.dsde.workbench.leonardo.app

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceAccountName
import org.broadinstitute.dsde.workbench.leonardo.app.AppInstall.Database
import org.broadinstitute.dsde.workbench.leonardo.util.{AKSInterpreterConfig, CreateAKSAppParams}
import org.broadinstitute.dsde.workbench.leonardo.{App, AppContext, AppType}
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
  def helmValues(params: CreateAKSAppParams,
                 config: AKSInterpreterConfig,
                 app: App,
                 relayPath: Uri,
                 ksaName: ServiceAccountName,
                 databaseNames: List[String]
  )(implicit ev: Ask[F, AppContext]): F[Values]

  /** Checks status of the app. */
  def checkStatus(baseUri: Uri, authHeader: Authorization)(implicit ev: Ask[F, AppContext]): F[Boolean]
}

object AppInstall {
  case class Database(prefix: String, allowAccessForAllWorkspaceUsers: Boolean)

  def appTypeToAppInstall[F[_]](wdsAppInstall: WdsAppInstall[F],
                                cromwellAppInstall: CromwellAppInstall[F]
  ): AppType => AppInstall[F] = _ match {
    case AppType.Wds      => wdsAppInstall
    case AppType.Cromwell => cromwellAppInstall
  }
}
