package org.broadinstitute.dsde.workbench.leonardo.util

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.azure.AzureCloudContext
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, AppId, AppName}

trait AKSAlgebra[F[_]] {

  /** Creates an app and polls it for completion */
  def createAndPollApp(params: CreateAKSAppParams)(implicit ev: Ask[F, AppContext]): F[Unit]

  // TODO (TOAZ-230): delete, stop, start

}

final case class CreateAKSAppParams(appId: AppId, appName: AppName, cloudContext: AzureCloudContext)
