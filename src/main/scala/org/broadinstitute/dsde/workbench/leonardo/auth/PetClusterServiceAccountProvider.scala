package org.broadinstitute.dsde.workbench.leonardo.auth

import com.typesafe.config.Config
import org.broadinstitute.dsde.workbench.leonardo.model.ServiceAccountProvider
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.{ExecutionContext, Future}

class PetsPerProjectServiceAccountProvider(val config: Config) extends ServiceAccountProvider(config) with SamProvider {

  override def getClusterServiceAccount(workbenchEmail: WorkbenchEmail, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Option[WorkbenchEmail]] = {
    Future {
      Option(samClient.getPetServiceAccount(workbenchEmail, googleProject))
    }
  }

  override def getNotebookServiceAccount(workbenchEmail: WorkbenchEmail, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Option[WorkbenchEmail]] = {
    Future(None)
  }
}
