package org.broadinstitute.dsde.workbench.leonardo.model

import com.typesafe.config.Config
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}

import scala.concurrent.Future

/**
  * Provides service accounts needed by Leo.
  *
  * @param config any necessary configuration information.
  */
abstract class ServiceAccountProvider(config: Config) {
  /**
    * The service account email used for making Google Dataproc calls.
    *
    * @return service account email
    */
  def getLeoServiceAccount: WorkbenchEmail

  /**
    * Optional. The service account email _passed_ to [dataproc clusters create]
    * (https://cloud.google.com/sdk/gcloud/reference/dataproc/clusters/create)
    * via the `--service-account` parameter, whose credentials will be used to set up the
    * instance and localized into the [GCE metadata server]
    * (https://cloud.google.com/compute/docs/storing-retrieving-metadata).
    *
    * If not present, the [Google Compute Engine default service account]
    * (https://cloud.google.com/compute/docs/access/service-accounts#compute_engine_default_service_account)
    * is used instead.
    *
    * @return service account email
    */
  def getClusterServiceAccount(userInfo: UserInfo, googleProject: GoogleProject): Future[Option[WorkbenchEmail]]

  /**
    * Optional. The service account email that will be localized into the user environment
    * and returned when any application asks [for application default credentials]
    * (https://developers.google.com/identity/protocols/application-default-credentials).
    *
    * If not present, application default credentials will return the service account in
    * instance metadata, i.e. the service account returned by [getClusterServiceAccount].
    *
    * @return service account email
    */
  def getOverrideServiceAccount(userInfo: UserInfo, googleProject: GoogleProject): Future[Option[WorkbenchEmail]]
}
