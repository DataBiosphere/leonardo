package org.broadinstitute.dsde.workbench.leonardo.model

import java.nio.file.Path
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.CloudContext
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}

/**
 * Deprecated: port functionality to SamService, which uses the generated Sam client and models.
 */
@Deprecated
abstract class ServiceAccountProvider[F[_]] {

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
   * @param userInfo the user who is making the Leo request
   * @param cloudContext the cloud context the cluster is created in
   * @return service account email
   */
  def getClusterServiceAccount(userInfo: UserInfo, cloudContext: CloudContext)(implicit
    ev: Ask[F, TraceId]
  ): F[Option[WorkbenchEmail]]

  /**
   * Optional. The service account email that will be localized into the user environment
   * and returned when any application asks [for application default credentials]
   * (https://developers.google.com/identity/protocols/application-default-credentials).
   *
   * If not present, application default credentials will return the service account in
   * instance metadata, i.e. the service account returned by [getClusterServiceAccount].
   *
   * @param userInfo the user who is making the Leo request
   * @param googleProject the Google project the cluster is created in
   * @return service account email
   */
  def getNotebookServiceAccount(userInfo: UserInfo, googleProject: GoogleProject)(implicit
    ev: Ask[F, TraceId]
  ): F[Option[WorkbenchEmail]]

  /**
   *
   * @param userEmail the user email for which we need to get a list of groups that will have access to the staging bucket
   * @return list of groups that will have reader access to a staging bucket
   */
  def listGroupsStagingBucketReaders(userEmail: WorkbenchEmail)(implicit
    ev: Ask[F, TraceId]
  ): F[List[WorkbenchEmail]]

  /**
   *
   * @param userEmail the user email for which we need to get a list of users that will have access to the staging bucket
   * @return list of users that will have reader access to a staging bucket
   */
  def listUsersStagingBucketReaders(userEmail: WorkbenchEmail): F[List[WorkbenchEmail]]

  def getAccessToken(userEmail: WorkbenchEmail, googleProject: GoogleProject)(implicit
    ev: Ask[F, TraceId]
  ): F[Option[String]]
}

/**
 * The service account email and pem file used for making Google Dataproc calls.
 *
 * @return service account email and pem file
 */
final case class ServiceAccountProviderConfig(leoServiceAccountJsonFile: Path, leoServiceAccountEmail: WorkbenchEmail)
