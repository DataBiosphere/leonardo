package org.broadinstitute.dsde.workbench.leonardo.util

import akka.actor.ActorSystem
import cats.data.OptionT
import cats.effect._
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO
import org.broadinstitute.dsde.workbench.leonardo.config.DataprocConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{GoogleComputeDAO, GoogleDataprocDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoException, ServiceAccountInfo}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{GoogleProject, ServiceAccountKey, ServiceAccountKeyId}
import org.broadinstitute.dsde.workbench.google.GoogleUtilities.RetryPredicates._
import org.broadinstitute.dsde.workbench.util.Retry

import scala.concurrent.ExecutionContext

case class ClusterIamSetupException(googleProject: GoogleProject)
  extends LeoException(s"Error occurred setting up IAM roles in project ${googleProject.value}")

class ClusterHelper(dbRef: DbReference,
                    dataprocConfig: DataprocConfig,
                    gdDAO: GoogleDataprocDAO,
                    googleComputeDAO: GoogleComputeDAO,
                    googleIamDAO: GoogleIamDAO)
                   (implicit val executionContext: ExecutionContext, val system: ActorSystem, val contextShift: ContextShift[IO]) extends LazyLogging with Retry {

  def createClusterIamRoles(googleProject: GoogleProject, serviceAccountInfo: ServiceAccountInfo): IO[Unit] = {
    updateClusterIamRoles(googleProject, serviceAccountInfo, true)
  }

  def removeClusterIamRoles(googleProject: GoogleProject, serviceAccountInfo: ServiceAccountInfo): IO[Unit] = {
    updateClusterIamRoles(googleProject, serviceAccountInfo, false)
  }

  private def updateClusterIamRoles(googleProject: GoogleProject, serviceAccountInfo: ServiceAccountInfo, create: Boolean): IO[Unit] = {
    val retryIam: (GoogleProject, WorkbenchEmail, Set[String]) => IO[Boolean] = (project, email, roles) =>
      IO.fromFuture[Boolean](IO(retryExponentially(when409, s"IAM policy change failed for Google project '$project'") { () =>
        if (create) {
          googleIamDAO.addIamRolesForUser(project, email, roles)
        } else {
          googleIamDAO.removeIamRolesForUser(project, email, roles)
        }
      }))

    // Add the Dataproc Worker role in the user's project to the cluster service account, if present.
    // This is needed to be able to spin up Dataproc clusters using a custom service account.
    // If the Google Compute default service account is being used, this is not necessary.
    val dataprocWorkerIO = serviceAccountInfo.clusterServiceAccount.map { email =>
      // Note: don't remove the role if there are existing active clusters owned by the same user,
      // because it could potentially break other clusters. We only check this for the 'remove' case,
      // it's ok to re-add the roles.
      IO.fromFuture(IO(dbRef.inTransaction { _.clusterQuery.countActiveByClusterServiceAccount(email) })).flatMap { count =>
        if (count > 0 && create == false) {
          IO.unit
        } else {
          retryIam(googleProject, email, Set("roles/dataproc.worker"))
        }
      }
    } getOrElse IO.unit

    // Add the Compute Image User role in the image project to the Google API service account.
    // This is needed in order to use a custom dataproc VM image.
    // If a custom image is not being used, this is not necessary.
    val computeImageUserIO = dataprocConfig.customDataprocImage.flatMap(parseImageProject) match {
      case None => IO.unit
      case Some(imageProject) if imageProject == googleProject => IO.unit
      case Some(imageProject) =>
        IO.fromFuture(IO(dbRef.inTransaction { _.clusterQuery.countActiveByProject(googleProject) })).flatMap { count =>
          // Note: don't remove the role if there are existing active clusters in the same project,
          // because it could potentially break other clusters. We only check this for the 'remove' case,
          // it's ok to re-add the roles.
          if (count > 0 && create == false) {
            IO.unit
          } else {
            for {
              emailOpt <- getDataprocServiceAccount(googleProject)
              _ <- emailOpt match {
                case Some(email) => retryIam(imageProject, email, Set("roles/compute.imageUser"))
                case None => IO.raiseError(ClusterIamSetupException(imageProject))
              }
            } yield ()
          }
        }
    }

    List(dataprocWorkerIO, computeImageUserIO).parSequence_
  }

  // Service account email format documented in:
  // https://cloud.google.com/iam/docs/service-accounts#google-managed_service_accounts
  private[leonardo] def googleApiServiceAccount: Long => WorkbenchEmail = projectNumber =>
    WorkbenchEmail(s"$projectNumber@cloudservices.gserviceaccount.com")

  // Service account email format documented in:
  // https://cloud.google.com/dataproc/docs/concepts/iam/iam#service_accounts
  private[leonardo] def dataprocServiceAccount: Long => WorkbenchEmail = projectNumber =>
    WorkbenchEmail(s"service-$projectNumber@dataproc-accounts.iam.gserviceaccount.com")

  // Returns the service account Dataproc uses to perform its actions.
  // Precedence order documented in:
  // https://cloud.google.com/dataproc/docs/concepts/iam/iam#service_accounts
  private def getDataprocServiceAccount(googleProject: GoogleProject): IO[Option[WorkbenchEmail]] = {
    val findSA = (email: WorkbenchEmail) => OptionT(IO.fromFuture(IO(googleIamDAO.findServiceAccount(googleProject, email))).map(_.map(_.email)))
    val findGoogleApiServiceAccount = findSA compose googleApiServiceAccount
    val findDataprocServiceAccount = findSA compose dataprocServiceAccount

    (for {
      number <- OptionT(IO.fromFuture(IO(googleComputeDAO.getProjectNumber(googleProject))))
      // use dataproc service account if it exists; otherwise fall back to the API service account
      precedence = (findDataprocServiceAccount, findGoogleApiServiceAccount).mapN(_ orElse _)
      sa <- precedence(number)
    } yield sa).value
  }

  def generateServiceAccountKey(googleProject: GoogleProject, serviceAccountEmailOpt: Option[WorkbenchEmail]): IO[Option[ServiceAccountKey]] = {
    // TODO: implement google2 version of GoogleIamDAO
    serviceAccountEmailOpt.traverse { email =>
      IO.fromFuture(IO(googleIamDAO.createServiceAccountKey(googleProject, email)))
    }
  }

  def removeServiceAccountKey(googleProject: GoogleProject, serviceAccountEmailOpt: Option[WorkbenchEmail], serviceAccountKeyIdOpt: Option[ServiceAccountKeyId]): IO[Unit] = {
    // TODO: implement google2 version of GoogleIamDAO
    (serviceAccountEmailOpt, serviceAccountKeyIdOpt).mapN { case (email, keyId) =>
      IO.fromFuture(IO(googleIamDAO.removeServiceAccountKey(googleProject, email, keyId)))
    } getOrElse IO.unit
  }

  // See https://cloud.google.com/dataproc/docs/guides/dataproc-images#custom_image_uri
  private def parseImageProject(customDataprocImage: String): Option[GoogleProject] = {
    val regex = ".*projects/(.*)/global/images/(.*)".r
    customDataprocImage match {
      case regex(project, _) => Some(GoogleProject(project))
      case _ => None
    }
  }

}
