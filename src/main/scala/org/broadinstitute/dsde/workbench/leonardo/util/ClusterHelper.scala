package org.broadinstitute.dsde.workbench.leonardo.util

import akka.actor.ActorSystem
import cats.effect._
import cats.implicits._
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO
import org.broadinstitute.dsde.workbench.leonardo.config.DataprocConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{GoogleComputeDAO, GoogleDataprocDAO}
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoException, ServiceAccountInfo}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{GoogleProject, ServiceAccountKey, ServiceAccountKeyId}
import org.broadinstitute.dsde.workbench.util.Retry

import scala.concurrent.ExecutionContext

case class ClusterIamSetupException(googleProject: GoogleProject)
  extends LeoException(s"Error occurred setting up IAM roles in project ${googleProject.value}")

class ClusterHelper(dataprocConfig: DataprocConfig,
                    gdDAO: GoogleDataprocDAO,
                    googleComputeDAO: GoogleComputeDAO,
                    googleIamDAO: GoogleIamDAO)
                   (implicit val executionContext: ExecutionContext, val system: ActorSystem) extends LazyLogging with Retry {

  implicit val cs = IO.contextShift(executionContext)

  def createClusterIamRoles(googleProject: GoogleProject, serviceAccountInfo: ServiceAccountInfo): IO[Unit] = {
    updateClusterIamRoles(googleProject, serviceAccountInfo, true)
  }

  def removeClusterIamRoles(googleProject: GoogleProject, serviceAccountInfo: ServiceAccountInfo): IO[Unit] = {
    updateClusterIamRoles(googleProject, serviceAccountInfo, false)
  }

  private def updateClusterIamRoles(googleProject: GoogleProject, serviceAccountInfo: ServiceAccountInfo, create: Boolean): IO[Unit] = {
    val whenGoogle409: PartialFunction[Throwable, Boolean] = {
      case t: GoogleJsonResponseException => t.getStatusCode == 409
      case _ => false
    }

    val retryIam: (GoogleProject, WorkbenchEmail, Set[String]) => IO[Unit] = (project, email, roles) =>
      IO.fromFuture[Unit](IO(retryExponentially(whenGoogle409, s"IAM policy change failed for Google project '$googleProject'") { () =>
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
      retryIam(googleProject, email, Set("roles/dataproc.worker"))
    } getOrElse IO.unit

    // Add the Compute Image User role in Leo's project to the Google API service account.
    // This is needed in order to use a custom dataproc VM image.
    // If a custom image is not being used, this is not necessary.
    val computeImageUserIO = if (dataprocConfig.customDataprocImage.isDefined) {
      for {
        emailOpt <- IO.fromFuture(IO(googleComputeDAO.getGoogleApiServiceAccount(googleProject)))
        _ <- emailOpt match {
          case Some(email) => retryIam(dataprocConfig.leoGoogleProject, email, Set("roles/compute.imageUser"))
          case None => IO.raiseError(ClusterIamSetupException(googleProject))
        }
      } yield ()
    } else IO.unit

    List(dataprocWorkerIO, computeImageUserIO).sequence[IO, Unit].void
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

}
