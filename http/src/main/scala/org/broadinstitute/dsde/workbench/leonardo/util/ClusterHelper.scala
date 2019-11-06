package org.broadinstitute.dsde.workbench.leonardo
package util

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import cats.effect._
import cats.implicits._
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.services.admin.directory.model.Group
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO.MemberType
import org.broadinstitute.dsde.workbench.google.{GoogleDirectoryDAO, GoogleIamDAO}
import org.broadinstitute.dsde.workbench.google.GoogleUtilities.RetryPredicates._
import org.broadinstitute.dsde.workbench.leonardo.config.{DataprocConfig, GoogleGroupsConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{GoogleComputeDAO, GoogleDataprocDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.LeoException
import org.broadinstitute.dsde.workbench.leonardo.util.ClusterHelper.{
  ClusterIamSetupException,
  GoogleGroupCreationException,
  ImageProjectNotFoundException
}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{GoogleProject, ServiceAccountKey, ServiceAccountKeyId}
import org.broadinstitute.dsde.workbench.util.Retry

import scala.concurrent.{ExecutionContext, Future}

class ClusterHelper(
  dbRef: DbReference,
  dataprocConfig: DataprocConfig,
  googleGroupsConfig: GoogleGroupsConfig,
  gdDAO: GoogleDataprocDAO,
  googleComputeDAO: GoogleComputeDAO,
  googleDirectoryDAO: GoogleDirectoryDAO,
  googleIamDAO: GoogleIamDAO
)(implicit val executionContext: ExecutionContext, val system: ActorSystem, val contextShift: ContextShift[IO])
    extends LazyLogging
    with Retry {

  def createClusterIamRoles(googleProject: GoogleProject, serviceAccountInfo: ServiceAccountInfo): IO[Unit] =
    updateClusterIamRoles(googleProject, serviceAccountInfo, createCluster = true)

  def removeClusterIamRoles(googleProject: GoogleProject, serviceAccountInfo: ServiceAccountInfo): IO[Unit] =
    updateClusterIamRoles(googleProject, serviceAccountInfo, createCluster = false)

  def generateServiceAccountKey(googleProject: GoogleProject,
                                serviceAccountEmailOpt: Option[WorkbenchEmail]): IO[Option[ServiceAccountKey]] =
    // TODO: implement google2 version of GoogleIamDAO
    serviceAccountEmailOpt.traverse { email =>
      IO.fromFuture(IO(googleIamDAO.createServiceAccountKey(googleProject, email)))
    }

  def removeServiceAccountKey(googleProject: GoogleProject,
                              serviceAccountEmailOpt: Option[WorkbenchEmail],
                              serviceAccountKeyIdOpt: Option[ServiceAccountKeyId]): IO[Unit] =
    // TODO: implement google2 version of GoogleIamDAO
    (serviceAccountEmailOpt, serviceAccountKeyIdOpt).mapN {
      case (email, keyId) =>
        IO.fromFuture(IO(googleIamDAO.removeServiceAccountKey(googleProject, email, keyId)))
    } getOrElse IO.unit

  def setupDataprocImageGoogleGroup(): IO[Boolean] =
    createDataprocImageUserGoogleGroupIfItDoesntExist()
      .flatMap { _ =>
        addIamRoleToDataprocImageGroup(dataprocConfig.customDataprocImage)
      }

  /**
   * Add the user's service account to the Google group.
   * This group has compute.imageUser role on the custom Dataproc image project,
   * which allows the user's cluster to pull the image.
   */
  def updateDataprocImageGroupMembership(googleProject: GoogleProject, createCluster: Boolean): IO[Unit] =
    dataprocConfig.customDataprocImage.flatMap(parseImageProject) match {
      case None => IO.unit
      case Some(imageProject) =>
        IO.fromFuture(IO(dbRef.inTransaction {
            _.clusterQuery.countActiveByProject(googleProject)
          }))
          .flatMap { count =>
            // Note: Don't remove the account if there are existing active clusters in the same project,
            // because it could potentially break other clusters. We only check this for the 'remove' case.
            if (count > 0 && !createCluster) {
              IO.unit
            } else {
              val projectNumberOptIO = IO.fromFuture(IO(googleComputeDAO.getProjectNumber(googleProject)))
              val clusterIamSetupExceptionIO = IO.raiseError[Long](ClusterIamSetupException(imageProject))
              for {
                projectNumber <- projectNumberOptIO.flatMap(_.fold(clusterIamSetupExceptionIO)(IO.pure))
                // Note that the Dataproc service account is used to retrieve the image, and not the user's
                // pet service account. There is one Dataproc service account per Google project. For more details:
                // https://cloud.google.com/dataproc/docs/concepts/iam/iam#service_accounts
                dataprocServiceAccountEmail = WorkbenchEmail(
                  s"service-${projectNumber}@dataproc-accounts.iam.gserviceaccount.com"
                )
                _ <- updateGroupMembership(googleGroupsConfig.dataprocImageProjectGroupEmail,
                                           dataprocServiceAccountEmail,
                                           createCluster)
              } yield ()
            }
          }
    }

  /**
   * Add the Dataproc Worker role in the user's project to the cluster service account, if present.
   * This is needed to be able to spin up Dataproc clusters using a custom service account.
   * If the Google Compute default service account is being used, this is not necessary.
   */
  private def updateClusterIamRoles(googleProject: GoogleProject,
                                    serviceAccountInfo: ServiceAccountInfo,
                                    createCluster: Boolean): IO[Unit] = {
    val retryIam: (GoogleProject, WorkbenchEmail, Set[String]) => IO[Unit] = (project, email, roles) =>
      IO.fromFuture[Unit](IO(retryExponentially(when409, s"IAM policy change failed for Google project '$project'") {
        () =>
          val iamUpdate = if (createCluster) {
            googleIamDAO.addIamRoles(project, email, MemberType.ServiceAccount, roles)
          } else {
            googleIamDAO.removeIamRoles(project, email, MemberType.ServiceAccount, roles)
          }
          iamUpdate map { _ =>
            ()
          }
      }))

    serviceAccountInfo.clusterServiceAccount.map { email =>
      // Note: Don't remove the role if there are existing active clusters owned by the same user,
      // because it could potentially break other clusters. We only check this for the 'remove' case,
      // it's ok to re-add the roles.
      IO.fromFuture(IO(dbRef.inTransaction { _.clusterQuery.countActiveByClusterServiceAccount(email) })).flatMap {
        count =>
          if (count > 0 && !createCluster) {
            IO.unit
          } else {
            retryIam(googleProject, email, Set("roles/dataproc.worker"))
          }
      }
    } getOrElse IO.unit
  }

  private def createDataprocImageUserGoogleGroupIfItDoesntExist(): IO[Unit] = {
    logger.debug(
      s"Checking if Dataproc image user Google group '${googleGroupsConfig.dataprocImageProjectGroupEmail}' already exists..."
    )

    for {
      groupOpt <- IO.fromFuture[Option[Group]](
        IO(googleDirectoryDAO.getGoogleGroup(googleGroupsConfig.dataprocImageProjectGroupEmail))
      )
      _ <- groupOpt match {
        case None =>
          logger.debug(
            s"Dataproc image user Google group '${googleGroupsConfig.dataprocImageProjectGroupEmail}' does not exist. Attempting to create it..."
          )
          createDataprocImageUserGoogleGroup()
        case Some(group) =>
          logger.debug(
            s"Dataproc image user Google group '${googleGroupsConfig.dataprocImageProjectGroupEmail}' already exists: $group \n Won't attempt to create it."
          )
          IO.unit
      }
    } yield ()
  }

  private def createDataprocImageUserGoogleGroup(): IO[Unit] =
    IO.fromFuture[Unit] {
      IO {
        googleDirectoryDAO
          .createGroup(googleGroupsConfig.dataprocImageProjectGroupName,
                       googleGroupsConfig.dataprocImageProjectGroupEmail,
                       Option(googleDirectoryDAO.lockedDownGroupSettings))
          .recoverWith {
            // In case group creation is attempted concurrently by multiple Leo instances
            case e: GoogleJsonResponseException if e.getDetails.getCode == StatusCodes.Conflict.intValue =>
              Future.unit
            case _ =>
              Future.failed(GoogleGroupCreationException(googleGroupsConfig.dataprocImageProjectGroupEmail))
          }
      }
    }

  private def addIamRoleToDataprocImageGroup(customDataprocImage: Option[String]): IO[Boolean] = {
    val computeImageUserRole = Set("roles/compute.imageUser")

    dataprocConfig.customDataprocImage.flatMap(parseImageProject) match {
      case None =>
        IO.raiseError[Boolean](ImageProjectNotFoundException())
      case Some(imageProject) =>
        logger.debug(
          s"Attempting to grant 'compute.imageUser' permissions to '${googleGroupsConfig.dataprocImageProjectGroupEmail}' on project '$imageProject' ..."
        )
        IO.fromFuture[Boolean] {
          IO {
            retryExponentially(
              when409,
              s"IAM policy change failed for '${googleGroupsConfig.dataprocImageProjectGroupEmail}' on Google project '$imageProject'."
            ) { () =>
              googleIamDAO.addIamRoles(imageProject,
                                       googleGroupsConfig.dataprocImageProjectGroupEmail,
                                       MemberType.Group,
                                       computeImageUserRole)
            }
          }
        }
    }
  }

  private def updateGroupMembership(groupEmail: WorkbenchEmail,
                                    memberEmail: WorkbenchEmail,
                                    addToGroup: Boolean): IO[Unit] =
    IO.fromFuture[Unit] {
      IO {
        retryExponentially(when409, s"Could not update group '$groupEmail' for member '$memberEmail'") { () =>
          logger.debug(s"Checking if '$memberEmail' is part of group '$groupEmail'...")
          googleDirectoryDAO.isGroupMember(groupEmail, memberEmail).flatMap {
            case false if (addToGroup) =>
              logger.debug(s"Adding '$memberEmail' to group '$groupEmail'...")
              googleDirectoryDAO.addMemberToGroup(groupEmail, memberEmail)
            case true if (!addToGroup) =>
              logger.debug(s"Removing '$memberEmail' from group '$groupEmail'...")
              googleDirectoryDAO.removeMemberFromGroup(groupEmail, memberEmail)
            case _ => Future.unit
          }
        }
      }
    }

  // See https://cloud.google.com/dataproc/docs/guides/dataproc-images#custom_image_uri
  private def parseImageProject(customDataprocImage: String): Option[GoogleProject] = {
    val regex = ".*projects/(.*)/global/images/(.*)".r
    customDataprocImage match {
      case regex(project, _) => Some(GoogleProject(project))
      case _                 => None
    }
  }

}

object ClusterHelper {
  final case class ClusterIamSetupException(googleProject: GoogleProject)
      extends LeoException(s"Error occurred setting up IAM roles in project ${googleProject.value}")

  final case class GoogleGroupCreationException(googleGroup: WorkbenchEmail)
      extends LeoException(s"Failed to create the Google group '${googleGroup}'", StatusCodes.InternalServerError)

  final case class ImageProjectNotFoundException()
      extends LeoException("Custom Dataproc image project not found", StatusCodes.NotFound)
}
