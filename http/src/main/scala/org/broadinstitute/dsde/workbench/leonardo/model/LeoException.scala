package org.broadinstitute.dsde.workbench.leonardo
package model

import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import org.broadinstitute.dsde.workbench.leonardo.http.{
  errorReportSource,
  ListPersistentDiskResponse,
  ListRuntimeResponse2
}
import org.broadinstitute.dsde.workbench.model.google.{GcsPath, GoogleProject}
import org.broadinstitute.dsde.workbench.leonardo.db.WsmResourceType
import org.broadinstitute.dsde.workbench.model.{ErrorReport, TraceId, WorkbenchEmail, WorkbenchException}

import scala.util.control.NoStackTrace

class LeoException(val message: String = null,
                   val statusCode: StatusCode = StatusCodes.InternalServerError,
                   val cause: Throwable = null,
                   extraMessageInLogging: String = "",
                   traceId: Option[TraceId]
) extends WorkbenchException(message, cause) {
  override def getMessage: String = if (message != null) message else super.getMessage

  def getLoggingMessage: String = s"${getMessage}. ${extraMessageInLogging}"

  def getLoggingContext: Map[String, String] = traceId match {
    case Some(value) => Map("traceId" -> value.asString)
    case None        => Map.empty
  }

  def toErrorReport: ErrorReport =
    ErrorReport(Option(getMessage).getOrElse(""), Some(statusCode), Seq(), Seq(), Some(this.getClass), traceId)
}

final case class BadRequestException(msg: String, traceId: Option[TraceId])
    extends LeoException(msg, StatusCodes.BadRequest, traceId = traceId)

final case class AuthenticationError(email: Option[WorkbenchEmail] = None, extraMessage: String = "")
    extends LeoException(
      s"${email.map(e => s"'${e.value}'").getOrElse("Your account")} is not authenticated",
      StatusCodes.Unauthorized,
      traceId = None,
      extraMessageInLogging = extraMessage
    )
    with NoStackTrace

case class ForbiddenError(email: WorkbenchEmail, traceId: Option[TraceId] = None)
    extends LeoException(
      s"${email.value} is unauthorized. " +
        "If you have proper permissions to use the workspace, make sure you are also added to the billing account",
      StatusCodes.Forbidden,
      traceId = traceId
    )

case class NotAnAdminError(email: WorkbenchEmail, traceId: Option[TraceId] = None)
    extends LeoException(
      s"${email.value} is not a Terra admin.",
      StatusCodes.Forbidden,
      traceId = traceId
    )

case class NoMatchingAppError(appType: AppType, cloudProvider: CloudProvider, traceId: Option[TraceId] = None)
    extends LeoException(
      s"No matching app config found for appType ${appType.toString} and cloud provider ${cloudProvider.asString}. Leo likely doesn't deploy this app on this cloud.",
      StatusCodes.PreconditionFailed,
      traceId = traceId
    )

final case class LeoInternalServerError(msg: String, traceId: Option[TraceId])
    extends LeoException(
      s"${msg}",
      StatusCodes.InternalServerError,
      traceId = traceId
    )

case class RuntimeNotFoundException(cloudContext: CloudContext,
                                    runtimeName: RuntimeName,
                                    msg: String,
                                    traceId: Option[TraceId] = None
) extends LeoException(
      s"Runtime ${cloudContext.asStringWithProvider}/${runtimeName.asString} not found",
      StatusCodes.NotFound,
      extraMessageInLogging = msg,
      traceId = traceId
    )

final case class RuntimeNotFoundByWorkspaceIdException(workspaceId: WorkspaceId,
                                                       runtimeName: RuntimeName,
                                                       msg: String,
                                                       traceId: Option[TraceId] = None
) extends LeoException(
      s"Runtime ${runtimeName.asString} not found in workspace ${workspaceId.value}",
      StatusCodes.NotFound,
      extraMessageInLogging = s"Details: ${msg}",
      traceId = traceId
    )

case class RuntimeNotFoundByIdException(id: Long, msg: String)
    extends LeoException(s"Runtime with id ${id} not found. Details: ${msg}", StatusCodes.NotFound, traceId = None)

case class RuntimeAlreadyExistsException(cloudContext: CloudContext, runtimeName: RuntimeName, status: RuntimeStatus)
    extends LeoException(
      s"Runtime ${cloudContext.asStringWithProvider}/${runtimeName.asString} already exists in ${status.toString} status",
      StatusCodes.Conflict,
      traceId = None
    )

case class RuntimeCannotBeStoppedException(cloudContext: CloudContext, runtimeName: RuntimeName, status: RuntimeStatus)
    extends LeoException(
      s"Runtime ${cloudContext.asStringWithProvider}/${runtimeName.asString} cannot be stopped in ${status.toString} status",
      StatusCodes.Conflict,
      traceId = None
    )

case class RuntimeCannotBeDeletedException(cloudContext: CloudContext, runtimeName: RuntimeName, status: RuntimeStatus)
    extends LeoException(
      s"Runtime ${cloudContext.asStringWithProvider}/${runtimeName.asString} cannot be deleted in ${status.toString} status, please wait and try again",
      StatusCodes.Conflict,
      traceId = None
    )

case class RuntimeCannotBeDeletedWsmException(cloudContext: CloudContext, runtimeName: RuntimeName, status: WsmState)
    extends LeoException(
      s"Runtime ${cloudContext.asStringWithProvider}/${runtimeName.asString} cannot be deleted in ${status.value} status, please wait and try again",
      StatusCodes.Conflict,
      traceId = None
    )

case class RuntimeCannotBeStartedException(cloudContext: CloudContext, runtimeName: RuntimeName, status: RuntimeStatus)
    extends LeoException(
      s"Runtime ${cloudContext.asStringWithProvider} cannot be started in ${status.toString} status",
      StatusCodes.Conflict,
      traceId = None
    )

case class RuntimeOutOfDateException()
    extends LeoException(
      "Your notebook runtime is out of date, and cannot be started due to recent updates in Terra. If you generated " +
        "data or copied external files to the runtime that you want to keep please contact support by emailing " +
        "Terra-support@broadinstitute.zendesk.com. Otherwise, simply delete your existing runtime and create a new one.",
      StatusCodes.Conflict,
      traceId = None
    )

case class RuntimeCannotBeUpdatedException(projectNameString: String, status: RuntimeStatus, userHint: String = "")
    extends LeoException(s"Runtime ${projectNameString} cannot be updated in ${status} status. ${userHint}",
                         StatusCodes.Conflict,
                         traceId = None
    )

case class RuntimeMachineTypeCannotBeChangedException(projectNameString: String, status: RuntimeStatus)
    extends LeoException(
      s"Runtime ${projectNameString} in ${status} status must be stopped in order to change machine type. Some updates require stopping the runtime, or a re-create. If you wish Leonardo to handle this for you, investigate the allowStop and allowDelete flags for this API.",
      StatusCodes.Conflict,
      traceId = None
    )

case class RuntimeDiskSizeCannotBeChangedException(projectNameString: String)
    extends LeoException(
      s"Persistent disk size cannot be updated without restarting the attached runtime ${projectNameString}. Some updates require stopping the runtime, or a re-create. If you wish Leonardo to handle this for you, investigate the allowStop and allowDelete flags for this API.",
      StatusCodes.Conflict,
      traceId = None
    )

case class RuntimeDiskSizeCannotBeDecreasedException(projectNameString: String)
    extends LeoException(s"Runtime ${projectNameString}: decreasing master disk size is not allowed",
                         StatusCodes.PreconditionFailed,
                         traceId = None
    )

case class BucketObjectException(gcsUri: String, msg: String)
    extends LeoException(s"${gcsUri} is invalid GCS URI due to ${msg}", StatusCodes.BadRequest, traceId = None)

case class BucketObjectAccessException(userEmail: WorkbenchEmail, gcsUri: GcsPath)
    extends LeoException(s"${userEmail.value} does not have access to ${gcsUri.toUri}",
                         StatusCodes.Forbidden,
                         traceId = None
    )

case class ParseLabelsException(msg: String) extends LeoException(msg, StatusCodes.BadRequest, traceId = None)

case class IllegalLabelKeyException(labelKey: String)
    extends LeoException(s"Labels cannot have a key of '$labelKey'", StatusCodes.NotAcceptable, traceId = None)

case class InvalidDataprocMachineConfigException(errorMsg: String)
    extends LeoException(s"${errorMsg}", StatusCodes.BadRequest, traceId = None)

final case class ImageNotFoundException(traceId: TraceId, image: ContainerImage)
    extends LeoException(s"Image ${image.imageUrl} not found", StatusCodes.NotFound, traceId = Some(traceId))

final case class InvalidImage(traceId: TraceId, image: ContainerImage, throwable: Option[Throwable])
    extends LeoException(
      s"Image ${image.imageUrl} doesn't have JUPYTER_HOME or RSTUDIO_HOME environment variables defined. Make sure your custom image extends from one of the Terra base images.",
      StatusCodes.NotFound,
      throwable.orNull,
      traceId = Some(traceId)
    )

final case class CloudServiceNotSupportedException(cloudService: CloudService)
    extends LeoException(
      s"Cloud service ${cloudService.asString} is not support in /api/cluster routes. Please use /api/google/v1/runtime instead.",
      StatusCodes.Conflict,
      traceId = None
    )

final case class DiskAlreadyFormattedError(alreadyFormattedBy: FormattedBy, targetAppName: String, traceId: TraceId)
    extends LeoException(
      message = s"Disk is formatted by $alreadyFormattedBy already, cannot be used for $targetAppName app",
      traceId = Some(traceId)
    )

case class NonDeletableRuntimesInWorkspaceFoundException(
  workspaceId: WorkspaceId,
  msg: String,
  traceId: Option[TraceId] = None
) extends LeoException(
      s"Workspace ${workspaceId} contains runtimes in a non deletable state.",
      StatusCodes.Conflict,
      extraMessageInLogging = s"Details: ${msg}",
      traceId = traceId
    )

case class NonDeletableRuntimesInProjectFoundException(googleProject: GoogleProject,
                                                       runtimes: Vector[ListRuntimeResponse2],
                                                       traceId: TraceId
) extends LeoException(
      s"Runtime(s) in project ${googleProject.value} with (name(s), status(es)) ${runtimes
          .map(runtime => s"(${runtime.clusterName},${runtime.status})")} cannot be deleted due to their status(es).",
      StatusCodes.Conflict,
      traceId = Some(traceId)
    )

case class NonDeletableDisksInProjectFoundException(googleProject: GoogleProject,
                                                    disks: Vector[ListPersistentDiskResponse],
                                                    traceId: TraceId
) extends LeoException(
      s"Disks(s) in project ${googleProject.value} with (name(s), status(es)) ${disks
          .map(disk => s"(${disk.name},${disk.status})")} cannot be deleted due to their status(es).",
      StatusCodes.Conflict,
      traceId = Some(traceId)
    )

case class AppResourceCannotBeDeletedException(wsmResourceId: WsmControlledResourceId,
                                               appId: AppId,
                                               status: String,
                                               wsmResourceType: WsmResourceType,
                                               traceId: TraceId
) extends LeoException(
      s"Azure ${wsmResourceType.toString} with id ${wsmResourceId.value} associated with ${appId.id} cannot be deleted in $status status in WSM, please wait and try again",
      StatusCodes.Conflict,
      traceId = Some(traceId)
    )
