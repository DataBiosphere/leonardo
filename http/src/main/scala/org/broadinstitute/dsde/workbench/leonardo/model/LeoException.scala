package org.broadinstitute.dsde.workbench.leonardo
package model

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.workbench.leonardo.http.LeoException
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.{GcsPath, GoogleProject}

import scala.util.control.NoStackTrace

final case class AuthenticationError(email: Option[WorkbenchEmail] = None)
    extends LeoException(s"${email.map(e => s"'${e.value}'").getOrElse("Your account")} is not authenticated",
                         StatusCodes.Unauthorized)
    with NoStackTrace

case class ForbiddenError(email: WorkbenchEmail)
    extends LeoException(
      s"${email.value} is unauthorized. " +
        "If you have proper permissions to use the workspace, make sure you are also added to the billing account",
      StatusCodes.Forbidden
    )

case class RuntimeNotFoundByIdException(id: Long, msg: String)
    extends LeoException(s"Runtime with id ${id} not found. Details: ${msg}", StatusCodes.NotFound)

case class RuntimeAlreadyExistsException(googleProject: GoogleProject, runtimeName: RuntimeName, status: RuntimeStatus)
    extends LeoException(
      s"Runtime ${googleProject.value}/${runtimeName.asString} already exists in ${status.toString} status",
      StatusCodes.Conflict
    )

case class RuntimeCannotBeStoppedException(googleProject: GoogleProject,
                                           runtimeName: RuntimeName,
                                           status: RuntimeStatus)
    extends LeoException(
      s"Runtime ${googleProject.value}/${runtimeName.asString} cannot be stopped in ${status.toString} status",
      StatusCodes.Conflict
    )

case class RuntimeCannotBeDeletedException(googleProject: GoogleProject,
                                           runtimeName: RuntimeName,
                                           status: RuntimeStatus = RuntimeStatus.Creating)
    extends LeoException(
      s"Runtime ${googleProject.value}/${runtimeName.asString} cannot be deleted in ${status} status",
      StatusCodes.Conflict
    )

case class RuntimeCannotBeStartedException(googleProject: GoogleProject,
                                           runtimeName: RuntimeName,
                                           status: RuntimeStatus)
    extends LeoException(
      s"Runtime ${googleProject.value}/${runtimeName.asString} cannot be started in ${status.toString} status",
      StatusCodes.Conflict
    )

case class RuntimeOutOfDateException()
    extends LeoException(
      "Your notebook runtime is out of date, and cannot be started due to recent updates in Terra. If you generated " +
        "data or copied external files to the runtime that you want to keep please contact support by emailing " +
        "Terra-support@broadinstitute.zendesk.com. Otherwise, simply delete your existing runtime and create a new one.",
      StatusCodes.Conflict
    )

case class RuntimeCannotBeUpdatedException(projectNameString: String, status: RuntimeStatus, userHint: String = "")
    extends LeoException(s"Runtime ${projectNameString} cannot be updated in ${status} status. ${userHint}",
                         StatusCodes.Conflict)

case class RuntimeMachineTypeCannotBeChangedException(projectNameString: String, status: RuntimeStatus)
    extends LeoException(
      s"Runtime ${projectNameString} in ${status} status must be stopped in order to change machine type. Some updates require stopping the runtime, or a re-create. If you wish Leonardo to handle this for you, investigate the allowStop and allowDelete flags for this API.",
      StatusCodes.Conflict
    )

case class RuntimeDiskSizeCannotBeChangedException(projectNameString: String)
    extends LeoException(
      s"Persistent disk size cannot be updated without restarting the attached runtime ${projectNameString}. Some updates require stopping the runtime, or a re-create. If you wish Leonardo to handle this for you, investigate the allowStop and allowDelete flags for this API.",
      StatusCodes.Conflict
    )

case class RuntimeDiskSizeCannotBeDecreasedException(projectNameString: String)
    extends LeoException(s"Runtime ${projectNameString}: decreasing master disk size is not allowed",
                         StatusCodes.PreconditionFailed)

case class BucketObjectException(gcsUri: String, msg: String)
    extends LeoException(s"${gcsUri} is invalid GCS URI due to ${msg}", StatusCodes.BadRequest)

case class BucketObjectAccessException(userEmail: WorkbenchEmail, gcsUri: GcsPath)
    extends LeoException(s"${userEmail.value} does not have access to ${gcsUri.toUri}", StatusCodes.Forbidden)

case class ParseLabelsException(labelString: String)
    extends LeoException(s"Could not parse label string: $labelString. Expected format [key1=value1,key2=value2,...]",
                         StatusCodes.BadRequest)

case class IllegalLabelKeyException(labelKey: String)
    extends LeoException(s"Labels cannot have a key of '$labelKey'", StatusCodes.NotAcceptable)

case class InvalidDataprocMachineConfigException(errorMsg: String)
    extends LeoException(s"${errorMsg}", StatusCodes.BadRequest)

final case class ImageNotFoundException(traceId: TraceId, image: ContainerImage)
    extends LeoException(s"${traceId} | Image ${image.imageUrl} not found", StatusCodes.NotFound)

final case class InvalidImage(traceId: TraceId, image: ContainerImage, throwable: Option[Throwable])
    extends LeoException(
      s"${traceId} | Image ${image.imageUrl} doesn't have JUPYTER_HOME or RSTUDIO_HOME environment variables defined. Make sure your custom image extends from one of the Terra base images.",
      StatusCodes.NotFound,
      throwable.orNull
    )

final case class CloudServiceNotSupportedException(cloudService: CloudService)
    extends LeoException(
      s"Cloud service ${cloudService.asString} is not support in /api/cluster routes. Please use /api/google/v1/runtime instead.",
      StatusCodes.Conflict
    )
