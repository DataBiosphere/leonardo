package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import cats.Parallel
import cats.effect.Async
import cats.implicits._
import cats.mtl.ApplicativeAsk
import io.chrisdavenport.log4cats.StructuredLogger
import org.broadinstitute.dsde.workbench.google2.DiskName
import org.broadinstitute.dsde.workbench.leonardo.SamResource.PersistentDiskSamResource
import org.broadinstitute.dsde.workbench.leonardo.config.PersistentDiskConfig
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.api.{
  CreateDiskRequest,
  GetPersistentDiskResponse,
  ListPersistentDiskResponse,
  UpdateDiskRequest
}
import org.broadinstitute.dsde.workbench.leonardo.http.service.DiskServiceInterp._
import org.broadinstitute.dsde.workbench.leonardo.http.service.LeonardoService.{
  includeDeletedKey,
  processListParameters
}
import org.broadinstitute.dsde.workbench.leonardo.model.PersistentDiskAction.{
  DeletePersistentDisk,
  ModifyPersistentDisk,
  ReadPersistentDisk
}
import org.broadinstitute.dsde.workbench.leonardo.model.ProjectAction._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.{
  CreateDiskMessage,
  DeleteDiskMessage,
  UpdateDiskMessage
}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}

import scala.concurrent.ExecutionContext

class DiskServiceInterp[F[_]: Parallel](config: PersistentDiskConfig,
                                        authProvider: LeoAuthProvider[F],
                                        serviceAccountProvider: ServiceAccountProvider[F],
                                        publisherQueue: fs2.concurrent.Queue[F, LeoPubsubMessage])(
  implicit F: Async[F],
  log: StructuredLogger[F],
  dbReference: DbReference[F],
  ec: ExecutionContext
) extends DiskService[F] {

  override def createDisk(
    userInfo: UserInfo,
    googleProject: GoogleProject,
    diskName: DiskName,
    req: CreateDiskRequest
  )(implicit as: ApplicativeAsk[F, AppContext]): F[Unit] =
    for {
      ctx <- as.ask

      hasPermission <- authProvider.hasProjectPermission(userInfo, CreatePersistentDisk, googleProject)
      _ <- if (hasPermission) F.unit else F.raiseError[Unit](AuthorizationError(Some(userInfo.userEmail)))
      // Grab the service accounts from serviceAccountProvider for use later
      serviceAccountOpt <- serviceAccountProvider
        .getClusterServiceAccount(userInfo, googleProject)
      petSA <- F.fromEither(
        serviceAccountOpt.toRight(new Exception(s"user ${userInfo.userEmail.value} doesn't have a PET SA"))
      )

      diskOpt <- persistentDiskQuery.getActiveByName(googleProject, diskName).transaction

      _ <- diskOpt match {
        case Some(c) =>
          F.raiseError[Unit](PersistentDiskAlreadyExistsException(googleProject, diskName, c.status, ctx.traceId))
        case None =>
          for {
            samResource <- F.delay(PersistentDiskSamResource(UUID.randomUUID().toString))
            disk <- F.fromEither(
              convertToDisk(userInfo, petSA, googleProject, diskName, samResource, config, req, ctx.now)
            )
            _ <- authProvider
              .notifyResourceCreated(samResource, userInfo.userEmail, googleProject)
              .handleErrorWith { t =>
                log.error(t)(
                  s"[${ctx.traceId}] Failed to notify the AuthProvider for creation of persistent disk ${disk.projectNameString}"
                ) >> F.raiseError(t)
              }
            //TODO: do we need to introduce pre status here?
            savedDisk <- persistentDiskQuery.save(disk).transaction
            _ <- publisherQueue.enqueue1(CreateDiskMessage.fromDisk(savedDisk, Some(ctx.traceId)))
          } yield ()
      }
    } yield ()

  override def getDisk(userInfo: UserInfo, googleProject: GoogleProject, diskName: DiskName)(
    implicit as: ApplicativeAsk[F, AppContext]
  ): F[GetPersistentDiskResponse] =
    for {
      ctx <- as.ask
      resp <- DiskServiceDbQueries.getGetPersistentDiskResponse(googleProject, diskName, ctx.traceId).transaction
      hasPermission <- authProvider.hasPersistentDiskPermission(resp.samResource,
                                                                userInfo,
                                                                ReadPersistentDisk,
                                                                googleProject)
      _ <- if (hasPermission) F.unit
      else F.raiseError[Unit](DiskNotFoundException(googleProject, diskName, ctx.traceId))

    } yield resp

  override def listDisks(userInfo: UserInfo, googleProject: Option[GoogleProject], params: Map[String, String])(
    implicit as: ApplicativeAsk[F, AppContext]
  ): F[Vector[ListPersistentDiskResponse]] =
    for {
      paramMap <- F.fromEither(processListParameters(params))
      disks <- DiskServiceDbQueries.listDisks(paramMap._1, paramMap._2, googleProject).transaction
      samVisibleDisks <- authProvider
        .filterUserVisiblePersistentDisks(userInfo, disks.map(d => (d.googleProject, d.samResource)))
    } yield {
      // Making the assumption that users will always be able to access disks that they create
      disks
        .filter(d =>
          d.auditInfo.creator == userInfo.userEmail || samVisibleDisks.contains((d.googleProject, d.samResource))
        )
        .map(d =>
          ListPersistentDiskResponse(d.id,
                                     d.googleProject,
                                     d.zone,
                                     d.name,
                                     d.status,
                                     d.auditInfo,
                                     d.size,
                                     d.diskType,
                                     d.blockSize)
        )
        .toVector
    }

  override def deleteDisk(userInfo: UserInfo, googleProject: GoogleProject, diskName: DiskName)(
    implicit as: ApplicativeAsk[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      // throw 404 if not existent
      diskOpt <- persistentDiskQuery.getActiveByName(googleProject, diskName).transaction
      disk <- diskOpt.fold(F.raiseError[PersistentDisk](DiskNotFoundException(googleProject, diskName, ctx.traceId)))(
        F.pure
      )
      // throw 404 if no ReadPersistentDisk permission
      // Note: the general pattern is to 404 (e.g. pretend the disk doesn't exist) if the caller doesn't have
      // ReadPersistentDisk permission. We return 403 if the user can view the disk but can't perform some other action.
      hasPermission <- authProvider.hasPersistentDiskPermission(disk.samResource,
                                                                userInfo,
                                                                ReadPersistentDisk,
                                                                googleProject)
      _ <- if (hasPermission) F.unit
      else F.raiseError[Unit](DiskNotFoundException(googleProject, diskName, ctx.traceId))
      // throw 403 if no DeleteDisk permission
      hasDeletePermission <- authProvider.hasPersistentDiskPermission(disk.samResource,
                                                                      userInfo,
                                                                      DeletePersistentDisk,
                                                                      googleProject)
      _ <- if (hasDeletePermission) F.unit else F.raiseError[Unit](AuthorizationError(Some(userInfo.userEmail)))
      // throw 409 if the disk is not deletable
      _ <- if (disk.status.isDeletable) F.unit
      else F.raiseError[Unit](DiskCannotBeDeletedException(disk.googleProject, disk.name, disk.status, ctx.traceId))
      // throw 409 if the disk is attached to a runtime
      attached <- RuntimeServiceDbQueries.isDiskAttachedToRuntime(disk).transaction
      _ <- if (attached) F.raiseError[Unit](DiskAlreadyAttachedException(googleProject, diskName, ctx.traceId))
      else F.unit
      // delete the disk
      _ <- persistentDiskQuery.markPendingDeletion(disk.id, ctx.now).transaction.void >> publisherQueue.enqueue1(
        DeleteDiskMessage(disk.id, Some(ctx.traceId))
      )

    } yield ()

  override def updateDisk(
    userInfo: UserInfo,
    googleProject: GoogleProject,
    diskName: DiskName,
    req: UpdateDiskRequest
  )(implicit as: ApplicativeAsk[F, AppContext]): F[Unit] =
    for {
      ctx <- as.ask
      // throw 404 if not existent
      diskOpt <- persistentDiskQuery.getActiveByName(googleProject, diskName).transaction
      disk <- diskOpt.fold(F.raiseError[PersistentDisk](DiskNotFoundException(googleProject, diskName, ctx.traceId)))(
        F.pure
      )
      // throw 400 if UpdateDiskRequest new size is smaller than disk's current size
      _ <- if (req.size.gb > disk.size.gb) F.unit
      else F.raiseError[Unit](DiskNotResizableException(googleProject, diskName, disk.size, req.size, ctx.traceId))
      // throw 404 if no ReadPersistentDisk permission
      // Note: the general pattern is to 404 (e.g. pretend the disk doesn't exist) if the caller doesn't have
      // ReadPersistentDisk permission. We return 403 if the user can view the disk but can't perform some other action.
      hasPermission <- authProvider.hasPersistentDiskPermission(disk.samResource,
                                                                userInfo,
                                                                ReadPersistentDisk,
                                                                googleProject)
      _ <- if (hasPermission) F.unit
      else F.raiseError[Unit](DiskNotFoundException(googleProject, diskName, ctx.traceId))
      // throw 403 if no ModifyPersistentDisk permission
      hasModifyPermission <- authProvider.hasPersistentDiskPermission(disk.samResource,
                                                                      userInfo,
                                                                      ModifyPersistentDisk,
                                                                      googleProject)
      _ <- if (hasModifyPermission) F.unit else F.raiseError[Unit](AuthorizationError(Some(userInfo.userEmail)))
      // throw 409 if the disk is not updatable
      _ <- if (disk.status.isUpdatable) F.unit
      else
        F.raiseError[Unit](DiskCannotBeUpdatedException(disk.projectNameString, disk.status, traceId = ctx.traceId))
      _ <- publisherQueue.enqueue1(
        UpdateDiskMessage(disk.id, req.size, Some(ctx.traceId))
      )
    } yield ()
}

object DiskServiceInterp {
  private[service] def convertToDisk(userInfo: UserInfo,
                                     serviceAccount: WorkbenchEmail,
                                     googleProject: GoogleProject,
                                     diskName: DiskName,
                                     samResource: PersistentDiskSamResource,
                                     config: PersistentDiskConfig,
                                     req: CreateDiskRequest,
                                     now: Instant): Either[Throwable, PersistentDisk] = {
    // create a LabelMap of default labels
    val defaultLabels = DefaultDiskLabels(
      diskName,
      googleProject,
      userInfo.userEmail,
      serviceAccount
    ).toMap

    // combine default and given labels
    val allLabels = req.labels ++ defaultLabels

    for {
      // check the labels do not contain forbidden keys
      labels <- if (allLabels.contains(includeDeletedKey))
        Left(IllegalLabelKeyException(includeDeletedKey))
      else
        Right(allLabels)
    } yield PersistentDisk(
      DiskId(0),
      googleProject,
      config.zone,
      diskName,
      None,
      serviceAccount,
      samResource,
      DiskStatus.Creating,
      AuditInfo(userInfo.userEmail, now, None, now),
      req.size.getOrElse(config.defaultDiskSizeGB),
      req.diskType.getOrElse(config.defaultDiskType),
      req.blockSize.getOrElse(config.defaultBlockSizeBytes),
      labels
    )
  }
}

case class PersistentDiskAlreadyExistsException(googleProject: GoogleProject,
                                                diskName: DiskName,
                                                status: DiskStatus,
                                                traceId: TraceId)
    extends LeoException(
      s"${traceId} | Persistent disk ${googleProject.value}/${diskName.value} already exists in ${status.toString} status",
      StatusCodes.Conflict
    )

case class DiskCannotBeDeletedException(googleProject: GoogleProject,
                                        diskName: DiskName,
                                        status: DiskStatus,
                                        traceId: TraceId)
    extends LeoException(
      s"${traceId} | Persistent disk ${googleProject.value}/${diskName.value} cannot be deleted in ${status} status",
      StatusCodes.Conflict
    )

case class DiskNotFoundException(googleProject: GoogleProject, diskName: DiskName, traceId: TraceId)
    extends LeoException(s"${traceId} | Persistent disk ${googleProject.value}/${diskName.value} not found",
                         StatusCodes.NotFound)

case class DiskCannotBeUpdatedException(projectNameString: String,
                                        status: DiskStatus,
                                        userHint: String = "",
                                        traceId: TraceId)
    extends LeoException(
      s"${traceId} | Persistent disk ${projectNameString} cannot be updated in ${status} status. ${userHint}",
      StatusCodes.Conflict
    )

case class DiskNotResizableException(googleProject: GoogleProject,
                                     diskName: DiskName,
                                     currentDiskSize: DiskSize,
                                     newDiskSize: DiskSize,
                                     traceId: TraceId)
    extends LeoException(
      s"${traceId} | Invalid value for disk size. New disk size ${newDiskSize.asString}GB must be larger than existing size of ${currentDiskSize.asString}GB for persistent disk ${googleProject.value}/${diskName.value}",
      StatusCodes.BadRequest
    )
