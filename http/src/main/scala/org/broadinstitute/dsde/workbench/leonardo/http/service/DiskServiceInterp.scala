package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import akka.http.scaladsl.model.StatusCodes
import cats.Parallel
import cats.data.NonEmptyList
import cats.effect.Async
import cats.effect.std.Queue
import cats.mtl.Ask
import cats.syntax.all._
import com.google.api.services.cloudresourcemanager.model.Ancestor
import org.broadinstitute.dsde.workbench.google.GoogleProjectDAO
import org.typelevel.log4cats.StructuredLogger
import org.broadinstitute.dsde.workbench.google2.{DiskName, GoogleDiskService}
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.config.PersistentDiskConfig
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.service.DiskServiceInterp._
import org.broadinstitute.dsde.workbench.leonardo.model.SamResourceAction._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.{
  CreateDiskMessage,
  DeleteDiskMessage,
  UpdateDiskMessage
}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}

import java.time.Instant
import java.util.UUID
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId._

import scala.concurrent.ExecutionContext

class DiskServiceInterp[F[_]: Parallel](config: PersistentDiskConfig,
                                        authProvider: LeoAuthProvider[F],
                                        serviceAccountProvider: ServiceAccountProvider[F],
                                        publisherQueue: Queue[F, LeoPubsubMessage],
                                        googleDiskService: GoogleDiskService[F],
                                        googleProjectDAO: GoogleProjectDAO
)(implicit
  F: Async[F],
  log: StructuredLogger[F],
  dbReference: DbReference[F],
  ec: ExecutionContext
) extends DiskService[F] {

  override def createDisk(
    userInfo: UserInfo,
    googleProject: GoogleProject,
    diskName: DiskName,
    req: CreateDiskRequest
  )(implicit as: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- as.ask

      hasPermission <- authProvider.hasPermission[ProjectSamResourceId, ProjectAction](
        ProjectSamResourceId(googleProject),
        ProjectAction.CreatePersistentDisk,
        userInfo
      )
      _ <- if (hasPermission) F.unit else F.raiseError[Unit](ForbiddenError(userInfo.userEmail))
      // Grab the service accounts from serviceAccountProvider for use later
      serviceAccountOpt <- serviceAccountProvider
        .getClusterServiceAccount(userInfo, CloudContext.Gcp(googleProject))
      petSA <- F.fromEither(
        serviceAccountOpt.toRight(new Exception(s"user ${userInfo.userEmail.value} doesn't have a PET SA"))
      )

      _ <- req.sourceDisk.traverse(sd => verifyOkToClone(sd.googleProject, googleProject))

      sourceDiskOpt <- req.sourceDisk.traverse(lookupSourceDisk(userInfo, ctx))
      cloudContext = CloudContext.Gcp(googleProject)
      diskOpt <- persistentDiskQuery.getActiveByName(cloudContext, diskName).transaction
      _ <- diskOpt match {
        case Some(c) =>
          F.raiseError[Unit](PersistentDiskAlreadyExistsException(googleProject, diskName, c.status, ctx.traceId))
        case None =>
          for {
            samResource <- F.delay(PersistentDiskSamResourceId(UUID.randomUUID().toString))
            disk <- F.fromEither(
              convertToDisk(userInfo.userEmail,
                            petSA,
                            cloudContext,
                            diskName,
                            samResource,
                            config,
                            req,
                            ctx.now,
                            sourceDiskOpt
              )
            )
            _ <- authProvider
              .notifyResourceCreated(samResource, userInfo.userEmail, googleProject)
              .handleErrorWith { t =>
                log.error(t)(
                  s"[${ctx.traceId}] Failed to notify the AuthProvider for creation of persistent disk ${disk.projectNameString}"
                ) >> F.raiseError(t)
              }
            // TODO: do we need to introduce pre status here?
            savedDisk <- persistentDiskQuery.save(disk).transaction
            _ <- publisherQueue.offer(CreateDiskMessage.fromDisk(savedDisk, Some(ctx.traceId)))
          } yield ()
      }
    } yield ()

  /**
   * Cloning persistent disks creates a data exfiltration path that must be blocked when appropriate. Disks
   * within a controlled perimeter must not be cloned to locations outside that perimeter. However, Leo has no notion
   * of what perimeters are. Fortunately google projects in perimeters are in their own google folder so this check
   * raises an error if source and target google projects are not in the same folder AND the source project is
   * in dontCloneFromTheseGoogleFolders.
   *
   * @param sourceGoogleProject the project containing the disk to be cloned
   * @param targetGoogleProject the project containing the new disk
   * @return
   */
  private def verifyOkToClone(sourceGoogleProject: GoogleProject, targetGoogleProject: GoogleProject)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      sourceAncestry <- F.fromFuture(F.delay(googleProjectDAO.getAncestry(sourceGoogleProject.value)))
      sourceAncestor <- immediateAncestor(sourceAncestry)

      targetAncestry <- F.fromFuture(F.delay(googleProjectDAO.getAncestry(targetGoogleProject.value)))
      targetAncestor <- immediateAncestor(targetAncestry)

      _ <- F.raiseWhen(
        sourceAncestor != targetAncestor &&
          config.dontCloneFromTheseGoogleFolders.contains(sourceAncestor.getResourceId.getId)
      )(
        BadRequestException(s"persistent disk clone from $sourceGoogleProject to $targetGoogleProject not permitted",
                            Option(ctx.traceId)
        )
      )
    } yield ()

  /**
   * Ancestors are ordered from bottom to top of the resource hierarchy.
   * The first ancestor is the project itself, followed by the project's parent, etc..
   */
  private def immediateAncestor(ancestry: Seq[Ancestor])(implicit
    as: Ask[F, AppContext]
  ): F[Ancestor] =
    for {
      ctx <- as.ask
      _ <- F.raiseWhen(ancestry.size < 2)(
        LeoInternalServerError(s"expected at least 2 ancestors but got ${ancestry.mkString(",")}", Option(ctx.traceId))
      )
    } yield ancestry.tail.head

  private def lookupSourceDisk(userInfo: UserInfo, ctx: AppContext)(
    sourceDiskReq: SourceDiskRequest
  )(implicit as: Ask[F, AppContext]): F[SourceDisk] =
    for {
      sourceDisk <- getDisk(userInfo, CloudContext.Gcp(sourceDiskReq.googleProject), sourceDiskReq.name).recoverWith {
        case _: DiskNotFoundException =>
          F.raiseError(BadRequestException("source disk does not exist", Option(ctx.traceId)))
      }
      maybeGoogleDisk <- googleDiskService.getDisk(sourceDiskReq.googleProject, sourceDisk.zone, sourceDisk.name)
      googleDisk <- maybeGoogleDisk.toOptionT.getOrElseF(
        F.raiseError(
          LeoInternalServerError(s"Source disk $sourceDiskReq does not exist in google", Option(ctx.traceId))
        )
      )
    } yield SourceDisk(DiskLink(googleDisk.getSelfLink), sourceDisk.formattedBy)

  override def getDisk(userInfo: UserInfo, cloudContext: CloudContext, diskName: DiskName)(implicit
    as: Ask[F, AppContext]
  ): F[GetPersistentDiskResponse] =
    for {
      ctx <- as.ask
      resp <- DiskServiceDbQueries.getGetPersistentDiskResponse(cloudContext, diskName, ctx.traceId).transaction
      hasPermission <- authProvider.hasPermissionWithProjectFallback[PersistentDiskSamResourceId, PersistentDiskAction](
        resp.samResource,
        PersistentDiskAction.ReadPersistentDisk,
        ProjectAction.ReadPersistentDisk,
        userInfo,
        GoogleProject(cloudContext.asString)
      ) // TODO: update this to support azure
      _ <-
        if (hasPermission) F.unit
        else F.raiseError[Unit](DiskNotFoundException(cloudContext, diskName, ctx.traceId))

    } yield resp

  override def listDisks(userInfo: UserInfo, cloudContext: Option[CloudContext], params: Map[String, String])(implicit
    as: Ask[F, AppContext]
  ): F[Vector[ListPersistentDiskResponse]] =
    for {
      ctx <- as.ask
      paramMap <- F.fromEither(processListParameters(params))
      disks <- DiskServiceDbQueries.listDisks(paramMap._1, paramMap._2, cloudContext).transaction
      _ <- ctx.span.traverse(s => F.delay(s.addAnnotation("Done DB call")))
      diskAndProjects = disks.map(d =>
        (GoogleProject(d.cloudContext.asString), d.samResource)
      ) // TODO: update this to support Azure
      samVisibleDisksOpt <- NonEmptyList.fromList(diskAndProjects).traverse { ds =>
        authProvider
          .filterUserVisibleWithProjectFallback(ds, userInfo)
      }
      _ <- ctx.span.traverse(s => F.delay(s.addAnnotation("Done checking Sam permission")))
      res = samVisibleDisksOpt match {
        case None => Vector.empty
        case Some(samVisibleDisks) =>
          val samVisibleDisksSet = samVisibleDisks.toSet
          // Making the assumption that users will always be able to access disks that they create
          disks
            .filter(d =>
              d.auditInfo.creator == userInfo.userEmail || samVisibleDisksSet.contains(
                (GoogleProject(d.cloudContext.asString), d.samResource)
              )
            )
            .map(d =>
              ListPersistentDiskResponse(d.id,
                                         d.cloudContext,
                                         d.zone,
                                         d.name,
                                         d.status,
                                         d.auditInfo,
                                         d.size,
                                         d.diskType,
                                         d.blockSize,
                                         d.labels.filter(l => paramMap._3.contains(l._1))
              )
            )
            .toVector
      }
    } yield res

  override def deleteDisk(userInfo: UserInfo, googleProject: GoogleProject, diskName: DiskName)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      cloudContext = CloudContext.Gcp(googleProject)
      // throw 404 if not existent
      diskOpt <- persistentDiskQuery.getActiveByName(cloudContext, diskName).transaction
      disk <- diskOpt.fold(F.raiseError[PersistentDisk](DiskNotFoundException(cloudContext, diskName, ctx.traceId)))(
        F.pure
      )
      // throw 404 if no ReadPersistentDisk permission
      // Note: the general pattern is to 404 (e.g. pretend the disk doesn't exist) if the caller doesn't have
      // ReadPersistentDisk permission. We return 403 if the user can view the disk but can't perform some other action.
      listOfPermissions <- authProvider.getActionsWithProjectFallback(disk.samResource, googleProject, userInfo)
      hasReadPermission = listOfPermissions._1.toSet
        .contains(PersistentDiskAction.ReadPersistentDisk) || listOfPermissions._2.toSet
        .contains(ProjectAction.ReadPersistentDisk)
      _ <-
        if (hasReadPermission) F.unit
        else F.raiseError[Unit](DiskNotFoundException(cloudContext, diskName, ctx.traceId))
      // throw 403 if no DeleteDisk permission
      hasDeletePermission = listOfPermissions._1.toSet
        .contains(PersistentDiskAction.DeletePersistentDisk) || listOfPermissions._2.toSet
        .contains(ProjectAction.DeletePersistentDisk)
      _ <- if (hasDeletePermission) F.unit else F.raiseError[Unit](ForbiddenError(userInfo.userEmail))
      // throw 409 if the disk is not deletable
      _ <-
        if (disk.status.isDeletable) F.unit
        else F.raiseError[Unit](DiskCannotBeDeletedException(googleProject, disk.name, disk.status, ctx.traceId))
      // throw 409 if the disk is attached to a runtime
      attached <- persistentDiskQuery.isDiskAttached(disk.id).transaction
      _ <-
        if (attached) F.raiseError[Unit](DiskAlreadyAttachedException(googleProject, diskName, ctx.traceId))
        else F.unit
      // delete the disk
      _ <- persistentDiskQuery.markPendingDeletion(disk.id, ctx.now).transaction.void >> publisherQueue.offer(
        DeleteDiskMessage(disk.id, Some(ctx.traceId))
      )

    } yield ()

  override def updateDisk(
    userInfo: UserInfo,
    googleProject: GoogleProject,
    diskName: DiskName,
    req: UpdateDiskRequest
  )(implicit as: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- as.ask
      cloudContext = CloudContext.Gcp(googleProject)
      // throw 404 if not existent
      diskOpt <- persistentDiskQuery.getActiveByName(cloudContext, diskName).transaction
      disk <- diskOpt.fold(F.raiseError[PersistentDisk](DiskNotFoundException(cloudContext, diskName, ctx.traceId)))(
        F.pure
      )
      // throw 400 if UpdateDiskRequest new size is smaller than disk's current size
      _ <-
        if (req.size.gb > disk.size.gb) for {
          // throw 404 if no ReadPersistentDisk permission
          // Note: the general pattern is to 404 (e.g. pretend the disk doesn't exist) if the caller doesn't have
          // ReadPersistentDisk permission. We return 403 if the user can view the disk but can't perform some other action.
          listOfPermissions <- authProvider.getActionsWithProjectFallback(disk.samResource, googleProject, userInfo)
          hasReadPermission = listOfPermissions._1.toSet
            .contains(PersistentDiskAction.ReadPersistentDisk) || listOfPermissions._2.toSet
            .contains(ProjectAction.ReadPersistentDisk)
          _ <-
            if (hasReadPermission) F.unit
            else F.raiseError[Unit](DiskNotFoundException(cloudContext, diskName, ctx.traceId))
          // throw 403 if no ModifyPersistentDisk permission
          hasModifyPermission = listOfPermissions._1.contains(PersistentDiskAction.ModifyPersistentDisk)
          _ <- if (hasModifyPermission) F.unit else F.raiseError[Unit](ForbiddenError(userInfo.userEmail))
          // throw 409 if the disk is not updatable
          _ <-
            if (disk.status.isUpdatable) F.unit
            else
              F.raiseError[Unit](
                DiskCannotBeUpdatedException(disk.projectNameString, disk.status, traceId = ctx.traceId)
              )
          _ <- publisherQueue.offer(
            UpdateDiskMessage(disk.id, req.size, Some(ctx.traceId))
          )
        } yield ()
        else if (req.size.gb == disk.size.gb) F.unit
        else
          F.raiseError[Unit](DiskNotResizableException(googleProject, diskName, disk.size, req.size, ctx.traceId))
    } yield ()
}

object DiskServiceInterp {
  private[service] def convertToDisk(userEmail: WorkbenchEmail,
                                     serviceAccount: WorkbenchEmail,
                                     cloudContext: CloudContext,
                                     diskName: DiskName,
                                     samResource: PersistentDiskSamResourceId,
                                     config: PersistentDiskConfig,
                                     req: CreateDiskRequest,
                                     now: Instant,
                                     sourceDisk: Option[SourceDisk]
  ): Either[Throwable, PersistentDisk] =
    convertToDisk(userEmail, serviceAccount, cloudContext, diskName, samResource, config, req, now, false, sourceDisk)

  private[service] def convertToDisk(userEmail: WorkbenchEmail,
                                     serviceAccount: WorkbenchEmail,
                                     cloudContext: CloudContext,
                                     diskName: DiskName,
                                     samResource: PersistentDiskSamResourceId,
                                     config: PersistentDiskConfig,
                                     req: CreateDiskRequest,
                                     now: Instant,
                                     willBeUsedByGalaxy: Boolean,
                                     sourceDisk: Option[SourceDisk]
  ): Either[Throwable, PersistentDisk] = {
    // create a LabelMap of default labels
    val defaultLabels = DefaultDiskLabels(
      diskName,
      cloudContext,
      userEmail,
      serviceAccount
    ).toMap

    // combine default and given labels
    val allLabels = req.labels ++ defaultLabels

    for {
      // check the labels do not contain forbidden keys
      labels <-
        if (allLabels.contains(includeDeletedKey))
          Left(IllegalLabelKeyException(includeDeletedKey))
        else
          Right(allLabels)
    } yield PersistentDisk(
      DiskId(0),
      cloudContext,
      req.zone.getOrElse(config.defaultZone),
      diskName,
      serviceAccount,
      samResource,
      DiskStatus.Creating,
      AuditInfo(userEmail, now, None, now),
      if (willBeUsedByGalaxy) req.size.getOrElse(config.defaultGalaxyNfsdiskSizeGb)
      else req.size.getOrElse(config.defaultDiskSizeGb),
      req.diskType.getOrElse(config.defaultDiskType),
      req.blockSize.getOrElse(config.defaultBlockSizeBytes),
      sourceDisk.flatMap(_.formattedBy),
      None,
      labels,
      sourceDisk.map(_.diskLink)
    )
  }
}

case class PersistentDiskAlreadyExistsException(googleProject: GoogleProject,
                                                diskName: DiskName,
                                                status: DiskStatus,
                                                traceId: TraceId
) extends LeoException(
      s"Persistent disk ${googleProject.value}/${diskName.value} already exists in ${status.toString} status",
      StatusCodes.Conflict,
      traceId = Some(traceId)
    )

case class DiskCannotBeDeletedException(googleProject: GoogleProject,
                                        diskName: DiskName,
                                        status: DiskStatus,
                                        traceId: TraceId
) extends LeoException(
      s"Persistent disk ${googleProject.value}/${diskName.value} cannot be deleted in ${status} status",
      StatusCodes.Conflict,
      traceId = Some(traceId)
    )

case class DiskNotFoundException(cloudContext: CloudContext, diskName: DiskName, traceId: TraceId)
    extends LeoException(s"Persistent disk ${cloudContext.asStringWithProvider}/${diskName.value} not found",
                         StatusCodes.NotFound,
                         traceId = Some(traceId)
    )

case class DiskNotFoundByIdException(diskId: DiskId, traceId: TraceId)
    extends LeoException(s"Persistent disk ${diskId.value} not found", StatusCodes.NotFound, traceId = Some(traceId))

case class DiskCannotBeUpdatedException(projectNameString: String,
                                        status: DiskStatus,
                                        userHint: String = "",
                                        traceId: TraceId
) extends LeoException(
      s"Persistent disk ${projectNameString} cannot be updated in ${status} status. ${userHint}",
      StatusCodes.Conflict,
      traceId = Some(traceId)
    )

case class DiskNotResizableException(googleProject: GoogleProject,
                                     diskName: DiskName,
                                     currentDiskSize: DiskSize,
                                     newDiskSize: DiskSize,
                                     traceId: TraceId
) extends LeoException(
      s"Invalid value for disk size. New disk size ${newDiskSize.asString}GB must be larger than existing size of ${currentDiskSize.asString}GB for persistent disk ${googleProject.value}/${diskName.value}",
      StatusCodes.BadRequest,
      traceId = Some(traceId)
    )
