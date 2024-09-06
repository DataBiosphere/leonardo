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
import org.broadinstitute.dsde.workbench.leonardo.dao.sam.SamService

import scala.concurrent.ExecutionContext

class DiskServiceInterp[F[_]: Parallel](config: PersistentDiskConfig,
                                        authProvider: LeoAuthProvider[F],
                                        publisherQueue: Queue[F, LeoPubsubMessage],
                                        googleDiskService: Option[GoogleDiskService[F]],
                                        googleProjectDAO: Option[GoogleProjectDAO],
                                        samService: SamService[F]
)(implicit
  F: Async[F],
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

      // Resolve the user email in Sam from the user token. This translates a pet token to the owner email.
      userEmail <- samService.getUserEmail(userInfo.accessToken.token)

      hasPermission <- authProvider.hasPermission[ProjectSamResourceId, ProjectAction](
        ProjectSamResourceId(googleProject),
        ProjectAction.CreatePersistentDisk,
        userInfo
      )
      _ <- if (hasPermission) F.unit else F.raiseError[Unit](ForbiddenError(userEmail))

      // Grab the pet service account for the user
      petSA <- samService.getPetServiceAccount(userInfo.accessToken.token, googleProject)

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

            // Retrieve parent workspaceId for the google project
            parentWorkspaceId <- samService.lookupWorkspaceParentForGoogleProject(userInfo.accessToken.token,
                                                                                  googleProject
            )

            disk <- F.fromEither(
              convertToDisk(userEmail,
                            petSA,
                            cloudContext,
                            diskName,
                            samResource,
                            config,
                            req,
                            ctx.now,
                            sourceDiskOpt,
                            parentWorkspaceId
              )
            )
            // Create a persistent-disk Sam resource with a creator policy and the google project as the parent
            _ <- samService.createResource(userInfo.accessToken.token,
                                           samResource,
                                           Some(googleProject),
                                           None,
                                           Map("creator" -> SamPolicyData(List(userEmail), List(SamRole.Creator)))
            )
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
      sourceAncestry <- F.fromFuture(F.delay(googleProjectDAO.get.getAncestry(sourceGoogleProject.value)))
      sourceAncestor <- immediateAncestor(sourceAncestry)

      targetAncestry <- F.fromFuture(F.delay(googleProjectDAO.get.getAncestry(targetGoogleProject.value)))
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
      maybeGoogleDisk <- googleDiskService.get.getDisk(sourceDiskReq.googleProject, sourceDisk.zone, sourceDisk.name)
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

      // throw 403 if no project-level permission
      hasProjectPermission <- authProvider.isUserProjectReader(
        cloudContext,
        userInfo
      )
      _ <- F.raiseWhen(!hasProjectPermission)(ForbiddenError(userInfo.userEmail, Some(ctx.traceId)))

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

      // throw 403 if user doesn't have project permission
      hasProjectPermission <- cloudContext.traverse(cc =>
        authProvider.isUserProjectReader(
          cc,
          userInfo
        )
      )
      _ <- F.raiseWhen(!hasProjectPermission.getOrElse(true))(ForbiddenError(userInfo.userEmail, Some(ctx.traceId)))

      paramMap <- F.fromEither(processListParameters(params))
      creatorOnly <- F.fromEither(processCreatorOnlyParameter(userInfo.userEmail, params, ctx.traceId))
      disks <- DiskServiceDbQueries.listDisks(paramMap._1, paramMap._2, creatorOnly, cloudContext).transaction
      partition = disks.partition(_.cloudContext.isInstanceOf[CloudContext.Gcp])
      _ <- ctx.span.traverse(s => F.delay(s.addAnnotation("Done DB call")))

      gcpDiskAndProjects = partition._1.map(d => (GoogleProject(d.cloudContext.asString), d.samResource))
      gcpSamVisibleDisksOpt <- NonEmptyList.fromList(gcpDiskAndProjects).traverse { ds =>
        authProvider
          .filterResourceProjectVisible(ds, userInfo)
      }

      // TODO: use filterUserVisible (and remove old function) or make filterResourceProjectVisible handle both Azure and GCP
      azureDiskAndProjects = partition._2.map(d => (GoogleProject(d.cloudContext.asString), d.samResource))
      azureSamVisibleDisksOpt <- NonEmptyList.fromList(azureDiskAndProjects).traverse { ds =>
        authProvider
          .filterUserVisibleWithProjectFallback(ds, userInfo)
      }

      samVisibleDisksOpt = (gcpSamVisibleDisksOpt, azureSamVisibleDisksOpt) match {
        case (Some(a), Some(b)) => Some(a ++ b)
        case (Some(a), None)    => Some(a)
        case (None, Some(b))    => Some(b)
        case (None, None)       => None
      }

      _ <- ctx.span.traverse(s => F.delay(s.addAnnotation("Done checking Sam permission")))
      res = samVisibleDisksOpt match {
        case None => Vector.empty
        case Some(samVisibleDisks) =>
          val samVisibleDisksSet = samVisibleDisks.toSet
          disks
            .filter(d =>
              samVisibleDisksSet.contains(
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
                                         d.labels.filter(l => paramMap._3.contains(l._1)),
                                         d.workspaceId
              )
            )
            .toVector
      }
      // We authenticate actions on resources. If there are no visible disks,
      // we need to check if user should be able to see the empty list.
      _ <- if (res.isEmpty) authProvider.checkUserEnabled(userInfo) else F.unit
    } yield res

  override def deleteDisk(userInfo: UserInfo, googleProject: GoogleProject, diskName: DiskName)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      cloudContext = CloudContext.Gcp(googleProject)

      // throw 403 if no project-level permission
      hasProjectPermission <- authProvider.isUserProjectReader(
        cloudContext,
        userInfo
      )
      _ <- F.raiseWhen(!hasProjectPermission)(ForbiddenError(userInfo.userEmail, Some(ctx.traceId)))

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
        else F.raiseError[Unit](DiskCannotBeDeletedException(disk.id, disk.status, cloudContext, ctx.traceId))
      // throw 409 if the disk is attached to a runtime
      attached <- persistentDiskQuery.isDiskAttached(disk.id).transaction
      _ <-
        if (attached)
          F.raiseError[Unit](DiskAlreadyAttachedException(CloudContext.Gcp(googleProject), diskName, ctx.traceId))
        else F.unit
      // delete the disk
      _ <- persistentDiskQuery.markPendingDeletion(disk.id, ctx.now).transaction.void >> publisherQueue.offer(
        DeleteDiskMessage(disk.id, Some(ctx.traceId))
      )

    } yield ()

  def deleteAllOrphanedDisks(userInfo: UserInfo,
                             cloudContext: CloudContext.Gcp,
                             runtimeDiskIds: Vector[DiskId],
                             appDisksNames: Vector[DiskName]
  )(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      // Get a list of disks that are not attached to any runtime or app
      disks <- listDisks(
        userInfo,
        Some(cloudContext),
        Map.empty
      )

      orphanedDisks = disks.filterNot(disk => runtimeDiskIds.contains(disk.id) || appDisksNames.contains(disk.name))

      nonDeletableDisks = orphanedDisks.filterNot(disk => DiskStatus.deletableStatuses.contains(disk.status))

      _ <- F
        .raiseError(NonDeletableDisksInProjectFoundException(cloudContext.value, nonDeletableDisks, ctx.traceId))
        .whenA(!nonDeletableDisks.isEmpty)

      _ <- orphanedDisks
        .traverse { disk =>
          deleteDisk(userInfo, cloudContext.value, disk.name)
        }
    } yield ()

  def deleteDiskRecords(userInfo: UserInfo, cloudContext: CloudContext.Gcp, disk: ListPersistentDiskResponse)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      // Find the disk's Sam resource id
      dbdisk <- DiskServiceDbQueries
        .getGetPersistentDiskResponse(cloudContext, disk.name, ctx.traceId)
        .transaction

      listOfPermissions <- authProvider.getActions(dbdisk.samResource, userInfo)

      // throw 404 if no ReadDiskStatus permission
      hasPermission = listOfPermissions.toSet.contains(PersistentDiskAction.ReadPersistentDisk)
      _ <-
        if (hasPermission) F.unit
        else
          F.raiseError[Unit](
            DiskNotFoundException(cloudContext, disk.name, ctx.traceId)
          )

      // throw 403 if no DeleteDisk permission
      hasDeletePermission = listOfPermissions.toSet.contains(PersistentDiskAction.DeletePersistentDisk)
      _ <- if (hasDeletePermission) F.unit else F.raiseError[Unit](ForbiddenError(userInfo.userEmail))

      // Mark the resource as deleted in Leo's DB
      _ <- dbReference.inTransaction(persistentDiskQuery.delete(disk.id, ctx.now))
      // Delete the persistent-disk Sam resource
      _ <- samService.deleteResource(userInfo.accessToken.token, dbdisk.samResource)
    } yield ()

  def deleteAllDisksRecords(userInfo: UserInfo, cloudContext: CloudContext.Gcp)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      disks <- listDisks(
        userInfo,
        Some(cloudContext),
        Map.empty
      )
      _ <- disks.traverse(disk => deleteDiskRecords(userInfo, cloudContext, disk))
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

      // throw 403 if no project-level permission
      hasProjectPermission <- authProvider.isUserProjectReader(
        cloudContext,
        userInfo
      )
      _ <- F.raiseWhen(!hasProjectPermission)(ForbiddenError(userInfo.userEmail, Some(ctx.traceId)))

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
                                     sourceDisk: Option[SourceDisk],
                                     workspaceId: Option[WorkspaceId]
  ): Either[Throwable, PersistentDisk] =
    convertToDisk(userEmail,
                  serviceAccount,
                  cloudContext,
                  diskName,
                  samResource,
                  config,
                  req,
                  now,
                  false,
                  sourceDisk,
                  workspaceId
    )

  private[service] def convertToDisk(userEmail: WorkbenchEmail,
                                     serviceAccount: WorkbenchEmail,
                                     cloudContext: CloudContext,
                                     diskName: DiskName,
                                     samResource: PersistentDiskSamResourceId,
                                     config: PersistentDiskConfig,
                                     req: CreateDiskRequest,
                                     now: Instant,
                                     willBeUsedByGalaxy: Boolean,
                                     sourceDisk: Option[SourceDisk],
                                     workspaceId: Option[WorkspaceId]
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
      sourceDisk.map(_.diskLink),
      None,
      workspaceId
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

case class DiskCannotBeDeletedException(id: DiskId, status: DiskStatus, cloudContext: CloudContext, traceId: TraceId)
    extends LeoException(
      s"Persistent disk ${id.value} cannot be deleted in $status status. CloudContext: ${cloudContext.asStringWithProvider}",
      StatusCodes.Conflict,
      traceId = Some(traceId)
    )

case class DiskCannotBeDeletedWsmException(id: DiskId, status: WsmState, cloudContext: CloudContext, traceId: TraceId)
    extends LeoException(
      s"Persistent disk ${id.value} cannot be deleted in ${status.value} status, please wait and try again. CloudContext: ${cloudContext.asStringWithProvider}",
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
