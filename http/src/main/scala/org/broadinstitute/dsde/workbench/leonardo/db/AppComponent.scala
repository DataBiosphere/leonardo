package org.broadinstitute.dsde.workbench.leonardo
package db

import akka.http.scaladsl.model.StatusCodes
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{NamespaceName, ServiceAccountName}
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.AppSamResourceId
import org.broadinstitute.dsde.workbench.leonardo.db.DBIOInstances._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.mappedColumnImplicits._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.dummyDate
import org.broadinstitute.dsde.workbench.leonardo.http.WORKSPACE_NAME_KEY
import org.broadinstitute.dsde.workbench.leonardo.model.LeoException
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsp.Release
import org.http4s.Uri
import slick.lifted.Tag

import java.sql.SQLIntegrityConstraintViolationException
import java.time.Instant
import scala.concurrent.ExecutionContext

final case class AppRecord(id: AppId,
                           nodepoolId: NodepoolLeoId,
                           appType: AppType,
                           appName: AppName,
                           appAccessScope: Option[AppAccessScope],
                           workspaceId: Option[WorkspaceId],
                           status: AppStatus,
                           chart: Chart,
                           release: Release,
                           samResourceId: AppSamResourceId,
                           googleServiceAccount: WorkbenchEmail,
                           kubernetesServiceAccount: Option[ServiceAccountName],
                           auditInfo: AuditInfo,
                           namespaceName: NamespaceName,
                           diskId: Option[DiskId],
                           customEnvironmentVariables: Option[Map[String, String]],
                           descriptorPath: Option[Uri],
                           extraArgs: Option[List[String]],
                           sourceWorkspaceId: Option[WorkspaceId],
                           numOfReplicas: Option[Int]
)

class AppTable(tag: Tag) extends Table[AppRecord](tag, "APP") {
  // unique (appName, nodepoolId, workspaceId, destroyedDate)
  def id = column[AppId]("id", O.PrimaryKey, O.AutoInc)
  def nodepoolId = column[NodepoolLeoId]("nodepoolId")
  def appType = column[AppType]("appType", O.Length(254))
  def appName = column[AppName]("appName", O.Length(254))
  def appAccessScope = column[Option[AppAccessScope]]("appAccessScope", O.Length(254))
  def workspaceId = column[Option[WorkspaceId]]("workspaceId", O.Length(254))
  def status = column[AppStatus]("status", O.Length(254))
  def chart = column[Chart]("chart", O.Length(254))
  def release = column[Release]("release", O.Length(254))
  def samResourceId = column[AppSamResourceId]("samResourceId", O.Length(254))
  def googleServiceAccount = column[WorkbenchEmail]("googleServiceAccount", O.Length(254))
  def kubernetesServiceAccount = column[Option[ServiceAccountName]]("kubernetesServiceAccount", O.Length(254))
  def creator = column[WorkbenchEmail]("creator", O.Length(254))
  def createdDate = column[Instant]("createdDate", O.SqlType("TIMESTAMP(6)"))
  def destroyedDate = column[Instant]("destroyedDate", O.SqlType("TIMESTAMP(6)"))
  def dateAccessed = column[Instant]("dateAccessed", O.SqlType("TIMESTAMP(6)"))
  def diskId = column[Option[DiskId]]("diskId", O.Length(254))
  def customEnvironmentVariables = column[Option[Map[String, String]]]("customEnvironmentVariables")
  def descriptorPath = column[Option[Uri]]("descriptorPath", O.Length(1024))
  def extraArgs = column[Option[List[String]]]("extraArgs")
  def sourceWorkspaceId = column[Option[WorkspaceId]]("sourceWorkspaceId", O.Length(254))
  def namespaceName = column[NamespaceName]("namespace", O.Length(254))
  def numOfReplicas = column[Option[Int]]("numOfReplicas", O.SqlType("SMALLINT"))

  def * =
    (
      id,
      nodepoolId,
      appType,
      appName,
      appAccessScope,
      workspaceId,
      status,
      chart,
      release,
      samResourceId,
      googleServiceAccount,
      kubernetesServiceAccount,
      (creator, createdDate, destroyedDate, dateAccessed),
      namespaceName,
      diskId,
      customEnvironmentVariables,
      descriptorPath,
      extraArgs,
      sourceWorkspaceId,
      numOfReplicas
    ) <> ({
      case (
            id,
            nodepoolId,
            appType,
            appName,
            appAccessScope,
            workspaceId,
            status,
            chart,
            release,
            samResourceId,
            googleServiceAccount,
            kubernetesServiceAccount,
            auditInfoRaw,
            namespaceName,
            diskId,
            customEnvironmentVariables,
            descriptorPath,
            extraArgs,
            sourceWorkspaceId,
            numOfReplicas
          ) =>
        AppRecord(
          id,
          nodepoolId,
          appType,
          appName,
          appAccessScope,
          workspaceId,
          status,
          chart,
          release,
          samResourceId,
          googleServiceAccount,
          kubernetesServiceAccount,
          AuditInfo(
            auditInfoRaw._1,
            auditInfoRaw._2,
            LeoProfile.unmarshalDestroyedDate(auditInfoRaw._3),
            auditInfoRaw._4
          ),
          namespaceName,
          diskId,
          customEnvironmentVariables,
          descriptorPath,
          extraArgs,
          sourceWorkspaceId,
          numOfReplicas
        )
    }, { r: AppRecord =>
      Some(
        (
          r.id,
          r.nodepoolId,
          r.appType,
          r.appName,
          r.appAccessScope,
          r.workspaceId,
          r.status,
          r.chart,
          r.release,
          r.samResourceId,
          r.googleServiceAccount,
          r.kubernetesServiceAccount,
          (r.auditInfo.creator,
           r.auditInfo.createdDate,
           r.auditInfo.destroyedDate.getOrElse(dummyDate),
           r.auditInfo.dateAccessed
          ),
          r.namespaceName,
          r.diskId,
          r.customEnvironmentVariables,
          r.descriptorPath,
          r.extraArgs,
          r.sourceWorkspaceId,
          r.numOfReplicas
        )
      )
    })
}

object appQuery extends TableQuery(new AppTable(_)) {
  def unmarshalApp(app: AppRecord,
                   services: List[KubernetesService],
                   labels: LabelMap,
                   namespace: NamespaceName,
                   disk: Option[PersistentDisk],
                   errors: List[AppError]
  ): App =
    App(
      app.id,
      app.nodepoolId,
      app.appType,
      app.appName,
      app.appAccessScope,
      app.workspaceId,
      app.status,
      app.chart,
      app.release,
      AppSamResourceId(app.samResourceId.resourceId, app.appAccessScope),
      app.googleServiceAccount,
      app.auditInfo,
      labels,
      AppResources(
        namespace,
        disk,
        services,
        app.kubernetesServiceAccount
      ),
      errors,
      app.customEnvironmentVariables.getOrElse(Map.empty),
      app.descriptorPath,
      app.extraArgs.getOrElse(List.empty),
      app.sourceWorkspaceId,
      app.numOfReplicas
    )

  def save(saveApp: SaveApp, traceId: Option[TraceId])(implicit ec: ExecutionContext): DBIO[App] = {
    val namespaceName = saveApp.app.appResources.namespace
    for {
      nodepool <- nodepoolQuery
        .getMinimalById(saveApp.app.nodepoolId)
        .flatMap(nodepoolOpt =>
          nodepoolOpt.fold[DBIO[Nodepool]](
            DBIO.failed(
              new SQLIntegrityConstraintViolationException(
                "Apps must be saved with a nodepool ID that exists in the DB. FK_APP_NODEPOOL_ID"
              )
            )
          )(DBIO.successful(_))
        )

      cluster <- kubernetesClusterQuery
        .getMinimalClusterById(nodepool.clusterId)
        .flatMap(clusterOpt =>
          clusterOpt.fold[DBIO[KubernetesCluster]](
            DBIO.failed(
              new SQLIntegrityConstraintViolationException(
                "Apps must be saved with a nodepool that has a cluster ID that exists in the DB. FK_NODEPOOL_CLUSTER_ID"
              )
            )
          )(DBIO.successful(_))
        )

      // v1 apps are unique by (appName, cloudContext)
      // v2 apps are unique by (appName, workspace)
      // This must be enforced at the code level, because the DB schema handles both v1 and v2.
      getAppResult <- saveApp.app.workspaceId match {
        case Some(wid) => KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName(wid, saveApp.app.appName)
        case None      => KubernetesServiceDbQueries.getActiveFullAppByName(cluster.cloudContext, saveApp.app.appName)
      }
      _ <- getAppResult.fold[DBIO[Unit]](DBIO.successful(()))(appResult =>
        DBIO.failed(
          AppExistsException(appResult.app.appName, saveApp.app.workspaceId.toLeft(cluster.cloudContext), traceId)
        )
      )

      diskOpt = saveApp.app.appResources.disk

      ksaOpt = saveApp.app.appResources.kubernetesServiceAccountName

      record = AppRecord(
        AppId(-1),
        saveApp.app.nodepoolId,
        saveApp.app.appType,
        saveApp.app.appName,
        saveApp.app.appAccessScope,
        saveApp.app.workspaceId,
        saveApp.app.status,
        saveApp.app.chart,
        saveApp.app.release,
        saveApp.app.samResourceId,
        saveApp.app.googleServiceAccount,
        ksaOpt,
        saveApp.app.auditInfo,
        namespaceName,
        diskOpt.map(_.id),
        if (saveApp.app.customEnvironmentVariables.isEmpty) None else Some(saveApp.app.customEnvironmentVariables),
        saveApp.app.descriptorPath,
        if (saveApp.app.extraArgs.isEmpty) None else Some(saveApp.app.extraArgs),
        saveApp.app.sourceWorkspaceId,
        saveApp.app.numOfReplicas
      )
      appId <- appQuery returning appQuery.map(_.id) += record
      _ <- labelQuery.saveAllForResource(appId.id, LabelResourceType.App, saveApp.app.labels)
      services <- serviceQuery.saveAllForApp(appId, saveApp.app.appResources.services)
    } yield saveApp.app.copy(id = appId, appResources = AppResources(namespaceName, diskOpt, services, ksaOpt))
  }

  def updateStatus(id: AppId, status: AppStatus): DBIO[Int] =
    getByIdQuery(id)
      .map(_.status)
      .update(status)

  def markAsErrored(id: AppId): DBIO[Int] =
    getByIdQuery(id)
      .map(x => (x.status, x.diskId))
      .update((AppStatus.Error, None))

  def updateChart(id: AppId, chart: Chart): DBIO[Int] =
    getByIdQuery(id)
      .map(_.chart)
      .update(chart)

  def markPendingDeletion(id: AppId): DBIO[Int] =
    updateStatus(id, AppStatus.Deleting)

  def getAppsByNodepool(
    nodepoolId: NodepoolLeoId
  )(implicit ec: ExecutionContext): DBIO[Vector[GetAppsByNodepoolResult]] =
    appQuery
      .filter(_.nodepoolId === nodepoolId)
      .result
      .map(_.map(x => GetAppsByNodepoolResult(x.samResourceId, x.auditInfo.creator)).toVector)

  def markAsDeleted(id: AppId, now: Instant): DBIO[Int] =
    getByIdQuery(id)
      .map(a => (a.status, a.destroyedDate, a.diskId))
      .update((AppStatus.Deleted, now, None))

  def updateDateAccessed(appName: AppName, cloudContext: CloudContext, now: Instant): DBIO[Int] =
    cloudContext match {
      case CloudContext.Gcp(_) =>
        // This query isn't efficient because appName isn't indexed. Future optimization is needed for findActiveByNameQuery method
        findActiveByNameQuery(appName)
          .filter(r => r.workspaceId.isEmpty)
          .map(a => a.dateAccessed)
          .update(now)
      case CloudContext.Azure(_) =>
        DBIO.failed(new RuntimeException("Please don't use this query for Azure apps"))
    }

  def isDiskAttached(diskId: DiskId)(implicit ec: ExecutionContext): DBIO[Boolean] =
    appQuery.filter(x => x.diskId.isDefined && x.diskId === diskId).length.result.map(_ > 0)

  def detachDisk(id: AppId): DBIO[Int] =
    getByIdQuery(id)
      .map(_.diskId)
      .update(None)

  def getDiskId(id: AppId)(implicit ec: ExecutionContext): DBIO[Option[DiskId]] =
    getByIdQuery(id).result.map(_.headOption.flatMap(_.diskId))

  def getLastUsedApp(id: AppId, traceId: Option[TraceId])(implicit ec: ExecutionContext): DBIO[Option[LastUsedApp]] =
    appQuery.filter(_.id === id).result.flatMap { x =>
      x.headOption.traverse[DBIO, LastUsedApp] { case app =>
        app.customEnvironmentVariables match {
          case None =>
            DBIO.failed(new LeoException(s"no customEnvironmentVariables found for ${id}", traceId = traceId))
          case Some(envs) =>
            envs.get(WORKSPACE_NAME_KEY) match {
              case Some(ws) =>
                DBIO.successful(
                  LastUsedApp(app.chart, app.release, app.namespaceName, WorkspaceName(ws))
                )
              case None => DBIO.failed(new LeoException(s"no WORKSPACE_NAME found for ${id}", traceId = traceId))
            }
        }
      }
    }

  def updateKubernetesServiceAccount(id: AppId, ksa: ServiceAccountName): DBIO[Int] =
    getByIdQuery(id)
      .map(_.kubernetesServiceAccount)
      .update(Option(ksa))

  def getAppType(appName: AppName): DBIO[Option[AppType]] =
    findActiveByNameQuery(appName).map(_.appType).result.headOption

  private[db] def getByIdQuery(id: AppId) =
    appQuery.filter(_.id === id)

  private[db] def findActiveByNameQuery(
    appName: AppName
  ): Query[AppTable, AppRecord, Seq] =
    nonDeletedAppQuery.filter(_.appName === appName)

  private[db] def nonDeletedAppQuery: Query[AppTable, AppRecord, Seq] =
    appQuery
      .filter(_.status =!= (AppStatus.Deleted: AppStatus))
      .filter(_.destroyedDate === dummyDate)

  private[db] def filterByWorkspaceIdAndCreator(query: Query[AppTable, AppRecord, Seq],
                                                workspaceId: Option[WorkspaceId],
                                                creatorOnly: Option[WorkbenchEmail]
  ): Query[AppTable, AppRecord, Seq] = {
    val queryWithWorkspaceFilter = workspaceId match {
      case Some(wid) => query.filter(_.workspaceId === wid)
      case None      => query
    }
    creatorOnly match {
      case Some(email) => queryWithWorkspaceFilter.filter(_.creator === email)
      case None        => queryWithWorkspaceFilter
    }
  }
}

case class SaveApp(app: App)
case class AppExistsException(appName: AppName,
                              workspaceOrCloudContext: Either[WorkspaceId, CloudContext],
                              traceId: Option[TraceId]
) extends LeoException(
      s"An app with name ${appName.value} already exists for ${workspaceOrCloudContext
          .fold(wid => s"workspace ${wid.value}", _.asStringWithProvider)}.",
      StatusCodes.Conflict,
      traceId = traceId
    )

final case class WorkspaceName(asString: String) extends AnyVal
final case class LastUsedApp(chart: Chart, release: Release, namespaceName: NamespaceName, workspace: WorkspaceName)
final case class GetAppsByNodepoolResult(samResourceId: SamResourceId.AppSamResourceId, creator: WorkbenchEmail)
