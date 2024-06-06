package org.broadinstitute.dsde.workbench.leonardo.http

import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsp.ChartVersion

import java.util.UUID

final case class UpdateAppsRequest(jobId: Option[UpdateAppJobId],
                                   appType: AppType,
                                   cloudProvider: CloudProvider,
                                   appVersionsInclude: List[ChartVersion],
                                   appVersionsExclude: List[ChartVersion],
                                   googleProject: Option[GoogleProject],
                                   workspaceId: Option[WorkspaceId],
                                   appNames: List[AppName],
                                   dryRun: Boolean
)

final case class ListUpdateableAppResponse(jobId: UpdateAppJobId,
                                           workspaceId: Option[WorkspaceId],
                                           cloudContext: CloudContext,
                                           status: AppStatus,
                                           appId: AppId,
                                           appName: AppName,
                                           appType: AppType,
                                           auditInfo: AuditInfo,
                                           chart: Chart,
                                           accessScope: Option[AppAccessScope],
                                           labels: LabelMap
)

object ListUpdateableAppResponse {
  def fromClusters(clusters: List[KubernetesCluster], jobId: UpdateAppJobId): List[ListUpdateableAppResponse] = for {
    cluster <- clusters
    nodepool <- cluster.nodepools
    app <- nodepool.apps
    resp = ListUpdateableAppResponse(
      jobId,
      app.workspaceId,
      cluster.cloudContext,
      app.status,
      app.id,
      app.appName,
      app.appType,
      app.auditInfo,
      app.chart,
      app.appAccessScope,
      app.labels
    )
  } yield resp
}
