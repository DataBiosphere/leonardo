package org.broadinstitute.dsde.workbench.leonardo.http

import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsp.ChartVersion

final case class UpdateAppsRequest(appType: AppType,
                                   cloudProvider: CloudProvider,
                                   appVersionsInclude: List[ChartVersion],
                                   appVersionsExclude: List[ChartVersion],
                                   googleProject: Option[GoogleProject],
                                   workspaceId: Option[WorkspaceId],
                                   appNames: List[AppName],
                                   dryRun: Boolean
)

final case class ListUpdateableAppsResponse(workspaceId: Option[WorkspaceId],
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

object ListUpdateableAppsResponse {
  def fromClusters(clusters: List[KubernetesCluster]): List[ListUpdateableAppsResponse] = for {
    cluster <- clusters
    nodepool <- cluster.nodepools
    app <- nodepool.apps
    resp = ListUpdateableAppsResponse(
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
