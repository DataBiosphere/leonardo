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


