package org.broadinstitute.dsde.workbench.leonardo

import monocle.Lens
import monocle.macros.GenLens
import org.broadinstitute.dsde.workbench.leonardo.db.GetClusterKey
import org.broadinstitute.dsde.workbench.leonardo.http.service.{CreateRuntimeAPIResponse, ListRuntimeResponse}

object LeoLenses {
  val clusterToClusterImages: Lens[Cluster, Set[RuntimeImage]] = GenLens[Cluster](_.clusterImages)

  val clusterToAuditInfo: Lens[Cluster, AuditInfo] = GenLens[Cluster](_.auditInfo)

  val clusterToRuntimeConfigId: Lens[Cluster, RuntimeConfigId] = GenLens[Cluster](_.runtimeConfigId)

  val createClusterAPIRespToGetClusterKey = Lens[CreateRuntimeAPIResponse, GetClusterKey](
    x => GetClusterKey(x.googleProject, x.clusterName, x.auditInfo.destroyedDate)
  )(
    x =>
      a =>
        a.copy(googleProject = x.googleProject,
               clusterName = x.clusterName,
               auditInfo = a.auditInfo.copy(destroyedDate = x.destroyedDate))
  )

  val createClusterAPIRespToListClusterResp = Lens[CreateRuntimeAPIResponse, ListRuntimeResponse](
    x =>
      ListRuntimeResponse(
        x.id,
        x.internalId,
        x.clusterName,
        x.googleProject,
        x.serviceAccountInfo,
        x.asyncRuntimeFields,
        x.auditInfo,
        x.runtimeConfig,
        x.clusterUrl,
        x.status,
        x.labels,
        x.jupyterExtensionUri,
        x.jupyterUserScriptUri,
        x.dataprocInstances,
        x.autopauseThreshold,
        x.defaultClientId,
        x.stopAfterCreation,
        x.welderEnabled
      )
  )(
    x =>
      c =>
        c.copy(
          id = x.id,
          internalId = x.internalId,
          clusterName = x.clusterName,
          googleProject = x.googleProject,
          serviceAccountInfo = x.serviceAccountInfo,
          asyncRuntimeFields = x.asyncRuntimeFields,
          auditInfo = x.auditInfo,
          clusterUrl = x.clusterUrl,
          status = x.status,
          labels = x.labels,
          jupyterExtensionUri = x.jupyterExtensionUri,
          jupyterUserScriptUri = x.jupyterUserScriptUri,
          dataprocInstances = x.dataprocInstances,
          autopauseThreshold = x.autopauseThreshold,
          defaultClientId = x.defaultClientId,
          stopAfterCreation = x.stopAfterCreation,
          welderEnabled = x.welderEnabled
        )
  )
}
