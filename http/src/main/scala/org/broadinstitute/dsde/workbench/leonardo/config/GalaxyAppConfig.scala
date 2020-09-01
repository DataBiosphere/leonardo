package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.NamespaceName
import org.broadinstitute.dsde.workbench.leonardo.{ChartName, KubernetesServiceAccount, ReleaseName, ServiceConfig}

case class GalaxyAppConfig(releaseNameSuffix: ReleaseName,
                           chart: ChartName,
                           namespaceNameSuffix: NamespaceName,
                           services: List[ServiceConfig],
                           serviceAccountSuffix: KubernetesServiceAccount,
                           uninstallKeepHistory: Boolean)
