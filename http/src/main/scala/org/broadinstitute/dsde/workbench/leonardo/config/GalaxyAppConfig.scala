package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.NamespaceName
import org.broadinstitute.dsde.workbench.leonardo.{ChartName, ReleaseName, ServiceConfig}

case class GalaxyAppConfig(releaseName: ReleaseName,
                           chart: ChartName,
                           namespaceNameSuffix: NamespaceName,
                           services: List[ServiceConfig])
