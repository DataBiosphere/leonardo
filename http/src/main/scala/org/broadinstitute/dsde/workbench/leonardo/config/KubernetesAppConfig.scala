package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{ServiceAccountName, ServiceName}
import org.broadinstitute.dsde.workbench.leonardo.config.CoaService.{Cbas, CbasUI, Cromwell}
import org.broadinstitute.dsde.workbench.leonardo.{
  Chart,
  DbPassword,
  GalaxyDrsUrl,
  GalaxyOrchUrl,
  KsaName,
  KubernetesService,
  NamespaceNameSuffix,
  ReleaseNameSuffix,
  ServiceConfig,
  ServiceId
}
import org.broadinstitute.dsp.{ChartName, ChartVersion}

import java.net.URL

sealed trait KubernetesAppConfig {
  def chartName: ChartName

  def chartVersion: ChartVersion

  def releaseNameSuffix: ReleaseNameSuffix

  def namespaceNameSuffix: NamespaceNameSuffix

  def serviceAccountName: ServiceAccountName

  def chart: Chart = Chart(chartName, chartVersion)

  def kubernetesServices: List[KubernetesService]

  def enabled: Boolean
}

final case class GalaxyAppConfig(releaseNameSuffix: ReleaseNameSuffix,
                                 chartName: ChartName,
                                 chartVersion: ChartVersion,
                                 namespaceNameSuffix: NamespaceNameSuffix,
                                 services: List[ServiceConfig],
                                 serviceAccountName: ServiceAccountName,
                                 uninstallKeepHistory: Boolean,
                                 postgresPassword: DbPassword,
                                 orchUrl: GalaxyOrchUrl,
                                 drsUrl: GalaxyDrsUrl,
                                 minMemoryGb: Int,
                                 minNumOfCpus: Int,
                                 enabled: Boolean
) extends KubernetesAppConfig {
  override lazy val kubernetesServices: List[KubernetesService] = services.map(s => KubernetesService(ServiceId(-1), s))
}

final case class CromwellAppConfig(chartName: ChartName,
                                   chartVersion: ChartVersion,
                                   namespaceNameSuffix: NamespaceNameSuffix,
                                   releaseNameSuffix: ReleaseNameSuffix,
                                   services: List[ServiceConfig],
                                   serviceAccountName: ServiceAccountName,
                                   dbPassword: DbPassword,
                                   enabled: Boolean
) extends KubernetesAppConfig {
  override lazy val kubernetesServices: List[KubernetesService] = services.map(s => KubernetesService(ServiceId(-1), s))
}

final case class CustomApplicationAllowListConfig(default: List[String], highSecurity: List[String])

final case class CustomAppConfig(chartName: ChartName,
                                 chartVersion: ChartVersion,
                                 releaseNameSuffix: ReleaseNameSuffix,
                                 namespaceNameSuffix: NamespaceNameSuffix,
                                 serviceAccountName: ServiceAccountName,
                                 customApplicationAllowList: CustomApplicationAllowListConfig,
                                 enabled: Boolean
) extends KubernetesAppConfig {
  // Not known at config. Generated at runtime.
  override lazy val kubernetesServices: List[KubernetesService] = List.empty
}

final case class CoaAppConfig(chartName: ChartName,
                              chartVersion: ChartVersion,
                              releaseNameSuffix: ReleaseNameSuffix,
                              namespaceNameSuffix: NamespaceNameSuffix,
                              ksaName: KsaName,
                              services: List[ServiceConfig],
                              instrumentationEnabled: Boolean,
                              enabled: Boolean,
                              dockstoreBaseUrl: URL
) extends KubernetesAppConfig {
  override lazy val kubernetesServices: List[KubernetesService] = services.map(s => KubernetesService(ServiceId(-1), s))
  override val serviceAccountName = ServiceAccountName(ksaName.value)

  def coaServices: Set[CoaService] = services
    .map(_.name)
    .collect {
      case ServiceName("cbas")     => Cbas
      case ServiceName("cbas-ui")  => CbasUI
      case ServiceName("cromwell") => Cromwell
    }
    .toSet
}

final case class WdsAppConfig(chartName: ChartName,
                              chartVersion: ChartVersion,
                              releaseNameSuffix: ReleaseNameSuffix,
                              namespaceNameSuffix: NamespaceNameSuffix,
                              ksaName: KsaName,
                              services: List[ServiceConfig],
                              instrumentationEnabled: Boolean,
                              enabled: Boolean,
                              databaseEnabled: Boolean
) extends KubernetesAppConfig {
  override lazy val kubernetesServices: List[KubernetesService] = services.map(s => KubernetesService(ServiceId(-1), s))
  override val serviceAccountName = ServiceAccountName(ksaName.value)
}

final case class HailBatchAppConfig(chartName: ChartName,
                                    chartVersion: ChartVersion,
                                    releaseNameSuffix: ReleaseNameSuffix,
                                    namespaceNameSuffix: NamespaceNameSuffix,
                                    ksaName: KsaName,
                                    services: List[ServiceConfig],
                                    enabled: Boolean
) extends KubernetesAppConfig {
  override lazy val kubernetesServices: List[KubernetesService] = services.map(s => KubernetesService(ServiceId(-1), s))
  override val serviceAccountName = ServiceAccountName(ksaName.value)
}

final case class RStudioAppConfig(chartName: ChartName,
                                  chartVersion: ChartVersion,
                                  namespaceNameSuffix: NamespaceNameSuffix,
                                  releaseNameSuffix: ReleaseNameSuffix,
                                  services: List[ServiceConfig],
                                  serviceAccountName: ServiceAccountName,
                                  enabled: Boolean
) extends KubernetesAppConfig {
  override lazy val kubernetesServices: List[KubernetesService] = services.map(s => KubernetesService(ServiceId(-1), s))
}

sealed trait CoaService

object CoaService {
  final case object Cbas extends CoaService

  final case object CbasUI extends CoaService

  final case object Cromwell extends CoaService

  final case object Tes extends CoaService
}
