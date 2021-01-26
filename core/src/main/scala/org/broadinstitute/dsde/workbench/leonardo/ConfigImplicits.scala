package org.broadinstitute.dsde.workbench.leonardo

import com.google.pubsub.v1.{ProjectSubscriptionName, ProjectTopicName, TopicName}
import org.broadinstitute.dsde.workbench.google2.{
  FirewallRuleName,
  Location,
  MaxRetries,
  NetworkName,
  RegionName,
  SubnetworkName
}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import pureconfig.ConfigReader
import pureconfig.error.ExceptionThrown
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{
  NamespaceName,
  SecretKey,
  SecretName,
  ServiceAccountName,
  ServiceName
}
import org.broadinstitute.dsde.workbench.leonardo.algebra.{NetworkLabel, SubnetworkLabel}
import org.broadinstitute.dsp.{ChartName, ChartVersion, Release}
import org.http4s.Uri

import java.nio.file.{Path, Paths}

object ConfigImplicits {
  implicit val pathConfigReader: ConfigReader[Path] =
    ConfigReader.stringConfigReader.emap(s => Either.catchNonFatal(Paths.get(s)).leftMap(err => ExceptionThrown(err)))

  implicit val maxRetryReader: ConfigReader[MaxRetries] =
    ConfigReader.intConfigReader.map(MaxRetries)

  implicit val uriReader: ConfigReader[Uri] =
    ConfigReader.stringConfigReader.emap(s => Uri.fromString(s).leftMap(err => ExceptionThrown(err)))

  implicit val diskSizeReader: ConfigReader[DiskSize] =
    ConfigReader.intConfigReader.map(DiskSize)

  implicit val blockSizeReader: ConfigReader[BlockSize] =
    ConfigReader.intConfigReader.map(BlockSize)

  implicit val locationReader: ConfigReader[Location] =
    ConfigReader.stringConfigReader.map(Location)

  implicit val regionNameReader: ConfigReader[RegionName] =
    ConfigReader.stringConfigReader.map(s => RegionName(s))

  implicit val cidrIPReader: ConfigReader[CidrIP] =
    ConfigReader.stringConfigReader.map(s => CidrIP(s))

  implicit val kubernetesClusterVersionReader: ConfigReader[KubernetesClusterVersion] =
    ConfigReader.stringConfigReader.map(s => KubernetesClusterVersion(s))

  implicit val subnetworkNameReader: ConfigReader[SubnetworkName] =
    ConfigReader.stringConfigReader.map(s => SubnetworkName(s))

  implicit val subnetworkLabelReader: ConfigReader[SubnetworkLabel] =
    ConfigReader.stringConfigReader.map(s => SubnetworkLabel(s))

  implicit val networkLabelReader: ConfigReader[NetworkLabel] =
    ConfigReader.stringConfigReader.map(s => NetworkLabel(s))

  implicit val networkTagReader: ConfigReader[NetworkTag] =
    ConfigReader.stringConfigReader.map(s => NetworkTag(s))

  implicit val chartNameReader: ConfigReader[ChartName] =
    ConfigReader.stringConfigReader.map(s => ChartName(s))

  implicit val chartVersionReader: ConfigReader[ChartVersion] =
    ConfigReader.stringConfigReader.map(s => ChartVersion(s))

  implicit val serviceAccountNameReader: ConfigReader[ServiceAccountName] =
    ConfigReader.stringConfigReader.map(s => ServiceAccountName(s))

  implicit val serviceNameReader: ConfigReader[ServiceName] =
    ConfigReader.stringConfigReader.map(s => ServiceName(s))

  implicit val kubernetesServiceKindNameReader: ConfigReader[KubernetesServiceKindName] =
    ConfigReader.stringConfigReader.map(s => KubernetesServiceKindName(s))

  implicit val ipRangeReader: ConfigReader[IpRange] =
    ConfigReader.stringConfigReader.map(s => IpRange(s))

  implicit val firewallRuleNameReader: ConfigReader[FirewallRuleName] =
    ConfigReader.stringConfigReader.map(s => FirewallRuleName(s))

  implicit val networkNameReader: ConfigReader[NetworkName] =
    ConfigReader.stringConfigReader.map(s => NetworkName(s))

  implicit val namespaceNameReader: ConfigReader[NamespaceName] =
    ConfigReader.stringConfigReader.map(s => NamespaceName(s))

  implicit val valueConfigReader: ConfigReader[ValueConfig] =
    ConfigReader.stringConfigReader.map(s => ValueConfig(s))

  implicit val secretNameReader: ConfigReader[SecretName] =
    ConfigReader.stringConfigReader.map(s => SecretName(s))

  implicit val secretKeyReader: ConfigReader[SecretKey] =
    ConfigReader.stringConfigReader.map(s => SecretKey(s))

  implicit val releaseReader: ConfigReader[Release] =
    ConfigReader.stringConfigReader.map(s => Release(s))

  implicit val leonardoBaseUrlReader: ConfigReader[LeonardoBaseUrl] =
    ConfigReader.stringConfigReader.map(s => LeonardoBaseUrl(s))

  implicit val projectTopicNameReader: ConfigReader[ProjectTopicName] =
    ConfigReader.forProduct2("project", "topic")((project, topic) => ProjectTopicName.of(project, topic))

  implicit val topicNameReader: ConfigReader[TopicName] =
    ConfigReader.forProduct2("project", "topic")((project, topic) => ProjectTopicName.of(project, topic))

  implicit val projectSubscriptionNameReader: ConfigReader[ProjectSubscriptionName] =
    ConfigReader.forProduct2("project", "subscription-name")((project, name) =>
      ProjectSubscriptionName.of(project, name)
    )

  implicit val googleProjectReader: ConfigReader[GoogleProject] =
    ConfigReader.stringConfigReader.map(GoogleProject)

}
