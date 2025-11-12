package org.broadinstitute.dsde.workbench.leonardo

import pureconfig.ConfigReader
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.azure.{ClientId, ClientSecret, ManagedAppTenantId}
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.google2.ZoneName
import org.broadinstitute.dsp.{ChartName, ChartVersion}
import pureconfig.error.ExceptionThrown

import java.nio.file.{Path, Paths}
import org.http4s.Uri

object ConfigImplicits {
  implicit val pathConfigReader: ConfigReader[Path] =
    ConfigReader.stringConfigReader.emap(s => Either.catchNonFatal(Paths.get(s)).leftMap(err => ExceptionThrown(err)))
  implicit val chartNameConfigReader: ConfigReader[ChartName] =
    ConfigReader.stringConfigReader.map(s => ChartName(s))
  implicit val chartVersionConfigReader: ConfigReader[ChartVersion] =
    ConfigReader.stringConfigReader.map(s => ChartVersion(s))
  implicit val zoneNameConfigReader: ConfigReader[ZoneName] =
    ConfigReader.stringConfigReader.map(s => ZoneName(s))
  implicit val diskTypeConfigReader: ConfigReader[DiskType] =
    ConfigReader.stringConfigReader.map(s => DiskType.stringToObject(s))
  implicit val diskSizeConfigReader: ConfigReader[DiskSize] =
    ConfigReader.intConfigReader.map(s => DiskSize(s))
  implicit val blockSizeConfigReader: ConfigReader[BlockSize] =
    ConfigReader.intConfigReader.map(s => BlockSize(s))

  implicit val cidrIpConfigReader: ConfigReader[CidrIP] =
    ConfigReader.stringConfigReader.map(s => CidrIP(s))

  implicit val appTypeConfigReader: ConfigReader[AppType] =
    ConfigReader.stringConfigReader.emap(s =>
      Either.fromOption(AppType.stringToObject.get(s), ExceptionThrown.apply(new Exception("invalid appType")))
    )

  implicit val clientIdConfigReader: ConfigReader[ClientId] =
    ConfigReader.stringConfigReader.map(s => ClientId(s))
  implicit val clientSecretConfigReader: ConfigReader[ClientSecret] =
    ConfigReader.stringConfigReader.map(s => ClientSecret(s))
  implicit val oauth2ClientIdConfigReader: ConfigReader[org.broadinstitute.dsde.workbench.oauth2.ClientId] =
    ConfigReader.stringConfigReader.map(s => org.broadinstitute.dsde.workbench.oauth2.ClientId(s))
  implicit val tentantIdConfigReader: ConfigReader[ManagedAppTenantId] =
    ConfigReader.stringConfigReader.map(s => ManagedAppTenantId(s))
  implicit val uriConfigReader: ConfigReader[Uri] =
    ConfigReader.stringConfigReader.emap(s =>
      Either.catchNonFatal(Uri.unsafeFromString(s)).leftMap(ExceptionThrown.apply)
    )
  implicit val serviceNameReader: ConfigReader[ServiceName] =
    ConfigReader.stringConfigReader.map(s => ServiceName(s))
}
