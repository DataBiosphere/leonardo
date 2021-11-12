package org.broadinstitute.dsde.workbench.leonardo

import cats.syntax.all._
import org.broadinstitute.dsde.workbench.google2.{FirewallRuleName, NetworkName, RegionName, SubnetworkName, ZoneName}
import org.broadinstitute.dsp.{ChartName, ChartVersion}
import pureconfig.ConfigReader
import pureconfig.error.ExceptionThrown
import pureconfig.configurable._

import java.nio.file.{Path, Paths}

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
  implicit val diskDiszeConfigReader: ConfigReader[DiskSize] =
    ConfigReader.intConfigReader.map(s => DiskSize(s))
  implicit val blockSizeConfigReader: ConfigReader[BlockSize] =
    ConfigReader.intConfigReader.map(s => BlockSize(s))
  implicit val ipRangeReader: ConfigReader[IpRange] =
    ConfigReader.stringConfigReader.map(s => IpRange(s))
  implicit val regionNameReader: ConfigReader[RegionName] =
    ConfigReader.stringConfigReader.map(s => RegionName(s))
  implicit val networkLabeleReader: ConfigReader[NetworkLabel] =
    ConfigReader.stringConfigReader.map(s => NetworkLabel(s))
  implicit val subnetworkLabelReader: ConfigReader[SubnetworkLabel] =
    ConfigReader.stringConfigReader.map(s => SubnetworkLabel(s))
  implicit val firewallAllowHttpsLabelKeyReader: ConfigReader[FirewallAllowHttpsLabelKey] =
    ConfigReader.stringConfigReader.map(s => FirewallAllowHttpsLabelKey(s))
  implicit val firewallAllowInternalLabelKeyReader: ConfigReader[FirewallAllowInternalLabelKey] =
    ConfigReader.stringConfigReader.map(s => FirewallAllowInternalLabelKey(s))
  implicit val networkNameReader: ConfigReader[NetworkName] =
    ConfigReader.stringConfigReader.map(s => NetworkName(s))
  implicit val subnetworkNameReader: ConfigReader[SubnetworkName] =
    ConfigReader.stringConfigReader.map(s => SubnetworkName(s))
  implicit val networkTagReader: ConfigReader[NetworkTag] =
    ConfigReader.stringConfigReader.map(s => NetworkTag(s))
  implicit val firewallRuleNameReader: ConfigReader[FirewallRuleName] =
    ConfigReader.stringConfigReader.map(s => FirewallRuleName(s))
  implicit val sourceRangeMapReader: ConfigReader[Map[RegionName, IpRange]] =
    genericMapReader[RegionName, IpRange](s => RegionName(s).asRight)
  implicit val sourceRangeMap2Reader: ConfigReader[Map[RegionName, List[IpRange]]] =
    genericMapReader[RegionName, List[IpRange]](s => RegionName(s).asRight)
}
