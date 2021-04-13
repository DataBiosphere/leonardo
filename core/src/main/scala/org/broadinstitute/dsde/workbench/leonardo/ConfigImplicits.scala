package org.broadinstitute.dsde.workbench.leonardo

import pureconfig.ConfigReader
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.google2.ZoneName
import org.broadinstitute.dsp.{ChartName, ChartVersion}
import pureconfig.error.ExceptionThrown

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
}
