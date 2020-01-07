package org.broadinstitute.dsde.workbench.leonardo.config

import com.typesafe.config.{Config => TypeSafeConfig}
import net.ceedubs.ficus.readers.ValueReader

final case class MemoryConfig(bytes: Long) {
  def kb: Double = bytes / 1024d
  def mb: Double = bytes / 1048576d
  def gb: Double = bytes / 1073741824d
}

object MemoryConfig {
  implicit val memoryConfigReader: ValueReader[MemoryConfig] = (config: TypeSafeConfig, path: String) =>
    MemoryConfig(config.getBytes(path))
}
