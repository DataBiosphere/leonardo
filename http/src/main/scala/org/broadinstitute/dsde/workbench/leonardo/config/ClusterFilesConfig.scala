package org.broadinstitute.dsde.workbench.leonardo.config

import java.nio.file.Path

case class ClusterFilesConfig(proxyServerCrt: Path,
                              proxyServerKey: Path,
                              proxyRootCaPem: Path,
                              proxyRootCaKey: Path,
                              rstudioLicenseFile: Path)
