package org.broadinstitute.dsde.workbench.leonardo
package config
import java.nio.file.Path

case class SecurityFilesConfig(proxyServerCrt: Path, proxyServerKey: Path, proxyRootCaPem: Path, proxyRootCaKey: Path)
