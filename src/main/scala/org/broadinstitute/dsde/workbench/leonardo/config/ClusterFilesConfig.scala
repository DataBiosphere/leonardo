package org.broadinstitute.dsde.workbench.leonardo.config

import java.io.File

case class ClusterFilesConfig(leonardoServicePem: File,
                              jupyterServerCrt: File,
                              jupyterServerKey: File,
                              jupyterRootCaPem: File,
                              jupyterRootCaKey: File)
