package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.leonardo.ContainerImage

final case class ImageConfig(
  welderImage: ContainerImage,
  jupyterImage: ContainerImage,
  legacyJupyterImage: ContainerImage,
  proxyImage: ContainerImage,
  jupyterContainerName: String,
  rstudioContainerName: String,
  welderContainerName: String,
  proxyContainerName: String,
  jupyterImageRegex: String,
  rstudioImageRegex: String,
  broadDockerhubImageRegex: String
)
