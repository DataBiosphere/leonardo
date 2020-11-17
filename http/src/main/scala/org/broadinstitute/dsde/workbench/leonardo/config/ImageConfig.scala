package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.leonardo.ContainerImage

final case class ImageConfig(
  welderGcrImage: ContainerImage,
  welderDockerHubImage: ContainerImage,
  welderHash: String,
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
