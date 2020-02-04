package org.broadinstitute.dsde.workbench.leonardo.config

final case class ImageConfig(
  welderImage: String,
  jupyterImage: String,
  legacyJupyterImage: String,
  proxyImage: String,
  jupyterContainerName: String,
  rstudioContainerName: String,
  welderContainerName: String,
  proxyContainerName: String,
  jupyterImageRegex: String,
  rstudioImageRegex: String,
  broadDockerhubImageRegex: String
)
