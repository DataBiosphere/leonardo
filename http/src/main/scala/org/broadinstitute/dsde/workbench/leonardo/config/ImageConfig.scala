package org.broadinstitute.dsde.workbench.leonardo.config

final case class ImageConfig(
  welderDockerImage: String,
  jupyterImage: String,
  jupyterImageRegex: String,
  rstudioImageRegex: String,
  dockerhubImageRegex: String
)
