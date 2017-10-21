package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.leonardo.model.GoogleProject

/**
  * Pet Service Account configuration.
  *
  * @param googleProject The project in which to create pet service accounts.
  */
case class PetServiceAccountConfig(googleProject: GoogleProject)

