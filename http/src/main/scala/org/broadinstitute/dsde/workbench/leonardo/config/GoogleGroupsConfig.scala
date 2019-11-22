package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

case class GoogleGroupsConfig(googleAdminEmail: WorkbenchEmail,
                              dataprocImageProjectGroupName: String,
                              dataprocImageProjectGroupEmail: WorkbenchEmail)
