package org.broadinstitute.dsde.workbench.leonardo
package config
import org.broadinstitute.dsde.workbench.leonardo.monitor.PollMonitorConfig
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

case class GoogleGroupsConfig(googleAdminEmail: WorkbenchEmail,
                              dataprocImageProjectGroupName: String,
                              dataprocImageProjectGroupEmail: WorkbenchEmail,
                              waitForMemberAddedPollConfig: PollMonitorConfig
)
