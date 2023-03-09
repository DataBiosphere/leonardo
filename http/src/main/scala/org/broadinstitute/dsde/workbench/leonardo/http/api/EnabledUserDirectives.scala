package org.broadinstitute.dsde.workbench.leonardo.http
package api

import akka.http.scaladsl.server.Directive
import org.broadinstitute.dsde.workbench.model.UserInfo

trait EnabledUserDirectives {
  def requireEnabledUser: Directive[UserInfo]
}
