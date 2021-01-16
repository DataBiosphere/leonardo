package org.broadinstitute.dsde.workbench.leonardo.http
package api

import akka.http.scaladsl.server.Directive1
import org.broadinstitute.dsde.workbench.model.UserInfo

/**
 * Created by rtitle on 10/16/17.
 */
trait UserInfoDirectives {
  def requireUserInfo: Directive1[UserInfo]
}
