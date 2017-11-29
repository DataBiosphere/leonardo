package org.broadinstitute.dsde.workbench.leonardo.model

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.workbench.model.{WorkbenchEmail, WorkbenchUserId}

/**
  * Created by rtitle on 10/16/17.
  */
case class UserInfo(accessToken: OAuth2BearerToken, userId: WorkbenchUserId, userEmail: WorkbenchEmail, tokenExpiresIn: Long)