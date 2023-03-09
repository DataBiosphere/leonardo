package org.broadinstitute.dsde.workbench.leonardo.http
package api

import akka.http.scaladsl.server.{Directive, Directive1}
import akka.http.scaladsl.server.Directives.{require, reject}
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.leonardo.dao.{AuthProviderException, SamDAO}
import org.broadinstitute.dsde.workbench.leonardo.model.AuthenticationError

object StandardEnabledUserDirectives extends EnabledUserDirectives {

  /** Directive requiring the given user is enabled in Sam. */
  override def requireEnabledUser(userInfo: UserInfo): Directive = require(isUserEnabled(userInfo))

  private[leonardo] def isUserEnabled(userInfo: UserInfo)(implicit
    ev: Ask[IO, TraceId]
  ): IO[Boolean] =
    for {
      ctx <- ev.ask[TraceId]
      samUserInfoOpt <- samDAO.getSamUserInfo(userInfo.accessToken.token)
      samUserInfo <- IO.fromOption(samUserInfo)(AuthenticationError(Some(userInfo.userEmail)))
      _ <-
        if (samUserInfo.enabled) IO.unit
        else
          IO.raiseError(
            AuthProviderException(
              ctx.traceId,
              s"[] User ${samUserInfo.userEmail.value} is disabled.",
              StatusCodes.Forbidden
            )
          )
    } yield IO.pure(true)
}
