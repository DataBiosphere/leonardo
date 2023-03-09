package org.broadinstitute.dsde.workbench.leonardo.http
package api

import akka.http.scaladsl.server.Directive
// TODO akka require isn't in this package - where is it? see https://doc.akka.io/docs/akka-http/current/routing-dsl/directives/custom-directives.html#require-trequire
import akka.http.scaladsl.server.Directives.require
import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo}
import org.broadinstitute.dsde.workbench.leonardo.dao.{AuthProviderException, SamDAO}
import org.broadinstitute.dsde.workbench.leonardo.model.AuthenticationError
import cats.effect.IO
import cats.mtl.Ask

object StandardEnabledUserDirectives extends EnabledUserDirectives {

  /** Directive requiring the given user is enabled in Sam. */
  // TODO how do I properly define this to accept userInfo as a param, and produce a Directive which itself accepts no params?
  // TODO do I need an Ask here to get TraceId?
  override def requireEnabledUser(userInfo: UserInfo): Directive = require(isUserEnabled(userInfo))

  // TODO how do I correctly preserve and promote errors that occur in the various stages of this req. an Either?
  private[leonardo] def isUserEnabled(userInfo: UserInfo)(implicit
    ev: Ask[IO, TraceId]
  ): IO[Boolean] =
    for {
      ctx <- ev.ask[TraceId]
      // TODO how to get access to the samDAO instance from this scope (StandardEnabledUserDirectives passed whole into the *Routes)
      samUserInfoOpt <- samDAO.getSamUserInfo(userInfo.accessToken.token)
      samUserInfo <- IO.fromOption(samUserInfoOpt)(AuthenticationError(Some(userInfo.userEmail)))
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
