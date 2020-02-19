package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.leonardo.http.api.{CreateRuntime2Request, RuntimeServiceContext}
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

trait RuntimeService[F[_]] {
  def createRuntime(userInfo: UserInfo,
                    googleProject: GoogleProject,
                    runtimeName: RuntimeName, //TODO: rename this to RuntimeName once Rob's PR is in
                    req: CreateRuntime2Request)(implicit as: ApplicativeAsk[F, RuntimeServiceContext]): F[Unit]
}
