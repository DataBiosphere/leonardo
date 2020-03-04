package org.broadinstitute.dsde.workbench.leonardo
package dao

import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.util.health.StatusCheckResponse
import org.http4s.EntityDecoder
import org.http4s.headers.Authorization

trait SamDAO[F[_]] {
  def getStatus(implicit ev: ApplicativeAsk[F, TraceId]): F[StatusCheckResponse]

  def hasResourcePermission(resourceId: String,
                            action: String,
                            resourceTypeName: ResourceTypeName,
                            authHeader: Authorization)(implicit ev: ApplicativeAsk[F, TraceId]): F[Boolean]

  def getResourcePolicies[A](authHeader: Authorization, resourseTypeName: ResourceTypeName)(
    implicit decoder: EntityDecoder[F, List[A]],
    ev: ApplicativeAsk[F, TraceId]
  ): F[List[A]]

  def createClusterResource(internalId: RuntimeInternalId,
                            creatorEmail: WorkbenchEmail,
                            googleProject: GoogleProject,
                            runtimeName: RuntimeName)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit]

  def deleteClusterResource(internalId: RuntimeInternalId,
                            userEmail: WorkbenchEmail,
                            creatorEmail: WorkbenchEmail,
                            googleProject: GoogleProject,
                            runtimeName: RuntimeName)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit]

  def getPetServiceAccount(authorization: Authorization, googleProject: GoogleProject)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[Option[WorkbenchEmail]]

  def getUserProxy(userEmail: WorkbenchEmail)(implicit ev: ApplicativeAsk[F, TraceId]): F[Option[WorkbenchEmail]]

  def getCachedPetAccessToken(userEmail: WorkbenchEmail, googleProject: GoogleProject)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[Option[String]]
}
