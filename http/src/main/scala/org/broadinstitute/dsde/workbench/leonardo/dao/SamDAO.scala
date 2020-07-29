package org.broadinstitute.dsde.workbench.leonardo
package dao

import cats.mtl.ApplicativeAsk
import io.circe.{Decoder, Encoder}
import org.broadinstitute.dsde.workbench.leonardo.model.{SamResource, SamResourceAction}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.util.health.StatusCheckResponse
import org.http4s.headers.Authorization

trait SamDAO[F[_]] {
  def getStatus(implicit ev: ApplicativeAsk[F, TraceId]): F[StatusCheckResponse]

  def hasResourcePermission[R, A](resource: R, action: A, authHeader: Authorization)(
    implicit sr: SamResourceAction[R, A],
    ev: ApplicativeAsk[F, TraceId]
  ): F[Boolean] =
    hasResourcePermissionUnchecked(sr.resourceType,
                                   sr.resourceIdAsString(resource),
                                   sr.actionAsString(action),
                                   authHeader)

  // This exists because of guava cache
  private[leonardo] def hasResourcePermissionUnchecked(resourceType: SamResourceType,
                                                       resource: String,
                                                       action: String,
                                                       authHeader: Authorization)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[Boolean]

  def getResourcePolicies[R](authHeader: Authorization)(
    implicit sr: SamResource[R],
    decoder: Decoder[R],
    ev: ApplicativeAsk[F, TraceId]
  ): F[List[(R, SamPolicyName)]]

  def createResource[R](resource: R, creatorEmail: WorkbenchEmail, googleProject: GoogleProject)(
    implicit sr: SamResource[R],
    ev: ApplicativeAsk[F, TraceId]
  ): F[Unit]

  def createResourceWithManagerPolicy[R](resource: R, creatorEmail: WorkbenchEmail, googleProject: GoogleProject)(
    implicit sr: SamResource[R],
    encoder: Encoder[R],
    ev: ApplicativeAsk[F, TraceId]
  ): F[Unit]

  def deleteResource[R](resource: R, creatorEmail: WorkbenchEmail, googleProject: GoogleProject)(
    implicit sr: SamResource[R],
    ev: ApplicativeAsk[F, TraceId]
  ): F[Unit]

  def getListOfResourcePermissions[R, A](resource: R, authHeader: Authorization)(
    implicit sr: SamResourceAction[R, A],
    ev: ApplicativeAsk[F, TraceId]
  ): F[List[sr.ActionCategory]]

  def getPetServiceAccount(authorization: Authorization, googleProject: GoogleProject)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[Option[WorkbenchEmail]]

  def getUserProxy(userEmail: WorkbenchEmail)(implicit ev: ApplicativeAsk[F, TraceId]): F[Option[WorkbenchEmail]]

  def getCachedPetAccessToken(userEmail: WorkbenchEmail, googleProject: GoogleProject)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[Option[String]]
}
