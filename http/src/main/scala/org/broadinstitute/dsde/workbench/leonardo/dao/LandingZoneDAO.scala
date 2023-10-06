package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, BillingProfileId, LandingZoneResources}
import org.http4s.headers.Authorization

trait LandingZoneDAO[F[_]] {

  /**
   * Queries the Landing Zone Service for the list of LZ resources for a given workspace.
   * Cached for performance.
   */
  def getLandingZoneResources(billingProfileId: BillingProfileId, userToken: Authorization)(implicit
    ev: Ask[F, AppContext]
  ): F[LandingZoneResources]
}
