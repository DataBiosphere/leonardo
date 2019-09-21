package org.broadinstitute.dsde.workbench.leonardo.dao

import akka.http.scaladsl.model.StatusCodes
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import org.broadinstitute.dsde.workbench.google.GoogleUtilities.RetryPredicates._

import scala.collection.JavaConverters._

package object google {

  final val retryPredicates = List(
    when5xx _,
    whenUsageLimited _,
    whenGlobalUsageLimited _,
    when404 _,
    whenInvalidValueOnBucketCreation _,
    whenNonHttpIOException _
  )

  final val when401: Throwable => Boolean = {
    case g: GoogleJsonResponseException if g.getStatusCode == StatusCodes.Unauthorized.intValue => true
    case _ => false
  }

  final val whenGoogleZoneCapacityIssue: Throwable => Boolean = {
    case t: GoogleJsonResponseException => t.getStatusCode == 429 && t.getDetails.getErrors.asScala.head.getReason.equalsIgnoreCase("rateLimitExceeded")
    case _ => false
  }

}
