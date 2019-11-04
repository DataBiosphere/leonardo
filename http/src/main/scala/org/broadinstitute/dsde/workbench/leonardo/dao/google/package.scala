package org.broadinstitute.dsde.workbench.leonardo.dao

import com.google.api.client.googleapis.json.GoogleJsonResponseException
import org.broadinstitute.dsde.workbench.google.GoogleUtilities.RetryPredicates._

package object google {

  final val retryPredicates = List(
    when5xx _,
    whenUsageLimited _,
    whenGlobalUsageLimited _,
    when404 _,
    whenInvalidValueOnBucketCreation _,
    whenNonHttpIOException _
  )

  final val when400: Throwable => Boolean = {
    case t: GoogleJsonResponseException => t.getStatusCode == 400
    case _                              => false
  }

  final val when401: Throwable => Boolean = {
    case t: GoogleJsonResponseException => t.getStatusCode == 401
    case _                              => false
  }

  final val whenGoogleZoneCapacityIssue: Throwable => Boolean = {
    case t: GoogleJsonResponseException =>
      t.getStatusCode == 429
    case _ => false
  }

}
