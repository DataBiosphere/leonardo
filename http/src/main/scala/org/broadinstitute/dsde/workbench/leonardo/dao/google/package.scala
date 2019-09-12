package org.broadinstitute.dsde.workbench.leonardo.dao

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

}
