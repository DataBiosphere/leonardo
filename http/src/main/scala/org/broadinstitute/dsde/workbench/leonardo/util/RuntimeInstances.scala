package org.broadinstitute.dsde.workbench.leonardo.util

import org.broadinstitute.dsde.workbench.leonardo.CloudService
import org.broadinstitute.dsde.workbench.leonardo.CloudService.{AzureVm, Dataproc, GCE}

class RuntimeInstances[F[_]](dataprocInterp: RuntimeAlgebra[F], gceInterp: RuntimeAlgebra[F]) {

  def interpreter(cloudService: CloudService): RuntimeAlgebra[F] = cloudService match {
    case Dataproc => dataprocInterp
    case GCE      => gceInterp
    case AzureVm  => throw new Exception("Azure is not handled with existing RuntimeAlgebra structure")
  }
}

//
// Adds syntax:
//  ```
//  import org.broadinstitute.dsde.workbench.leonardo.http._
//  cloudService.interpreter.deleteRuntime(params)
//  ```
// Requires an implicit RuntimeInstances in scope.
//
final class CloudServiceOps(cloudService: CloudService) {
  def interpreter[F[_]](implicit runtimeInstances: RuntimeInstances[F]): RuntimeAlgebra[F] =
    runtimeInstances.interpreter(cloudService)
}
