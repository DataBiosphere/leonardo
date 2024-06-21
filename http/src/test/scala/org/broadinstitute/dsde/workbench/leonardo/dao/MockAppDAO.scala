package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.IO
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.leonardo.AppName
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

class MockAppDAO(isUp: Boolean = true) extends AppDAO[IO] {
  override def isProxyAvailable(googleProject: GoogleProject,
                                appName: AppName,
                                serviceName: ServiceName,
                                traceId: TraceId
  ): IO[Boolean] =
    IO.pure(isUp)
}
object MockAppDAO extends MockAppDAO(isUp = true)
