package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.IO
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.model.TraceId

class MockAppDescriptorDAO extends AppDescriptorDAO[IO] {
  override def getDescriptor(path: String)(implicit ev: Ask[IO, TraceId]): IO[AppDescriptor] =
    IO(AppDescriptor("app", "test@app.org", "A test app", "0.1.0", Map.empty))
}

object MockAppDescriptorDAO extends MockAppDescriptorDAO
