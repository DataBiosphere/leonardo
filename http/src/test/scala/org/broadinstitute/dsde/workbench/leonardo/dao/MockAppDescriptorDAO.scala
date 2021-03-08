package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.IO
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.AppContext
import org.http4s.Uri

class MockAppDescriptorDAO extends AppDescriptorDAO[IO] {
  override def getDescriptor(path: Uri)(implicit ev: Ask[IO, AppContext]): IO[AppDescriptor] =
    IO(AppDescriptor("app", "test@app.org", "A test app", "0.1.0", Map.empty))
}

object MockAppDescriptorDAO extends MockAppDescriptorDAO
