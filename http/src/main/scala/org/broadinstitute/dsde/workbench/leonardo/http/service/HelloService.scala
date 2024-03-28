package org.broadinstitute.dsde.workbench.leonardo.http.service;

import cats.effect.IO

class HelloService {
  def getResponse: IO[String] = IO.pure("Hello, World!")
}
