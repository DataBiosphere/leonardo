package org.broadinstitute.dsde.workbench.leonardo

import org.scalatest.Tag

object LeonardoTags {
  // intent: run tests of Slick queries separately
  object SlickPlainQueryTest extends Tag("SlickPlainQueryTest")

  // intent: do not run in given execution context
  object ExcludeFromJenkins extends Tag("ExcludeFromJenkins")
}
