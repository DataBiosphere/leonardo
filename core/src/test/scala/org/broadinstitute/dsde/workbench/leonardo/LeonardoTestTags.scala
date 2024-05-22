package org.broadinstitute.dsde.workbench.leonardo

import org.scalatest.Tag

object LeonardoTestTags {
  // intent: run tests of Slick queries separately
  object SlickPlainQueryTest extends Tag("SlickPlainQueryTest")

  // intent: do not run in given execution context
  // this is also synonmous with 'azure test'
  object ExcludeFromJenkins extends Tag("ExcludeFromJenkins")

  // intent: do not run on every PR commit
  // e.g. GPU tests should not run at every PR commit to avoid resource allocation problems
  object ExcludeFromPRCommit extends Tag("ExcludeFromPRCommit")
}
