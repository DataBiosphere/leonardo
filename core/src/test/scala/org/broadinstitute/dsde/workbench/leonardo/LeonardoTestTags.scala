package org.broadinstitute.dsde.workbench.leonardo

import org.scalatest.Tag

object LeonardoTestTags {
  // intent: run tests of Slick queries separately
  object SlickPlainQueryTest extends Tag("SlickPlainQueryTest")

  // intent: do not run in given execution context
  // this is also synonmous with 'azure test'
  object ExcludeFromJenkins extends Tag("ExcludeFromJenkins")

  // intent: run in cron scheduled tests only, not at every PR commit
  // example use case is for GPU testing that would otherwise run into resource allocation problems
  object ScheduledTest extends Tag("ScheduledTest")
}
