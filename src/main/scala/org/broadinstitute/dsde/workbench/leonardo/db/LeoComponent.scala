package org.broadinstitute.dsde.workbench.leonardo.db

import slick.jdbc.JdbcProfile

import scala.concurrent.ExecutionContext

trait LeoComponent {
  val profile: JdbcProfile
  implicit val executionContext: ExecutionContext
}