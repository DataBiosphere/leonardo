package org.broadinstitute.dsde.workbench.leonardo.db

import java.sql.Timestamp
import java.time.Instant

import slick.jdbc.JdbcProfile

import scala.concurrent.ExecutionContext

trait LeoComponent {
  val profile: JdbcProfile
  implicit val executionContext: ExecutionContext

  protected final val dummyDate: Instant = Instant.ofEpochMilli(1000)

  protected def unmarshalDestroyedDate(destroyedDate: Timestamp): Option[Instant] = {
    if(destroyedDate.toInstant != dummyDate)
      Some(destroyedDate.toInstant)
    else
      None
  }

  protected def marshalDestroyedDate(destroyedDate: Option[Instant]): Timestamp = {
    Timestamp.from(destroyedDate.getOrElse(dummyDate))
  }
}