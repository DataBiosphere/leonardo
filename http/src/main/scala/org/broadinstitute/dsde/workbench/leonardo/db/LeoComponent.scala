package org.broadinstitute.dsde.workbench.leonardo.db

import java.sql.Timestamp
import java.time.Instant

import slick.jdbc.JdbcProfile

import scala.concurrent.ExecutionContext

trait LeoComponent {
  val profile: JdbcProfile
  implicit val executionContext: ExecutionContext

  protected final val dummyDate: Instant = Instant.ofEpochMilli(1000)

  // We use dummyDate when we don't have a destroyedDate but we need to insert something
  // into the database for that column as it can't be nullable since the column is used
  // as part of a unique key (along with googleProject and clusterName)
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