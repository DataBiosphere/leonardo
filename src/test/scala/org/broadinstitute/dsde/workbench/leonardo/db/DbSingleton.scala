package org.broadinstitute.dsde.workbench.leonardo.db

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.workbench.leonardo.TestExecutionContext

// initialize database tables and connection pool only once
object DbSingleton {
  import TestExecutionContext.testExecutionContext

  val actorSystem = ActorSystem("testActorSystem")
  val ref: DbReference = DbReference.init(ConfigFactory.load(), actorSystem)
}
