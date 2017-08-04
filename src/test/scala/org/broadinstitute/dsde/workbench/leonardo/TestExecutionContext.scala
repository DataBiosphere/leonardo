package org.broadinstitute.dsde.workbench.leonardo

import java.util.concurrent.{ExecutorService, Executors}

import scala.concurrent.ExecutionContextExecutor

/**
  * Created by dvoet on 10/9/15.
  */
object TestExecutionContext {
  implicit val testExecutionContext = new TestExecutionContext()
}

class TestExecutionContext() extends ExecutionContextExecutor {
  val pool: ExecutorService = Executors.newCachedThreadPool()
  override def execute(runnable: Runnable): Unit = {
    pool.execute(runnable)
  }

  override def reportFailure(cause: Throwable): Unit = {
    cause.printStackTrace()
  }
}
