package org.broadinstitute.dsde.workbench.leonardo.monitor

import akka.actor.{Actor, Props}

object NoopActor {
  def props: Props = Props(new NoopActor)
}

/**
  * Created by rtitle on 9/12/17.
  */
class NoopActor extends Actor {
  override def receive: Receive = {
    case msg => // noop
  }
}
