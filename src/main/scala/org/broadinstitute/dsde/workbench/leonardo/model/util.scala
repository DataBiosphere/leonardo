package org.broadinstitute.dsde.workbench.leonardo

import scala.concurrent.duration._

/**
 * Created by dvoet on 2/24/17.
 */
package object util {
  def toScalaDuration(javaDuration: java.time.Duration) = Duration.fromNanos(javaDuration.toNanos)

  def addJitter(baseTime: FiniteDuration, maxJitterToAdd: FiniteDuration): FiniteDuration = {
    baseTime + ((scala.util.Random.nextFloat * maxJitterToAdd.toNanos) nanoseconds)
  }

  def addJitter(baseTime: FiniteDuration): FiniteDuration = {
    if(baseTime < (1 second)) {
      addJitter(baseTime, 100 milliseconds)
    } else if (baseTime < (10 seconds)) {
      addJitter(baseTime, 500 milliseconds)
    } else {
      addJitter(baseTime, 1 second)
    }
  }

  /**
    * Converts a [[java.util.Map.Entry]] to a [[scala.Tuple2]]
    */
  implicit class JavaEntrySupport[A, B](entry: java.util.Map.Entry[A, B]) {
    def toTuple: (A, B) = (entry.getKey, entry.getValue)
  }
}
