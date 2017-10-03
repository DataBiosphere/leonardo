package org.broadinstitute.dsde.firecloud.util

import java.util.UUID

import scala.concurrent.duration.FiniteDuration
import scala.util.Random

/**
  */
object Util {

  def appendUnderscore(string: String): String = {
    string match {
      case "" => ""
      case s => s + "_"
    }
  }

  def makeRandomId(length: Int = 7): String = {
    Random.alphanumeric.take(length).mkString
  }

  def makeUuid: String = {
    UUID.randomUUID().toString
  }
}
