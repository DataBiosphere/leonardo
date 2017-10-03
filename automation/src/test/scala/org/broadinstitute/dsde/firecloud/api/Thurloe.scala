package org.broadinstitute.dsde.firecloud.api

import akka.http.scaladsl.model.headers.{ModeledCustomHeader, ModeledCustomHeaderCompanion}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.firecloud.config.FireCloudConfig
import org.broadinstitute.dsde.workbench.api.WorkbenchClient
import org.broadinstitute.dsde.workbench.config.AuthToken

import scala.util.Try

/**
  */
object Thurloe extends WorkbenchClient with LazyLogging {

  private val thurloeHeaders = List(FireCloudIdHeader(FireCloudConfig.FireCloud.fireCloudId))
  private val url = FireCloudConfig.FireCloud.thurloeApiUrl

  object FireCloudIdHeader extends ModeledCustomHeaderCompanion[FireCloudIdHeader] {
    override def name: String = "X-FireCloud-Id"
    override def parse(value: String): Try[FireCloudIdHeader] = Try(FireCloudIdHeader(value))
  }
  case class FireCloudIdHeader(id: String) extends ModeledCustomHeader[FireCloudIdHeader] {
    override def companion: ModeledCustomHeaderCompanion[FireCloudIdHeader] = FireCloudIdHeader
    override def renderInRequests(): Boolean = true
    override def renderInResponses(): Boolean = false
    override def value(): String = id
  }

  object keyValuePairs {

    def delete(subjectId: String, key: String)(implicit token: AuthToken): Unit = {
      logger.info(s"Deleting $key for $subjectId")
      deleteRequest(url + s"api/thurloe/$subjectId/$key", thurloeHeaders)
    }

    def deleteAll(subjectId: String)(implicit token: AuthToken): Unit = {
      logger.info(s"Deleting all key/value pairs from for $subjectId")
      getAll(subjectId).foreach[Unit] {
        case (key, _) =>
          delete(subjectId, key)
      }
    }

    def getAll(subjectId: String)(implicit token: AuthToken): Map[String, String] = {

      def extractTupleFromKeyValueMap(m: Map[String, String]): Option[(String, String)] =
        (m.get("key"), m.get("value")) match {
          case (Some(k), Some(v)) => Some(k -> v)
          case _ => None
        }

      val response: String = parseResponse(getRequest(url + s"api/thurloe/$subjectId", thurloeHeaders))

      /*
       * "keyValuePairs" contains a list of maps, each with 2 entries: one for
       * the key and one for the value. To make it easier to work with, we need
       * to turn this:
       *
       *   List(
       *     Map("key" -> "foo1", "value" -> "bar1"),
       *     Map("key" -> "foo2", "value" -> "bar2"))
       *
       * into this:
       *
       *   Map("foo1" -> "bar1", "foo2" -> "bar2")
       *
       */
      response.fromJsonMapAs[List[Map[String, String]]]("keyValuePairs") match {
        case Some(maps) => maps.flatMap(m => extractTupleFromKeyValueMap(m)).toMap
        case None => Map()
      }
    }
  }
}

