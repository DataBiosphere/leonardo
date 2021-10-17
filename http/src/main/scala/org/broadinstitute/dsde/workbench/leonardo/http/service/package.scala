package org.broadinstitute.dsde.workbench.leonardo
package http

import cats.syntax.all._
import cats.Monoid
import org.broadinstitute.dsde.workbench.leonardo.model.{BadRequestException, ParseLabelsException}
import org.broadinstitute.dsde.workbench.model.TraceId

package object service {

  /**
   * There are 2 styles of passing labels to the list clusters endpoint:
   *
   * 1. As top-level query string parameters: GET /api/clusters?foo=bar&baz=biz
   * 2. Using the _labels query string parameter: GET /api/clusters?_labels=foo%3Dbar,baz%3Dbiz
   *
   * The latter style exists because Swagger doesn't provide a way to specify free-form query string
   * params. This method handles both styles, and returns a Map[String, String] representing the labels.
   *
   * Note that style 2 takes precedence: if _labels is present on the query string, any additional
   * parameters are ignored.
   *
   * @param params raw query string params
   * @return a Map[String, String] representing the labels
   */
  private[service] def processLabelMap(params: LabelMap): Either[ParseLabelsException, LabelMap] =
    params.get("_labels") match {
      case Some(extraLabels) =>
        val labels: List[Either[ParseLabelsException, LabelMap]] = extraLabels
          .split(',')
          .map { c =>
            c.split('=') match {
              case Array(key, value) => Map(key -> value).asRight[ParseLabelsException]
              case _                 => (ParseLabelsException(extraLabels)).asLeft[LabelMap]
            }
          }
          .toList

        implicit val mapAdd: Monoid[Map[String, String]] = Monoid.instance(Map.empty, (mp1, mp2) => mp1 ++ mp2)
        labels.combineAll
      case None => Right(params)
    }

  private[service] def processListParameters(
    params: LabelMap
  ): Either[ParseLabelsException, (LabelMap, Boolean)] =
    params.get(includeDeletedKey) match {
      case Some(includeDeletedValue) =>
        processLabelMap(params - includeDeletedKey).map(lm => (lm, includeDeletedValue.toBoolean))
      case None =>
        processLabelMap(params).map(lm => (lm, false))
    }

  /**
   * Top-level query string parameter for apps and disks: GET /api/apps?includeLabels=foo,bar
   * where foo,bar are label keys for which this endpoint returns the LabelMap for each key
   *
   * @param params raw query string params
   */
  private[service] def processLabelsToReturn(
    params: LabelMap,
    traceId: Option[TraceId]
  ): Either[BadRequestException, List[String]] =
    params.get(includeLabelsKey) match {
      case Some(includeLabelsValue) =>
        Either
          .catchNonFatal(includeLabelsValue.split(',').toList)
          .leftMap(_ =>
            BadRequestException(
              s"Failed to process ${includeLabelsKey} query string because it's not comma separated. Expected format [key1,key2,...]",
              traceId
            )
          )
      case None =>
        List.empty.asRight[BadRequestException]
    }

}
