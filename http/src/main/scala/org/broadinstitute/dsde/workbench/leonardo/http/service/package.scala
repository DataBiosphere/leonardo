package org.broadinstitute.dsde.workbench.leonardo
package http

import cats.syntax.all._
import cats.Monoid
import org.broadinstitute.dsde.workbench.leonardo.model.ParseLabelsException

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
              case _ =>
                ParseLabelsException(
                  s"Could not parse label string: $extraLabels. Expected format [key1=value1,key2=value2,...]"
                ).asLeft[LabelMap]
            }
          }
          .toList

        implicit val mapAdd: Monoid[Map[String, String]] = Monoid.instance(Map.empty, (mp1, mp2) => mp1 ++ mp2)
        labels.combineAll
      case None => Right(params)
    }

  private[service] def processListParameters(
    params: LabelMap
  ): Either[ParseLabelsException, (LabelMap, Boolean, List[String])] =
    // returns a tuple made of three elements:
    // 1) LabelMap - represents the labels to filter the request by
    // 2) includeDeleted - Boolean which determines if we include deleted resources in the response
    // 3) includeLabels - List of label keys which represent the labels (key, value pairs) that will be returned in response
    for {
      labelMap <- processLabelMap(params - includeDeletedKey - includeLabelsKey - creatorOnlyKey)
      includeDeleted = params.get(includeDeletedKey) match {
        case Some(includeDeletedValue) =>
          if (includeDeletedValue.toLowerCase == "true")
            true
          else false
        case None => false
      }
      includeLabels <- params.get(includeLabelsKey) match {
        case Some(includeLabelsValue) => processLabelsToReturn(includeLabelsValue)
        case None                     => Either.right(List.empty[String])
      }
    } yield (labelMap, includeDeleted, includeLabels)

  /**
   * Top-level query string parameter for apps and disks: GET /api/apps?includeLabels=foo,bar
   * where foo,bar are label keys for which this endpoint returns the LabelMap for each key
   *
   * @param params raw query string params
   */
  private[service] def processLabelsToReturn(
    labelsToReturn: String
  ): Either[ParseLabelsException, List[String]] =
    Either
      .catchNonFatal(labelsToReturn.split(',').toList.filter(l => !l.isEmpty))
      .leftMap(_ =>
        ParseLabelsException(
          s"Failed to process ${includeLabelsKey} query string because it's not comma separated. Expected format [key1,key2,...]"
        )
      )

}
