package org.broadinstitute.dsde.workbench.leonardo.model
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._

import scala.util.parsing.json.JSONObject

case class ClusterRequest(bucketPath: String, serviceAccount: String, labels: Map[String, String])
case class ClusterResponse(clusterName: String, googleProject: String, clusterId: String, status: String, description: String, operationName: String) // <--- Expand as needed

object LeonardoJsonSupport {
  import spray.json.DefaultJsonProtocol._

  implicit val ClusterRequestFormat = jsonFormat3(ClusterRequest)
  implicit val clusterResponseFormat = jsonFormat6(ClusterResponse)

}