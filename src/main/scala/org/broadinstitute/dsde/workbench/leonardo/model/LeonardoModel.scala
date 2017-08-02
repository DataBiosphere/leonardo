package org.broadinstitute.dsde.workbench.leonardo.model
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._

case class ClusterRequest(bucketPath: String, serviceAccount: String, labels: Map[String, String])


object LeonardoJsonSupport {
  import spray.json.DefaultJsonProtocol._

  implicit val ClusterRequestFormat = jsonFormat3(ClusterRequest)

}