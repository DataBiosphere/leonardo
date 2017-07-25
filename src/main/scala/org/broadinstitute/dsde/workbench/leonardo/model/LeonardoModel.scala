package org.broadinstitute.dsde.workbench.leonardo.model

case class ClusterRequest (bucketPath: String, serviceAccount: String, labels: /*Map[String, String]*/ String)


object LeonardoJsonSupport {
  import spray.json.DefaultJsonProtocol._

  implicit val ClusterRequestFormat = jsonFormat3(ClusterRequest)

}