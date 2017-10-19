package org.broadinstitute.dsde.workbench.leonardo

import java.net.URL
import java.time.Instant
import java.util.UUID

import scala.language.implicitConversions
import org.broadinstitute.dsde.workbench.leonardo.ClusterStatus.ClusterStatus
import org.broadinstitute.dsde.workbench.leonardo.StringValueClass.LabelMap

sealed trait StringValueClass extends Any
case class GoogleProject(string: String) extends AnyVal with StringValueClass
case class ClusterName(string: String) extends AnyVal with StringValueClass
case class GoogleServiceAccount(string: String) extends AnyVal with StringValueClass
case class IP(string: String) extends AnyVal with StringValueClass
case class OperationName(string: String) extends AnyVal with StringValueClass

case class GcsPath(bucketName: GcsBucketName, relativePath: GcsRelativePath) {
  final val GCS_SCHEME = "gs"
  def toUri: String = s"$GCS_SCHEME://${bucketName.name}/${relativePath.name}"
}

/** A GCS relative path */
case class GcsRelativePath(name: String) extends AnyVal

/** A valid GCS bucket name */
case class GcsBucketName(name: String) extends AnyVal

case class Cluster(clusterName: ClusterName,
                   googleId: UUID,
                   googleProject: GoogleProject,
                   googleServiceAccount: GoogleServiceAccount,
                   googleBucket: GcsBucketName,
                   clusterUrl: URL,
                   operationName: OperationName,
                   status: ClusterStatus,
                   hostIp: Option[IP],
                   createdDate: Instant,
                   destroyedDate: Option[Instant],
                   labels: LabelMap,
                   jupyterExtensionUri: Option[GcsPath])

case class ClusterRequest(bucketPath: GcsBucketName,
                          serviceAccount: GoogleServiceAccount,
                          labels: LabelMap,
                          jupyterExtensionUri: Option[GcsPath])

case class DefaultLabels(clusterName: ClusterName,
                         googleProject: GoogleProject,
                         googleBucket: GcsBucketName,
                         serviceAccount: GoogleServiceAccount,
                         notebookExtension: Option[GcsPath]) {

  // TODO don't hardcode fields
  def toMap: Map[String, String] = {
    val ext: Map[String, String] = notebookExtension map { ext => Map("notebookExtension" -> ext.toUri) } getOrElse Map.empty

    Map(
      "clusterName" -> clusterName.string,
      "googleProject" -> googleProject.string,
      "googleBucket" -> googleBucket.name,
      "serviceAccount" -> serviceAccount.string
    ) ++ ext
  }
}

object StringValueClass {
  type LabelMap = Map[String, String]
}

object ClusterStatus extends Enumeration {
  type ClusterStatus = Value
  //NOTE: Remember to update the definition of this enum in Swagger when you add new ones
  val Unknown, Creating, Running, Updating, Error, Deleting, Deleted = Value
  val activeStatuses = Set(Unknown, Creating, Running, Updating)
  val monitoredStatuses = Set(Unknown, Creating, Updating, Deleting)

  class StatusValue(status: ClusterStatus) {
    def isActive: Boolean = activeStatuses contains status
    def isMonitored: Boolean = monitoredStatuses contains status
  }
  implicit def enumConvert(status: ClusterStatus): StatusValue = new StatusValue(status)

  def withNameOpt(s: String): Option[ClusterStatus] = values.find(_.toString == s)

  def withNameIgnoreCase(str: String): ClusterStatus = {
    values.find(_.toString.equalsIgnoreCase(str)).getOrElse(throw new IllegalArgumentException(s"Unknown cluster status: $str"))
  }
}
