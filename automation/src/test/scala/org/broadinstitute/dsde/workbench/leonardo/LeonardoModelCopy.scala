package org.broadinstitute.dsde.workbench.leonardo

import java.net.URL
import java.time.Instant
import java.util.UUID
import scala.language.implicitConversions
import org.broadinstitute.dsde.workbench.leonardo.ClusterStatus.ClusterStatus
import org.broadinstitute.dsde.workbench.leonardo.StringValueClass.LabelMap
import org.broadinstitute.dsde.workbench.google.gcs.GcsPath
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

sealed trait StringValueClass extends Any
case class ClusterName(string: String) extends AnyVal with StringValueClass
case class GoogleServiceAccount(string: String) extends AnyVal with StringValueClass
case class IP(string: String) extends AnyVal with StringValueClass
case class OperationName(string: String) extends AnyVal with StringValueClass

/** A GCS relative path */
case class GcsRelativePath(name: String) extends AnyVal

/** A valid GCS bucket name */
case class GcsBucketName(name: String) extends AnyVal

case class MachineConfig(numberOfWorkers: Option[Int] = None,
                         masterMachineType: Option[String] = None,
                         masterDiskSize: Option[Int] = None,  //min 10
                         workerMachineType: Option[String] = None,
                         workerDiskSize: Option[Int] = None,   //min 10
                         numberOfWorkerLocalSSDs: Option[Int] = None, //min 0 max 8
                         numberOfPreemptibleWorkers: Option[Int] = None
                        )
object MachineConfig {
  // TODO: something less hacky
  def apply(m: Map[String, String]): MachineConfig = MachineConfig(
    m.get("numberOfWorkers").map(Integer.parseInt),
    m.get("masterMachineType"),
    m.get("masterDiskSize").map(Integer.parseInt),
    m.get("workerMachineType"),
    m.get("workerDiskSize").map(Integer.parseInt),
    m.get("numberOfWorkerLocalSSDs").map(Integer.parseInt),
    m.get("numberOfPreemptibleWorkers").map(Integer.parseInt)
  )
}

case class ServiceAccountInfo(clusterServiceAccount: Option[WorkbenchEmail],
                              overrideServiceAccount: Option[WorkbenchEmail])

object ServiceAccountInfo {
  // TODO: something less hacky, please!
  // If we're going to use Jackson we should use it the right way, with annotations in our model.
  // Otherwise we should rip out LeonardoModelCopy + ClusterKluge and just use Leo model objects + spray json (my prefrence).
  def apply(m: Map[String, String]): ServiceAccountInfo = ServiceAccountInfo(
    m.get("clusterServiceAccount").map(WorkbenchEmail),
    m.get("overrideServiceAccount").map(WorkbenchEmail)
  )
}

case class Cluster(clusterName: ClusterName,
                   googleId: UUID,
                   googleProject: GoogleProject,
                   serviceAccountInfo: ServiceAccountInfo,
                   googleBucket: GcsBucketName,
                   machineConfig: MachineConfig,
                   clusterUrl: URL,
                   operationName: OperationName,
                   status: ClusterStatus,
                   hostIp: Option[IP],
                   creator: WorkbenchEmail,
                   createdDate: Instant,
                   destroyedDate: Option[Instant],
                   labels: LabelMap,
                   jupyterExtensionUri: Option[GcsPath])

case class ClusterRequest(bucketPath: GcsBucketName,
                          labels: LabelMap,
                          jupyterExtensionUri: Option[String] = None)

case class DefaultLabels(clusterName: ClusterName,
                         googleProject: GoogleProject,
                         googleBucket: GcsBucketName,
                         clusterServiceAccount: Option[WorkbenchEmail],
                         overrideServiceAccount: Option[WorkbenchEmail],
                         notebookExtension: Option[String]) {

  // TODO don't hardcode fields
  def toMap: Map[String, String] = {
    val ext: Map[String, String] = notebookExtension map { ext => Map("notebookExtension" -> ext) } getOrElse Map.empty
    val clusterSa: Map[String, String] = clusterServiceAccount map { sa => Map("clusterServiceAccount" -> sa.value) } getOrElse Map.empty
    val overrideSa: Map[String, String] = overrideServiceAccount map { sa => Map("overrideServiceAccount" -> sa.value) } getOrElse Map.empty

    Map(
      "clusterName" -> clusterName.string,
      "googleProject" -> googleProject.value,
      "googleBucket" -> googleBucket.name
    ) ++ ext ++ clusterSa ++ overrideSa
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
