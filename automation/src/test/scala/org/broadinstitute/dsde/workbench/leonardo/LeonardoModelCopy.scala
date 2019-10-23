package org.broadinstitute.dsde.workbench.leonardo

import java.time.Instant

import org.broadinstitute.dsde.workbench.leonardo.ClusterStatus.ClusterStatus
import org.broadinstitute.dsde.workbench.leonardo.StringValueClass.LabelMap
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google._
import scala.language.implicitConversions

sealed trait StringValueClass extends Any
case class ClusterName(string: String) extends AnyVal with StringValueClass
case class GoogleServiceAccount(string: String) extends AnyVal with StringValueClass

case class Cluster(clusterName: ClusterName,
                   googleProject: GoogleProject,
                   serviceAccountInfo: ServiceAccountInfo,
                   machineConfig: MachineConfig,
                   status: ClusterStatus,
                   creator: WorkbenchEmail,
                   labels: LabelMap,
                   stagingBucket:Option[GcsBucketName],
                   errors:List[ClusterError],
                   dateAccessed: Instant,
                   stopAfterCreation: Boolean) {
  def projectNameString: String = s"${googleProject.value}/${clusterName.string}"
}

case class ClusterRequest(labels: LabelMap = Map(),
                          jupyterExtensionUri: Option[String] = None,
                          jupyterUserScriptUri: Option[String] = None,
                          machineConfig: Option[MachineConfig] = None,
                          properties: Map[String, String] = Map(),
                          stopAfterCreation: Option[Boolean] = None,
                          userJupyterExtensionConfig: Option[UserJupyterExtensionConfig] = None,
                          autopause: Option[Boolean] = None,
                          autopauseThreshold: Option[Int] = None,
                          defaultClientId: Option[String] = None,
                          jupyterDockerImage: Option[String] = None,
                          rstudioDockerImage: Option[String] = None,
                          welderDockerImage: Option[String] = None,
                          scopes: Set[String] = Set.empty,
                          enableWelder: Option[Boolean] = None,
                          customClusterEnvironmentVariables: Map[String, String] = Map.empty)

case class UserJupyterExtensionConfig(nbExtensions: Map[String, String] = Map(),
                                      serverExtensions: Map[String, String] = Map(),
                                      combinedExtensions: Map[String, String] = Map(),
                                      labExtensions: Map[String, String] = Map())

case class DefaultLabels(clusterName: ClusterName,
                         googleProject: GoogleProject,
                         creator: WorkbenchEmail,
                         clusterServiceAccount: Option[WorkbenchEmail],
                         notebookServiceAccount: Option[WorkbenchEmail],
                         notebookExtension: Option[String],
                         notebookUserScript: Option[String]) {

  // TODO don't hardcode fields
  def toMap: Map[String, String] = {
    val ext: Map[String, String] = notebookExtension map { ext => Map("notebookExtension" -> ext) } getOrElse Map.empty
    val userScr: Map[String, String] = notebookUserScript map {userScr => Map("notebookUserScript" -> userScr) }  getOrElse Map.empty
    val clusterSa: Map[String, String] = clusterServiceAccount map { sa => Map("clusterServiceAccount" -> sa.value) } getOrElse Map.empty
    val notebookSa: Map[String, String] = notebookServiceAccount map { sa => Map("notebookServiceAccount" -> sa.value) } getOrElse Map.empty

    Map(
      "clusterName" -> clusterName.string,
      "googleProject" -> googleProject.value,
      "creator" -> creator.value
    ) ++ ext ++ userScr ++ clusterSa ++ notebookSa
  }
}

object StringValueClass {
  type LabelMap = Map[String, String]
}

object ClusterStatus extends Enumeration {
  type ClusterStatus = Value
  //NOTE: Remember to update the definition of this enum in Swagger when you add new ones
  val Unknown, Creating, Running, Updating, Error, Deleting, Deleted, Stopping, Stopped, Starting = Value
  val activeStatuses = Set(Unknown, Creating, Running, Updating)
  val monitoredStatuses = Set(Unknown, Creating, Updating, Deleting)
  val deletableStatuses: Set[ClusterStatus] = Set(Unknown, Running, Updating, Error, Stopping, Stopped, Starting)

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
