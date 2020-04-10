package org.broadinstitute.dsde.workbench.leonardo

import java.net.URL
import java.time.Instant

import enumeratum.{Enum, EnumEntry}
import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.ClusterStatus.ClusterStatus
import org.broadinstitute.dsde.workbench.leonardo.RuntimeStatus.findValues
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google._

import scala.language.implicitConversions

sealed trait StringValueClass extends Any
case class GoogleServiceAccount(string: String) extends AnyVal with StringValueClass

case class  ClusterCopy(clusterName: RuntimeName,
                       googleProject: GoogleProject,
                       serviceAccountInfo: ServiceAccountInfo,
                       machineConfig: RuntimeConfig,
                       status: ClusterStatus,
                       creator: WorkbenchEmail,
                       labels: LabelMap,
                       stagingBucket: Option[GcsBucketName],
                       errors: List[RuntimeError],
                       dateAccessed: Instant,
                       stopAfterCreation: Boolean,
                       autopauseThreshold: Int,
                       patchInProgress: Boolean) {
  def projectNameString: String = s"${googleProject.value}/${clusterName.asString}"
}

// Same as DataprocConfig but uses String instead of MachineConfig because of jackson serialization problems
// https://github.com/FasterXML/jackson-module-scala/issues/209
/*final case class DataprocConfigCopy(numberOfWorkers: Int,
                                    masterMachineType: String,
                                    masterDiskSize: Int, //min 10
                                    // worker settings are None when numberOfWorkers is 0
                                    workerMachineType: Option[String] = None,
                                    workerDiskSize: Option[Int] = None, //min 10
                                    numberOfWorkerLocalSSDs: Option[Int] = None, //min 0 max 8
                                    numberOfPreemptibleWorkers: Option[Int] = None)


object DataprocConfigCopy {

  def fromDataprocConfig(dataprocConfig: RuntimeConfig.DataprocConfig): DataprocConfigCopy =
    DataprocConfigCopy(
      dataprocConfig.numberOfWorkers,
      dataprocConfig.machineType.value,
      dataprocConfig.masterDiskSize.gb,
      dataprocConfig.workerMachineType.map(_.value),
      dataprocConfig.workerDiskSize.map(_.gb),
      dataprocConfig.numberOfWorkerLocalSSDs,
      dataprocConfig.numberOfPreemptibleWorkers
    )
}*/

sealed trait RuntimeConfigRequest extends Product with Serializable {
  def typedCloudService: CloudService
}
object RuntimeConfigRequest {
  final case class GceConfig(
                              cloudService: String = "GCE",
                              machineType: Option[String],
                              diskSize: Option[Int]
                            ) extends RuntimeConfigRequest {
    val typedCloudService: CloudService = CloudService.GCE

  }




  final case class DataprocConfig(numberOfWorkers: Option[Int],
                                  masterMachineType: Option[String],
                                  masterDiskSize: Option[Int], //min 10
                                  // worker settings are None when numberOfWorkers is 0
                                  workerMachineType: Option[String] = None,
                                  workerDiskSize: Option[Int] = None, //min 10
                                  numberOfWorkerLocalSSDs: Option[Int] = None, //min 0 max 8
                                  numberOfPreemptibleWorkers: Option[Int] = None,
                                  properties: Map[String, String])
    extends RuntimeConfigRequest {
    val typedCloudService: CloudService = CloudService.Dataproc

  }
}

case class ClusterRequest(labels: LabelMap = Map(),
                          jupyterExtensionUri: Option[String] = None,
                          jupyterUserScriptUri: Option[String] = None,
                          jupyterStartUserScriptUri: Option[String] = None,
                          machineConfig: Option[RuntimeConfigRequest] = None,
                          //machineConfig: Option[DataprocConfigCopy] = None,
                          properties: Map[String, String] = Map(),
                          stopAfterCreation: Option[Boolean] = None,
                          userJupyterExtensionConfig: Option[UserJupyterExtensionConfig] = None,
                          autopause: Option[Boolean] = None,
                          autopauseThreshold: Option[Int] = None,
                          defaultClientId: Option[String] = None,
                          toolDockerImage: Option[String] = None,
                          welderDockerImage: Option[String] = None,
                          scopes: Set[String] = Set.empty,
                          enableWelder: Option[Boolean] = None,
                          customClusterEnvironmentVariables: Map[String, String] = Map.empty,
                          allowStop: Boolean = false)
//ceck swagger page to clean up parameters
case class RuntimeRequest(labels: LabelMap = Map(),
                          jupyterExtensionUri: Option[String] = None,
                          jupyterUserScriptUri: Option[String] = None,
                          jupyterStartUserScriptUri: Option[String] = None,
                          runtimeConfig: Option[RuntimeConfigRequest] = None,
                          properties: Map[String, String] = Map(),
                          stopAfterCreation: Option[Boolean] = None,
                          userJupyterExtensionConfig: Option[UserJupyterExtensionConfig] = None,
                          autopause: Option[Boolean] = None,
                          autopauseThreshold: Option[Int] = None,
                          defaultClientId: Option[String] = None,
                          toolDockerImage: Option[String] = None,
                          welderDockerImage: Option[String] = None,
                          scopes: Set[String] = Set.empty,
                          customClusterEnvironmentVariables: Map[String, String] = Map.empty,
                          allowStop: Boolean = false)

case class UserJupyterExtensionConfig(nbExtensions: Map[String, String] = Map(),
                                      serverExtensions: Map[String, String] = Map(),
                                      combinedExtensions: Map[String, String] = Map(),
                                      labExtensions: Map[String, String] = Map())

case class DefaultLabelsCopy(runtimeName: RuntimeName,
                             googleProject: GoogleProject,
                             creator: WorkbenchEmail,
                             clusterServiceAccount: Option[WorkbenchEmail],
                             notebookServiceAccount: Option[WorkbenchEmail],
                             notebookExtension: Option[String],
                             notebookUserScript: Option[String],
                             notebookStartUserScript: Option[String],
                             tool: String) {

  // TODO don't hardcode fields
  def toMap: Map[String, String] = {
    val ext: Map[String, String] = notebookExtension map { ext =>
      Map("notebookExtension" -> ext)
    } getOrElse Map.empty
    val userScr: Map[String, String] = notebookUserScript map { userScr =>
      Map("notebookUserScript" -> userScr)
    } getOrElse Map.empty
    val startScr: Map[String, String] = notebookStartUserScript map { startScr =>
      Map("notebookStartUserScript" -> startScr)
    } getOrElse Map.empty
    val clusterSa: Map[String, String] = clusterServiceAccount map { sa =>
      Map("clusterServiceAccount" -> sa.value)
    } getOrElse Map.empty
    val notebookSa: Map[String, String] = notebookServiceAccount map { sa =>
      Map("notebookServiceAccount" -> sa.value)
    } getOrElse Map.empty

    Map(
      "clusterName" -> runtimeName.asString,
      "runtimeName" -> runtimeName.asString,
      "googleProject" -> googleProject.value,
      "creator" -> creator.value,
      "tool" -> tool
    ) ++ ext ++ userScr ++ startScr ++ clusterSa ++ notebookSa
  }
}

//TODO Rename to RuntimeStatus once everything is running. Avoiding for now for compile issues
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

  def withNameIgnoreCase(str: String): ClusterStatus =
    values
      .find(_.toString.equalsIgnoreCase(str))
      .getOrElse(throw new IllegalArgumentException(s"Unknown cluster status: $str"))
}



final case class GetRuntimeResponseCopy(
                                    runtimeName: RuntimeName,
                                    googleProject: GoogleProject,
                                    serviceAccount: WorkbenchEmail,
                                    auditInfo: AuditInfo,
                                    asyncRuntimeFields: Option[AsyncRuntimeFields],
                                    runtimeConfig: RuntimeConfig,
                                    clusterUrl: URL,
                                    status: ClusterStatus,
                                    labels: LabelMap,
                                    jupyterExtensionUri: Option[GcsPath],
                                    jupyterUserScriptUri: Option[UserScriptPath],
                                    jupyterStartUserScriptUri: Option[UserScriptPath],
                                    errors: List[RuntimeError],
                                    //dataprocInstances: Set[DataprocInstance],
                                    userJupyterExtensionConfig: Option[UserJupyterExtensionConfig],
                                    autopauseThreshold: Int)

final case class ListRuntimeResponseCopy(id: Long,
                                         runtimeName: RuntimeName,
                                         googleProject: GoogleProject,
                                         serviceAccount: WorkbenchEmail,
                                         asyncRuntimeFields: Option[AsyncRuntimeFields],
                                         auditInfo: AuditInfo,
                                         runtimeConfig: RuntimeConfig,
                                         proxyUrl: URL,
                                         status: ClusterStatus,
                                         labels: LabelMap,
                                         jupyterExtensionUri: Option[GcsPath],
                                         jupyterUserScriptUri: Option[UserScriptPath],
                                         autopauseThreshold: Int,
                                         defaultClientId: Option[String]
                                        )