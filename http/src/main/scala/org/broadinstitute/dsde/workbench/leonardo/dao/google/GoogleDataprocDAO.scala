package org.broadinstitute.dsde.workbench.leonardo
package dao
package google

import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.workbench.google2.{OperationName, SubnetworkName}
import org.broadinstitute.dsde.workbench.leonardo.CustomImage.DataprocCustomImage
import org.broadinstitute.dsde.workbench.leonardo.model.LeoException
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google._

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

case class DataprocDisabledException(errorMsg: String) extends LeoException(s"${errorMsg}", StatusCodes.Forbidden)

// TODO: use google2 DataprocService
trait GoogleDataprocDAO {
  def createCluster(createClusterConfig: CreateClusterConfig): Future[GoogleOperation]

  def deleteCluster(googleProject: GoogleProject, clusterName: RuntimeName): Future[Unit]

  def getClusterStatus(googleProject: GoogleProject, clusterName: RuntimeName): Future[Option[DataprocClusterStatus]]

  def listClusters(googleProject: GoogleProject): Future[List[UUID]]

  def getClusterMasterInstance(googleProject: GoogleProject,
                               clusterName: RuntimeName
  ): Future[Option[DataprocInstanceKey]]

  def getClusterStagingBucket(googleProject: GoogleProject, clusterName: RuntimeName): Future[Option[GcsBucketName]]

  def getClusterErrorDetails(operationName: Option[OperationName]): Future[Option[RuntimeErrorDetails]]

  def resizeCluster(googleProject: GoogleProject,
                    clusterName: RuntimeName,
                    numWorkers: Option[Int] = None,
                    numPreemptibles: Option[Int] = None
  ): Future[Unit]
}

final case class CreateClusterConfig(
  projectAndName: RuntimeProjectAndName,
  machineConfig: RuntimeConfig.DataprocConfig,
  initScripts: List[GcsPath],
  clusterServiceAccount: WorkbenchEmail,
  credentialsFileName: String,
  stagingBucket: GcsBucketName,
  clusterScopes: Set[String],
  subnetwork: SubnetworkName,
  dataprocCustomImage: DataprocCustomImage,
  creationTimeout: FiniteDuration
)
