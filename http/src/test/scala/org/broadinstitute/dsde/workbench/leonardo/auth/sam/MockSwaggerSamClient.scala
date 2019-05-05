package org.broadinstitute.dsde.workbench.leonardo.auth.sam

import java.io.File

import io.swagger.client.ApiException
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.duration.{FiniteDuration, SECONDS}
import scala.collection.concurrent.TrieMap
import scala.collection.mutable

class MockSwaggerSamClient extends SwaggerSamClient("fake/path", true, new FiniteDuration(1, SECONDS), 0, WorkbenchEmail("fake-user@example.com"), new File("fake-pem")) {

  val billingProjects: mutable.Map[(GoogleProject, WorkbenchEmail), Set[String]] =  new TrieMap()
  val notebookClusters: mutable.Map[(GoogleProject, ClusterName, WorkbenchEmail), Set[String]] = new TrieMap()
  val projectOwners: mutable.Map[WorkbenchEmail, Set[GoogleProject]] = new TrieMap()
  val clusterCreators: mutable.Map[WorkbenchEmail, Set[(GoogleProject, ClusterName)]] = new TrieMap()
  val userProxy = "PROXY_1234567890@dev.test.firecloud.org"
  val serviceAccount = WorkbenchEmail("pet-1234567890@test-project.iam.gserviceaccount.com")

  @throws[ApiException]
  override def createNotebookClusterResource(userEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName) = {
    notebookClusters += (googleProject, clusterName, userEmail) -> Set("status", "connect", "sync", "delete", "read_policies")
  }

  @throws[ApiException]
  override def deleteNotebookClusterResource(userEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName) = {
    notebookClusters.remove((googleProject, clusterName, userEmail))
  }

  override def hasActionOnBillingProjectResource(userInfo: UserInfo, googleProject: GoogleProject, action: String): Boolean = {
    billingProjects.get((googleProject, userInfo.userEmail)) //look it up: Option[Set]
      .map( _.contains(action) ) //open the option to peek the set: Option[Bool]
      .getOrElse(false) //unpack the resulting option and handle the project never having existed
  }

  override def hasActionOnNotebookClusterResource(userInfo: UserInfo, googleProject: GoogleProject, clusterName: ClusterName, action: String): Boolean = {
    notebookClusters.get((googleProject, clusterName, userInfo.userEmail))
      .map( _.contains(action) )
      .getOrElse(false)
  }

  override def getUserProxyFromSam(userEmail: WorkbenchEmail): WorkbenchEmail = {
    WorkbenchEmail(userProxy)
  }

  override def getPetServiceAccount(userInfo: UserInfo, googleProject: GoogleProject): WorkbenchEmail = {
    serviceAccount
  }

  override def listOwningProjects(userInfo: UserInfo): List[GoogleProject] = {
    projectOwners.get(userInfo.userEmail).map(_.toList).getOrElse(List.empty)
  }

  override def listCreatedClusters(userInfo: UserInfo): List[(GoogleProject, ClusterName)] = {
    clusterCreators.get(userInfo.userEmail).map(_.toList).getOrElse(List.empty)
  }

  override def getCachedPetAccessToken(userEmail: WorkbenchEmail, googleProject: GoogleProject): String = {
    "token"
  }
}
