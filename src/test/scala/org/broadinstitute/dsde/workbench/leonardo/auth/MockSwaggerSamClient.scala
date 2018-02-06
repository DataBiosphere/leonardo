package org.broadinstitute.dsde.workbench.leonardo.auth

import java.io.File

import org.broadinstitute.dsde.workbench.leonardo.model.ClusterName
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.duration.{FiniteDuration, SECONDS}
import scala.collection.concurrent.TrieMap
import scala.collection.mutable

class MockSwaggerSamClient extends SwaggerSamClient("fake/path", new FiniteDuration(1, SECONDS), 0, WorkbenchEmail("fake-user@example.com"), new File("fake-pem")) {

  val billingProjects: mutable.Map[(GoogleProject, WorkbenchEmail), Set[String]] =  new TrieMap()
  val notebookClusters: mutable.Map[(GoogleProject, ClusterName, WorkbenchEmail), Set[String]] = new TrieMap()
  val userProxy = "PROXY_1234567890@dev.test.firecloud.org"

  override def createNotebookClusterResource(userEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName) = {
    notebookClusters += (googleProject, clusterName, userEmail) -> Set("status", "connect", "sync", "delete", "read_policies")
  }

  override def deleteNotebookClusterResource(userEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName) = {
    notebookClusters.remove((googleProject, clusterName, userEmail))
  }

  override def hasActionOnBillingProjectResource(userEmail: WorkbenchEmail, googleProject: GoogleProject, action: String): Boolean = {
    billingProjects.get((googleProject, userEmail)) //look it up: Option[Set]
      .map( _.contains(action) ) //open the option to peek the set: Option[Bool]
      .getOrElse(false) //unpack the resulting option and handle the project never having existed
  }

  override def hasActionOnNotebookClusterResource(userEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName, action: String): Boolean = {
    notebookClusters.get((googleProject, clusterName, userEmail))
      .map( _.contains(action) )
      .getOrElse(false)
  }

  override def getUserProxyFromSam(userEmail: WorkbenchEmail): WorkbenchEmail = {
    WorkbenchEmail(userProxy)
  }
}
