package org.broadinstitute.dsde.workbench.leonardo.auth

import java.io.File

import org.broadinstitute.dsde.workbench.leonardo.model.ClusterName
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.ExecutionContext

class MockSwaggerSamClient extends SwaggerSamClient("fake/path", 0, 0, WorkbenchEmail("fake-user@example.com"), new File("fake-pem")) {


  val billingProjects: mutable.Map[(GoogleProject, WorkbenchEmail),  Set[String]] =  new TrieMap()
  val notebookClusters: mutable.Map[(GoogleProject, ClusterName, WorkbenchEmail), Set[String]] = new TrieMap()


  override def createNotebookClusterResource(userEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext) = {
    notebookClusters += (googleProject, clusterName, userEmail) -> Set("status", "connect", "sync", "delete", "read_policies")
  }

  override def deleteNotebookClusterResource(userEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext) = {
    notebookClusters.remove((googleProject, clusterName, userEmail))
  }

  override def hasActionOnBillingProjectResource(userEmail: WorkbenchEmail, googleProject: GoogleProject, action: String)(implicit executionContext: ExecutionContext): Boolean = {
    if (billingProjects.contains((googleProject, userEmail)))
      billingProjects.get((googleProject, userEmail)).get.contains(action)
    else false
  }

  override def hasActionOnNotebookClusterResource(userEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName, action: String)(implicit executionContext: ExecutionContext): Boolean = {
    if (notebookClusters.contains((googleProject, clusterName, userEmail)))
      notebookClusters.get((googleProject, clusterName, userEmail)).get.contains(action)
    else false
  }
}
