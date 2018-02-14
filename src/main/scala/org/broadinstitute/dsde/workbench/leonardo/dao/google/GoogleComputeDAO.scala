package org.broadinstitute.dsde.workbench.leonardo.dao.google

import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.Future

/**
  * Created by rtitle on 2/13/18.
  */
trait GoogleComputeDAO {

  def getInstance(instanceKey: InstanceKey): Future[Option[Instance]]

  def stopInstance(instanceKey: InstanceKey): Future[Unit]

  def startInstance(instanceKey: InstanceKey): Future[Unit]

  def updateFirewallRule(googleProject: GoogleProject, firewallRule: FirewallRule): Future[Unit]

  def getComputeEngineDefaultServiceAccount(googleProject: GoogleProject): Future[Option[WorkbenchEmail]]

}
