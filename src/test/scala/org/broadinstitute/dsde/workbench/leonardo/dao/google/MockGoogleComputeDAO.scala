package org.broadinstitute.dsde.workbench.leonardo.dao.google
import org.broadinstitute.dsde.workbench.leonardo.model.google.InstanceStatus.{Running, Stopped}
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.Future

/**
  * Created by rtitle on 2/16/18.
  */
class MockGoogleComputeDAO extends GoogleComputeDAO {
  val instances: mutable.Map[InstanceKey, Instance] = new TrieMap()
  val firewallRules: mutable.Map[GoogleProject, FirewallRule] = new TrieMap()

  override def getInstance(instanceKey: InstanceKey): Future[Option[Instance]] = {
    Future.successful(instances.get(instanceKey))
  }

  override def stopInstance(instanceKey: InstanceKey): Future[Unit] = {
    instances.get(instanceKey).foreach { instance =>
      instances += instanceKey -> instance.copy(status = Stopped)
    }
    Future.successful(())
  }

  override def startInstance(instanceKey: InstanceKey): Future[Unit] = {
    instances.get(instanceKey).foreach { instance =>
      instances += instanceKey -> instance.copy(status = Running)
    }
    Future.successful(())
  }

  override def updateFirewallRule(googleProject: GoogleProject, firewallRule: FirewallRule): Future[Unit] = {
    if (!firewallRules.contains(googleProject)) {
      firewallRules += googleProject -> firewallRule
    }
    Future.successful(())
  }

  override def getComputeEngineDefaultServiceAccount(googleProject: GoogleProject): Future[Option[WorkbenchEmail]] = {
    Future.successful(Some(WorkbenchEmail("compute-engine@example.com")))
  }
}
