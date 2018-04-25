package org.broadinstitute.dsde.workbench.leonardo.dao.google

import java.text.SimpleDateFormat
import java.time.Instant

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import cats.implicits._
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.http.HttpResponseException
import com.google.api.services.cloudresourcemanager.CloudResourceManager
import com.google.api.services.compute.model.Firewall.Allowed
import com.google.api.services.compute.model.Metadata.Items
import com.google.api.services.compute.model.{Firewall, Metadata, Instance => GoogleInstance}
import com.google.api.services.compute.{Compute, ComputeScopes}
import org.broadinstitute.dsde.workbench.google.AbstractHttpGoogleDAO
import org.broadinstitute.dsde.workbench.google.GoogleCredentialModes._
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.metrics.GoogleInstrumentedService
import org.broadinstitute.dsde.workbench.metrics.GoogleInstrumentedService.GoogleInstrumentedService
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{WorkbenchEmail, WorkbenchException}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
  * Created by rtitle on 2/13/18.
  */
class HttpGoogleComputeDAO(appName: String,
                           googleCredentialMode: GoogleCredentialMode,
                           override val workbenchMetricBaseName: String)
                          (implicit override val system: ActorSystem, override val executionContext: ExecutionContext)
  extends AbstractHttpGoogleDAO(appName, googleCredentialMode, workbenchMetricBaseName) with GoogleComputeDAO {

  override implicit val service: GoogleInstrumentedService = GoogleInstrumentedService.Compute

  override val scopes: Seq[String] = Seq(ComputeScopes.COMPUTE)

  private lazy val resourceManagerScopes = Seq(ComputeScopes.CLOUD_PLATFORM)

  private lazy val compute = {
    new Compute.Builder(httpTransport, jsonFactory, googleCredential)
      .setApplicationName(appName).build()
  }

  private lazy val cloudResourceManager = {
    val resourceManagerCredential = googleCredential.createScoped(resourceManagerScopes.asJava)
    new CloudResourceManager.Builder(httpTransport, jsonFactory, resourceManagerCredential)
      .setApplicationName(appName).build()
  }

  override def getInstance(instanceKey: InstanceKey): Future[Option[Instance]] = {
    val request = compute.instances().get(instanceKey.project.value, instanceKey.zone.value, instanceKey.name.value)

    retryWithRecoverWhen500orGoogleError { () =>
      Option(executeGoogleRequest(request)) map { gi =>
        Instance(
          instanceKey,
          gi.getId,
          InstanceStatus.withNameInsensitive(gi.getStatus),
          getInstanceIP(gi),
          dataprocRole = None,
          parseGoogleTimestamp(gi.getCreationTimestamp).getOrElse(Instant.now))
      }
    } {
      case e: HttpResponseException if e.getStatusCode == StatusCodes.NotFound.intValue => None
    }.handleGoogleException(instanceKey)
  }

  override def stopInstance(instanceKey: InstanceKey): Future[Unit] = {
    val request = compute.instances().stop(instanceKey.project.value, instanceKey.zone.value, instanceKey.name.value)

    retryWhen500orGoogleError(() => executeGoogleRequest(request)).void.handleGoogleException(instanceKey)
  }

  override def startInstance(instanceKey: InstanceKey): Future[Unit] = {
    val request = compute.instances.start(instanceKey.project.value, instanceKey.zone.value, instanceKey.name.value)

    retryWhen500orGoogleError(() => executeGoogleRequest(request)).void.handleGoogleException(instanceKey)
  }

  override def addInstanceMetadata(instanceKey: InstanceKey, metadata: Map[String, String]): Future[Unit] = {
    val getInstanceRequest = compute.instances().get(instanceKey.project.value, instanceKey.zone.value, instanceKey.name.value)
    retryWhen500orGoogleError(() => executeGoogleRequest(getInstanceRequest)).flatMap { googleInstance =>
      val curMetadataOpt = for {
        instance <- Option(googleInstance)
        metadata <- Option(instance.getMetadata)
      } yield metadata

      val fingerprint = curMetadataOpt.flatMap(m => Option(m.getFingerprint)).orNull
      val curItems: Seq[Items] = curMetadataOpt.flatMap(m => Option(m.getItems)).map(_.asScala).getOrElse(List.empty)

      val newMetadata = new Metadata()
        .setFingerprint(fingerprint)
        .setItems((curItems.filterNot(i => metadata.contains(i.getKey)) ++ metadata.toList.map { case (k, v) => new Items().setKey(k).setValue(v) }).asJava)
      val setMetadataRequest = compute.instances.setMetadata(instanceKey.project.value, instanceKey.zone.value, instanceKey.name.value, newMetadata)
      retryWhen500orGoogleError(() => executeGoogleRequest(setMetadataRequest)).void.handleGoogleException(instanceKey)
    }
  }

  override def updateFirewallRule(googleProject: GoogleProject, firewallRule: FirewallRule): Future[Unit] = {
    val request = compute.firewalls().get(googleProject.value, firewallRule.name.value)
    val response = retryWithRecoverWhen500orGoogleError { () =>
      executeGoogleRequest(request)
      ()
    } {
      case e: HttpResponseException if e.getStatusCode == StatusCodes.NotFound.intValue => addFirewallRule(googleProject, firewallRule)
    }

    response.handleGoogleException(googleProject, Some(firewallRule.name.value))
  }

  /**
    * Adds a firewall rule in the given google project. This firewall rule allows ingress traffic through a specified port for all
    * VMs with the network tag "leonardo". This rule should only be added once per project.
    * To think about: do we want to remove this rule if a google project no longer has any clusters? */
  private def addFirewallRule(googleProject: GoogleProject, firewallRule: FirewallRule): Future[Unit] = {
    val allowed = new Allowed().setIPProtocol(firewallRule.protocol.value).setPorts(firewallRule.ports.map(_.value).asJava)
    // note: network not used
    val googleFirewall = new Firewall()
      .setName(firewallRule.name.value)
      .setTargetTags(firewallRule.targetTags.map(_.value).asJava)
      .setAllowed(List(allowed).asJava)

    val request = compute.firewalls().insert(googleProject.value, googleFirewall)
    logger.info(s"Creating firewall rule with name '${firewallRule.name.value}' in project ${googleProject.value}")
    retryWhen500orGoogleError(() => executeGoogleRequest(request)).void
  }

  override def getComputeEngineDefaultServiceAccount(googleProject: GoogleProject): Future[Option[WorkbenchEmail]] = {
    getProjectNumber(googleProject).map { numberOpt =>
      numberOpt.map { number =>
        // Service account email format documented in:
        // https://cloud.google.com/compute/docs/access/service-accounts#compute_engine_default_service_account
        WorkbenchEmail(s"$number-compute@developer.gserviceaccount.com")
      }
    }.handleGoogleException(googleProject)
  }

  private def parseGoogleTimestamp(googleTimestamp: String): Option[Instant] = {
    Try {
      new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX").parse(googleTimestamp)
    }.toOption map { date =>
      Instant.ofEpochMilli(date.getTime)
    }
  }

  private def getProjectNumber(googleProject: GoogleProject): Future[Option[Long]] = {
    val request = cloudResourceManager.projects().get(googleProject.value)
    retryWithRecoverWhen500orGoogleError { () =>
      Option(executeGoogleRequest(request).getProjectNumber).map(_.toLong)
    } {
      case e: HttpResponseException if e.getStatusCode == StatusCodes.NotFound.intValue => None
    }
  }

  /**
    * Gets the public IP from a google Instance, with error handling.
    * @param instance the Google instance
    * @return error or public IP, as a String
    */
  private def getInstanceIP(instance: GoogleInstance): Option[IP] = {
    for {
      interfaces <- Option(instance.getNetworkInterfaces)
      interface <- interfaces.asScala.headOption
      accessConfigs <- Option(interface.getAccessConfigs)
      accessConfig <- accessConfigs.asScala.headOption
    } yield IP(accessConfig.getNatIP)
  }

  private implicit class GoogleExceptionSupport[A](future: Future[A]) {
    def handleGoogleException(project: GoogleProject, context: Option[String] = None): Future[A] = {
      future.recover {
        case e: GoogleJsonResponseException =>
          val msg = s"Call to Google API failed for ${project.value} ${context.map(c => s"/ $c").getOrElse("")}. Status: ${e.getStatusCode}. Message: ${e.getDetails.getMessage}"
          logger.error(msg, e)
          throw new WorkbenchException(msg, e)
        case e: IllegalArgumentException =>
          val msg = s"Illegal argument passed to Google request for ${project.value} ${context.map(c => s"/ $c").getOrElse("")}. Message: ${e.getMessage}"
          logger.error(msg, e)
          throw new WorkbenchException(msg, e)
      }
    }

    def handleGoogleException(instanceKey: InstanceKey): Future[A] = {
      handleGoogleException(instanceKey.project, Some(instanceKey.name.value))
    }
  }
}
