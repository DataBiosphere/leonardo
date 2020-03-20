package org.broadinstitute.dsde.workbench.leonardo.dao

import java.text.SimpleDateFormat
import java.time.Instant

import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.cloud.compute.v1.Instance
import org.broadinstitute.dsde.workbench.google.GoogleUtilities.RetryPredicates._
import org.broadinstitute.dsde.workbench.google2.{MachineTypeName, NetworkName, RegionName, SubnetworkName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.VPCConfig.{VPCNetwork, VPCSubnet}
import org.broadinstitute.dsde.workbench.leonardo.IP
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.collection.JavaConverters._
import scala.util.Try

package object google {

  final val retryPredicates = List(
    when5xx _,
    whenUsageLimited _,
    whenGlobalUsageLimited _,
    when404 _,
    whenInvalidValueOnBucketCreation _,
    whenNonHttpIOException _
  )

  final val when400: Throwable => Boolean = {
    case t: GoogleJsonResponseException => t.getStatusCode == 400
    case _                              => false
  }

  final val when401: Throwable => Boolean = {
    case t: GoogleJsonResponseException => t.getStatusCode == 401
    case _                              => false
  }

  final val whenGoogleZoneCapacityIssue: Throwable => Boolean = {
    case t: GoogleJsonResponseException =>
      t.getStatusCode == 429
    case _ => false
  }

  def parseGoogleTimestamp(googleTimestamp: String): Option[Instant] =
    Try {
      new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX").parse(googleTimestamp)
    }.toOption map { date =>
      Instant.ofEpochMilli(date.getTime)
    }

  def getInstanceIP(instance: Instance): Option[IP] =
    for {
      interfaces <- Option(instance.getNetworkInterfacesList)
      interface <- interfaces.asScala.headOption
      accessConfigs <- Option(interface.getAccessConfigsList)
      accessConfig <- accessConfigs.asScala.headOption
    } yield IP(accessConfig.getNatIP)

  def buildMachineTypeUri(zone: ZoneName, machineTypeName: MachineTypeName): String =
    s"zones/${zone.value}/machineTypes/${machineTypeName.value}"

  // Note networks are global, subnets are regional
  // See: https://cloud.google.com/vpc/docs/vpc

  def buildNetworkUri(googleProject: GoogleProject, vpcNetwork: NetworkName): String =
    s"projects/${googleProject.value}/global/networks/${vpcNetwork.value}"

  def buildSubnetworkUri(googleProject: GoogleProject, regionName: RegionName, vpcSubnet: SubnetworkName): String =
    s"projects/${googleProject.value}/regions/${regionName.value}/subnetworks/${vpcSubnet.value}"
}
