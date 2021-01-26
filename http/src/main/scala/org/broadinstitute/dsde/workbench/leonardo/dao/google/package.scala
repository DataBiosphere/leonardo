package org.broadinstitute.dsde.workbench.leonardo.dao

import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.cloud.compute.v1.Instance
import org.broadinstitute.dsde.workbench.google.GoogleUtilities.RetryPredicates._
import org.broadinstitute.dsde.workbench.google2.{MachineTypeName, ZoneName}
import org.broadinstitute.dsde.workbench.model.IP

import java.text.SimpleDateFormat
import java.time.Instant
import scala.jdk.CollectionConverters._
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
    }.toOption map { date => Instant.ofEpochMilli(date.getTime) }

  def getInstanceIP(instance: Instance): Option[IP] =
    for {
      interfaces <- Option(instance.getNetworkInterfacesList)
      interface <- interfaces.asScala.headOption
      accessConfigs <- Option(interface.getAccessConfigsList)
      accessConfig <- accessConfigs.asScala.headOption
      natIP <- Option(accessConfig.getNatIP)
    } yield IP(natIP)

  def buildMachineTypeUri(zone: ZoneName, machineTypeName: MachineTypeName): String =
    s"zones/${zone.value}/machineTypes/${machineTypeName.value}"
}
