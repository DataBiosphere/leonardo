package org.broadinstitute.dsde.workbench.leonardo

import java.net.{URI, URL}
import java.time.Instant
import java.util.UUID
import com.google.common.net.UrlEscapers
import scala.language.implicitConversions
import org.broadinstitute.dsde.workbench.leonardo.ClusterStatus.ClusterStatus
import org.broadinstitute.dsde.workbench.leonardo.StringValueClass.LabelMap
import scala.util.{Failure, Success, Try}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

sealed trait StringValueClass extends Any
case class ClusterName(string: String) extends AnyVal with StringValueClass
case class GoogleServiceAccount(string: String) extends AnyVal with StringValueClass
case class IP(string: String) extends AnyVal with StringValueClass
case class OperationName(string: String) extends AnyVal with StringValueClass

case class GcsPath(bucketName: GcsBucketName, relativePath: GcsRelativePath) {
  final val GCS_SCHEME = "gs"
  def toUri: String = s"$GCS_SCHEME://${bucketName.name}/${relativePath.name}"
}

object GcsPath {
    def parse(str: String): Either[GcsParseError, GcsPath] =
        GcsPathParser.parseGcsPathFromString(str)
  }

case class GcsParseError(message: String) extends AnyVal

private object GcsPathParser {
    final val GCS_SCHEME = "gs"

      /*
   * Provides some level of validation of GCS bucket names.
   * See https://cloud.google.com/storage/docs/naming for full spec
   */
    final val GCS_BUCKET_NAME_PATTERN_BASE =
        """[a-z0-9][a-z0-9-_\\.]{1,61}[a-z0-9]"""
    final val GCS_BUCKET_NAME_PATTERN = s"""^$GCS_BUCKET_NAME_PATTERN_BASE$$""".r

      /*
   * Regex for a full GCS path which captures the bucket name.
   */
    final val GCS_PATH_PATTERN =
        s"""
      (?x)                                      # Turn on comments and whitespace insensitivity
      ^${GCS_SCHEME}://
      (                                         # Begin capturing group for gcs bucket name
        $GCS_BUCKET_NAME_PATTERN_BASE           # Regex for bucket name - soft validation, see comment above
      )                                         # End capturing group for gcs bucket name
      /
      (?:
        .*                                      # No validation here
      )?
    """.trim.r


    def parseGcsPathFromString(path: String): Either[GcsParseError, GcsPath] = {
        for {
            uri <- parseAsUri(path).right
            _ <- validateScheme(uri).right
                                              host <- getAndValidateHost(path, uri).right
                                            relativePath <- getAndValidateRelativePath(uri).right
        } yield GcsPath(host, relativePath)
      }
    def parseAsUri(path: String): Either[GcsParseError, URI] = {
        Try {
            URI.create(UrlEscapers.urlFragmentEscaper().escape(path))
          } match {
          case Success(uri) => Right[GcsParseError, URI](uri)
          case Failure(regret) => Left[GcsParseError, URI](GcsParseError(s"Unparseable GCS path: ${regret.getMessage}"))
        }
      }

      def validateScheme(uri: URI): Either[GcsParseError, Unit] = {
        // Allow null or gs:// scheme
          if (uri.getScheme == null || uri.getScheme == GCS_SCHEME) {
            Right(())
          } else {
            Left(GcsParseError(s"Invalid scheme: ${uri.getScheme}"))
          }
      }

      def getAndValidateHost(path: String, uri: URI): Either[GcsParseError, GcsBucketName] = {
        // Get the host from the URI if we can, and validate it against GCS_BUCKET_NAME_PATTERN
          val parsedFromUri = for {
            h <- Option(uri.getHost)
            _ <- GCS_BUCKET_NAME_PATTERN.findFirstMatchIn(h)
                        } yield h

    // It's possible for it to not be a valid URI, but still be a valid bucket name.
       // For example a_bucket_with_underscores is a valid GCS name but not a valid URI.
         // So if URI parsing fails, still try to extract a valid bucket name using GCS_PATH_PATTERN.
        val parsed = parsedFromUri.orElse {
            for {
                m <- GCS_PATH_PATTERN.findFirstMatchIn(path)
                g <- Option(m.group(1))
            } yield g
          }

          parsed.map(GcsBucketName.apply)
          .toRight(GcsParseError(s"Could not parse bucket name from path: $path"))
      }

      def getAndValidateRelativePath(uri: URI): Either[GcsParseError, GcsRelativePath] = {
        Option(uri.getPath).map(_.stripPrefix("/")).map(GcsRelativePath.apply)
          .toRight(GcsParseError(s"Could not parse bucket relative path from path: ${uri.toString}"))
      }
  }

/** A GCS relative path */
case class GcsRelativePath(name: String) extends AnyVal

/** A valid GCS bucket name */
case class GcsBucketName(name: String) extends AnyVal

case class MachineConfig(numberOfWorkers: Option[Int] = None,
                         masterMachineType: Option[String] = None,
                         masterDiskSize: Option[Int] = None,  //min 10
                         workerMachineType: Option[String] = None,
                         workerDiskSize: Option[Int] = None,   //min 10
                         numberOfWorkerLocalSSDs: Option[Int] = None, //min 0 max 8
                         numberOfPreemptibleWorkers: Option[Int] = None
                        )
object MachineConfig {
  // TODO: something less hacky
  def apply(m: Map[String, String]): MachineConfig = MachineConfig(
    m.get("numberOfWorkers").map(Integer.parseInt),
    m.get("masterMachineType"),
    m.get("masterDiskSize").map(Integer.parseInt),
    m.get("workerMachineType"),
    m.get("workerDiskSize").map(Integer.parseInt),
    m.get("numberOfWorkerLocalSSDs").map(Integer.parseInt),
    m.get("numberOfPreemptibleWorkers").map(Integer.parseInt)
  )
}

case class Cluster(clusterName: ClusterName,
                   googleId: UUID,
                   googleProject: GoogleProject,
                   googleServiceAccount: GoogleServiceAccount,
                   googleBucket: GcsBucketName,
                   machineConfig: MachineConfig,
                   clusterUrl: URL,
                   operationName: OperationName,
                   status: ClusterStatus,
                   hostIp: Option[IP],
                   createdDate: Instant,
                   destroyedDate: Option[Instant],
                   labels: LabelMap,
                   jupyterExtensionUri: Option[GcsPath])

case class ClusterRequest(bucketPath: GcsBucketName,
                          labels: LabelMap,
                          jupyterExtensionUri: Option[String] = None)

case class DefaultLabels(clusterName: ClusterName,
                         googleProject: GoogleProject,
                         googleBucket: GcsBucketName,
                         serviceAccount: WorkbenchEmail,
                         notebookExtension: Option[GcsPath]) {

  // TODO don't hardcode fields
  def toMap: Map[String, String] = {
    val ext: Map[String, String] = notebookExtension map { ext => Map("notebookExtension" -> ext.toUri) } getOrElse Map.empty

    Map(
      "clusterName" -> clusterName.string,
      "googleProject" -> googleProject.value,
      "googleBucket" -> googleBucket.name,
      "serviceAccount" -> serviceAccount.value
    ) ++ ext
  }
}

object StringValueClass {
  type LabelMap = Map[String, String]
}

object ClusterStatus extends Enumeration {
  type ClusterStatus = Value
  //NOTE: Remember to update the definition of this enum in Swagger when you add new ones
  val Unknown, Creating, Running, Updating, Error, Deleting, Deleted = Value
  val activeStatuses = Set(Unknown, Creating, Running, Updating)
  val monitoredStatuses = Set(Unknown, Creating, Updating, Deleting)

  class StatusValue(status: ClusterStatus) {
    def isActive: Boolean = activeStatuses contains status
    def isMonitored: Boolean = monitoredStatuses contains status
  }
  implicit def enumConvert(status: ClusterStatus): StatusValue = new StatusValue(status)

  def withNameOpt(s: String): Option[ClusterStatus] = values.find(_.toString == s)

  def withNameIgnoreCase(str: String): ClusterStatus = {
    values.find(_.toString.equalsIgnoreCase(str)).getOrElse(throw new IllegalArgumentException(s"Unknown cluster status: $str"))
  }
}
