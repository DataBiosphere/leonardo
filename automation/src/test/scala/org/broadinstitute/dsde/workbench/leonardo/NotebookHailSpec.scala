package org.broadinstitute.dsde.workbench.leonardo

import java.io.File
import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermission

import org.broadinstitute.dsde.workbench.ResourceFile

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.collection.JavaConverters._

class NotebookHailSpec extends ClusterFixtureSpec {

  /**
    * Create a new temp dir
    */
  private def tempDownloadDir(): String = {
    val downloadPath = s"chrome/downloads/${makeRandomId(5)}"
    val dir = new File(downloadPath)
    dir.deleteOnExit()
    dir.mkdirs()
    val path = dir.toPath
    logger.info(s"mkdir: $path")
    val permissions = Set(
      PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_EXECUTE,
      PosixFilePermission.GROUP_WRITE, PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_EXECUTE,
      PosixFilePermission.OTHERS_WRITE, PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_EXECUTE)
    import scala.collection.JavaConverters._
    Files.setPosixFilePermissions(path, permissions.asJava)
    path.toString
  }


  "Leonardo notebooks" - {

    val hailUploadFile = ResourceFile(s"diff-tests/import-hail.ipynb")
    val hailTutorialUploadFile = ResourceFile(s"diff-tests/hail-tutorial.ipynb")

    "should upload notebook and verify execution" in { clusterFixture =>
      // output for this notebook includes an IP address which can vary
      val downloadDir = tempDownloadDir()
      withWebDriver(downloadDir) { implicit driver =>
        uploadDownloadTest(clusterFixture.cluster, hailUploadFile, 60.seconds, downloadDir)(compareFilesExcludingIPs)
      }
    }

    // See https://hail.is/docs/stable/tutorials-landing.html
    // Note this is for the stable Hail version (0.1). The tutorial script has changed in Hail 0.2.
    "should run the Hail tutorial" in { clusterFixture =>
      val downloadDir = tempDownloadDir()
      withWebDriver(downloadDir) { implicit driver =>
        uploadDownloadTest(clusterFixture.cluster, hailTutorialUploadFile, 3.minutes, downloadDir) { (uploadFile, downloadFile) =>
          // There are many differences including timestamps, so we can't really compare uploadFile
          // and downloadFile correctly. For now just verify the absence of ClassCastExceptions, which is the
          // issue reported in https://github.com/DataBiosphere/leonardo/issues/222.
          val downloadFileContents: String = Files.readAllLines(downloadFile.toPath).asScala.mkString
          downloadFileContents should not include "ClassCastException"
        }
      }
    }

  }

}
