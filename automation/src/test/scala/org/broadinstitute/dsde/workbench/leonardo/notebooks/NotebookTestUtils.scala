package org.broadinstitute.dsde.workbench.leonardo.notebooks

import java.io.{File, FileOutputStream}
import java.nio.charset.StandardCharsets
import java.util.Base64

import org.broadinstitute.dsde.workbench.dao.Google.googleStorageDAO
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.service.Sam
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google._
import org.openqa.selenium.WebDriver
import org.scalatest.Suite

import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

trait NotebookTestUtils extends LeonardoTestUtils {
  this: Suite with BillingFixtures =>

  private def whenKernelNotReady(t: Throwable): Boolean = t match {
    case e: KernelNotReadyException => true
    case _ => false
  }

  def verifyNotebookCredentials(notebookPage: NotebookPage, expectedEmail: WorkbenchEmail): Unit = {
    // verify google-auth
    notebookPage.executeCell("import google.auth") shouldBe None
    notebookPage.executeCell("credentials, project_id = google.auth.default()") shouldBe None
    notebookPage.executeCell("print credentials._service_account_email") shouldBe Some(expectedEmail.value)

    // verify FISS
    notebookPage.executeCell("import firecloud.api as fapi") shouldBe None
    notebookPage.executeCell("fiss_credentials, project = fapi.google.auth.default()") shouldBe None
    notebookPage.executeCell("print fiss_credentials.service_account_email") shouldBe Some(expectedEmail.value)

    // verify Spark
    notebookPage.executeCell("hadoop_config = sc._jsc.hadoopConfiguration()") shouldBe None
    notebookPage.executeCell("print hadoop_config.get('google.cloud.auth.service.account.enable')") shouldBe Some("true")
    notebookPage.executeCell("print hadoop_config.get('google.cloud.auth.service.account.json.keyfile')") shouldBe Some("/etc/service-account-credentials.json")
    val nbEmail = notebookPage.executeCell("! grep client_email /etc/service-account-credentials.json")
    nbEmail shouldBe 'defined
    nbEmail.get should include (expectedEmail.value)
  }

  // TODO: is there a way to check the cluster credentials on the metadata server?
  def verifyNoNotebookCredentials(notebookPage: NotebookPage): Unit = {
    // verify google-authn
    notebookPage.executeCell("import google.auth") shouldBe None
    notebookPage.executeCell("credentials, project_id = google.auth.default()") shouldBe None
    notebookPage.executeCell("print credentials.service_account_email") shouldBe Some("default")

    // verify FISS
    notebookPage.executeCell("import firecloud.api as fapi") shouldBe None
    notebookPage.executeCell("fiss_credentials, project = fapi.google.auth.default()") shouldBe None
    notebookPage.executeCell("print fiss_credentials.service_account_email") shouldBe Some("default")

    // verify Spark
    notebookPage.executeCell("hadoop_config = sc._jsc.hadoopConfiguration()") shouldBe None
    notebookPage.executeCell("print hadoop_config.get('google.cloud.auth.service.account.enable')") shouldBe Some("None")
    notebookPage.executeCell("print hadoop_config.get('google.cloud.auth.service.account.json.keyfile')") shouldBe Some("None")
  }

  def withNotebooksListPage[T](cluster: Cluster)(testCode: NotebooksListPage => T)(implicit webDriver: WebDriver, token: AuthToken): T = {
    val notebooksListPage = Notebook.get(cluster.googleProject, cluster.clusterName)
    testCode(notebooksListPage.open)
  }


  def withFileUpload[T](cluster: Cluster, file: File)(testCode: NotebooksListPage => T)(implicit webDriver: WebDriver, token: AuthToken): T = {
    withNotebooksListPage(cluster) { notebooksListPage =>
      notebooksListPage.upload(file)
      testCode(notebooksListPage)
    }
  }

  def withNotebookUpload[T](cluster: Cluster, file: File, timeout: FiniteDuration = 2.minutes)(testCode: NotebookPage => T)(implicit webDriver: WebDriver, token: AuthToken): T = {
    withFileUpload(cluster, file) { notebooksListPage =>
      notebooksListPage.withOpenNotebook(file, timeout) { notebookPage =>
        testCode(notebookPage)
      }
    }
  }

  implicit val ec: ExecutionContextExecutor = ExecutionContext.global
  def withNewNotebook[T](cluster: Cluster, kernel: NotebookKernel = Python2, timeout: FiniteDuration = 2.minutes)(testCode: NotebookPage => T)(implicit webDriver: WebDriver, token: AuthToken): T = {
    withNotebooksListPage(cluster) { notebooksListPage =>
      val result: Future[T] = retryUntilSuccessOrTimeout(whenKernelNotReady, failureLogMessage = s"Cannot make new notebook")(30 seconds, 2 minutes) {() =>
        Future(notebooksListPage.withNewNotebook(kernel, timeout) { notebookPage =>
          testCode(notebookPage)
        })
      }
      Await.result(result, 10 minutes)
    }
  }

  def withOpenNotebook[T](cluster: Cluster, notebookPath: File, timeout: FiniteDuration = 2.minutes)(testCode: NotebookPage => T)(implicit webDriver: WebDriver, token: AuthToken): T = {
    withNotebooksListPage(cluster) { notebooksListPage =>
      notebooksListPage.withOpenNotebook(notebookPath, timeout) { notebookPage =>
        testCode(notebookPage)
      }
    }
  }

  def withDummyClientPage[T](cluster: Cluster)(testCode: DummyClientPage => T)(implicit webDriver: WebDriver, token: AuthToken): T = {
    // start a server to load the dummy client page
    val bindingFuture = Notebook.dummyClient.startServer
    val testResult = Try {
      val dummyClientPage = Notebook.dummyClient.get(cluster.googleProject, cluster.clusterName)
      testCode(dummyClientPage)
    }
    // stop the server
    Notebook.dummyClient.stopServer(bindingFuture)
    testResult.get
  }


  def withLocalizeDelocalizeFiles[T](cluster: Cluster, fileToLocalize: String, fileToLocalizeContents: String,
                                     fileToDelocalize: String, fileToDelocalizeContents: String,
                                     dataFileName: String, dataFileContents: String)(testCode: (Map[String, String], GcsBucketName, NotebookPage) => T)
                                    (implicit webDriver: WebDriver, token: AuthToken): T = {
    implicit val patienceConfig: PatienceConfig = storagePatience

    withNewGoogleBucket(cluster.googleProject) { bucketName =>
      // give the user's pet owner access to the bucket
      val petServiceAccount = Sam.user.petServiceAccountEmail(cluster.googleProject.value)
      googleStorageDAO.setBucketAccessControl(bucketName, EmailGcsEntity(GcsEntityTypes.User, petServiceAccount), GcsRoles.Owner).futureValue

      // create a bucket object to localize
      val bucketObjectToLocalize = GcsObjectName(fileToLocalize)
      withNewBucketObject(bucketName, bucketObjectToLocalize, fileToLocalizeContents, "text/plain") { objectName =>
        // give the user's pet read access to the object
        googleStorageDAO.setObjectAccessControl(bucketName, objectName, EmailGcsEntity(GcsEntityTypes.User, petServiceAccount), GcsRoles.Reader).futureValue

        // create a notebook file to delocalize
        withNewNotebook(cluster) { notebookPage =>
          notebookPage.executeCell(s"""! echo -n "$fileToDelocalizeContents" > "$fileToDelocalize"""")

          val localizeRequest = Map(
            fileToLocalize -> GcsPath(bucketName, bucketObjectToLocalize).toUri,
            GcsPath(bucketName, GcsObjectName(fileToDelocalize)).toUri -> fileToDelocalize,
            dataFileName -> s"data:text/plain;base64,${Base64.getEncoder.encodeToString(dataFileContents.getBytes(StandardCharsets.UTF_8))}"
          )

          val testResult = Try(testCode(localizeRequest, bucketName, notebookPage))

          // Verify and save the localization.log file to test output to aid in debugging
          Try(verifyAndSaveLocalizationLog(cluster)) match {
            case Success(downloadFile) =>
              logger.info(s"Saved localization log for cluster ${cluster.projectNameString} to ${downloadFile.getAbsolutePath}")
            case Failure(e) =>
              logger.warn(s"Could not obtain localization log files for cluster ${cluster.projectNameString}: ${e.getMessage}")
          }

          //TODO:: the code below messes up the test somehow, figure out why that happens and fix.
          //TODO:: https://github.com/DataBiosphere/leonardo/issues/643
          // clean up files on the cluster
          // no need to clean up the bucket objects; that will happen as part of `withNewBucketObject`
          //notebookPage.executeCell(s"""! rm -f $fileToLocalize""")
          //notebookPage.executeCell(s"""! rm -f $fileToDelocalize""")

          testResult.get
        }
      }
    }
  }

  def verifyLocalizeDelocalize(cluster: Cluster, localizedFileName: String, localizedFileContents: String,
                               delocalizedBucketPath: GcsPath, delocalizedBucketContents: String,
                               dataFileName: String, dataFileContents: String)(implicit token: AuthToken): Unit = {
    implicit val patienceConfig: PatienceConfig = storagePatience

    // the localized file should exist on the notebook VM
    val item = Notebook.getContentItem(cluster.googleProject, cluster.clusterName, localizedFileName, includeContent = true)
    item.content shouldBe Some(localizedFileContents)

    // the delocalized file should exist in the Google bucket
    val bucketData = googleStorageDAO.getObject(delocalizedBucketPath.bucketName, delocalizedBucketPath.objectName).futureValue
    bucketData.map(_.toString) shouldBe Some(delocalizedBucketContents)

    // the data file should exist on the notebook VM
    val dataItem = Notebook.getContentItem(cluster.googleProject, cluster.clusterName, dataFileName, includeContent = true)
    dataItem.content shouldBe Some(dataFileContents)
  }

  def verifyAndSaveLocalizationLog(cluster: Cluster)(implicit token: AuthToken): File = {
    // check localization.log for existence
    val localizationLog = Notebook.getContentItem(cluster.googleProject, cluster.clusterName, "localization.log", includeContent = true)
    localizationLog.content shouldBe defined

    // Save localization.log to test output to aid in debugging
    val downloadFile = new File(logDir, s"${cluster.googleProject.value}-${cluster.clusterName.string}-localization.log")
    val fos = new FileOutputStream(downloadFile)
    fos.write(localizationLog.content.get.getBytes)
    fos.close()

    downloadFile
  }

  def verifyHailImport(notebookPage: NotebookPage, vcfPath: GcsPath, cluster: Cluster): Unit = {
    val hailTimeout = 10 minutes
    val welcomeToHail =
      """Welcome to
        |     __  __     <>__
        |    / /_/ /__  __/ /
        |   / __  / _ `/ / /
        |  /_/ /_/\_,_/_/_/""".stripMargin

    val vcfDescription =
      """Row fields:
        |    'locus': locus<GRCh37>
        |    'alleles': array<str>
        |    'rsid': str
        |    'qual': float64
        |    'filters': set<str>
        |    'info': struct {
        |        LDAF: float64,
        |        AVGPOST: float64,
        |        RSQ: float64,
        |        ERATE: float64,
        |        THETA: float64,
        |        CIEND: array<int32>,
        |        CIPOS: array<int32>,
        |        END: int32,
        |        HOMLEN: array<int32>,
        |        HOMSEQ: array<str>,
        |        SVLEN: int32,
        |        SVTYPE: str,
        |        AC: array<int32>,
        |        AN: int32,
        |        AA: str,
        |        AF: array<float64>,
        |        AMR_AF: float64,
        |        ASN_AF: float64,
        |        AFR_AF: float64,
        |        EUR_AF: float64,
        |        VT: str,
        |        SNPSOURCE: array<str>""".stripMargin

    val elapsed = time {
      notebookPage.executeCell("import hail as hl") shouldBe None
      notebookPage.executeCell("hl.init(sc)").get should include(welcomeToHail)

      notebookPage.executeCell(s"chr20vcf = '${vcfPath.toUri}'") shouldBe None
      notebookPage.executeCell("imported = hl.import_vcf(chr20vcf)", hailTimeout) shouldBe None

      //notebookPage.executeCell("imported.describe()", hailTimeout).get should include(vcfDescription)
      notebookPage.executeCell("imported.describe()", hailTimeout).get should include("Row fields:")
    }

    logger.info(s"Hail import for cluster ${cluster.projectNameString}} took ${elapsed.duration.toSeconds} seconds")

    // show that the Hail log contains jobs that were run on preemptible nodes
    // TODO this check is not always reliable

    //val preemptibleNodePrefix = cluster.clusterName.string + "-sw"
    //notebookPage.executeCell(s"! grep Finished ~/hail.log | grep $preemptibleNodePrefix").get should include(preemptibleNodePrefix)
  }

  def uploadDownloadTest(cluster: Cluster, uploadFile: File, timeout: FiniteDuration, fileDownloadDir: String)(assertion: (File, File) => Any)(implicit webDriver: WebDriver, token: AuthToken): Any = {
    cluster.status shouldBe ClusterStatus.Running
    uploadFile.exists() shouldBe true

    withNotebookUpload(cluster, uploadFile) { notebook =>
      notebook.runAllCells(timeout)
      notebook.downloadAsIpynb()
    }

    // sanity check the file downloaded correctly
    val downloadFile = new File(fileDownloadDir, uploadFile.getName)
    downloadFile.exists() shouldBe true
    downloadFile.isFile() shouldBe true
    downloadFile.deleteOnExit()
  }


  def pipInstall(notebookPage: NotebookPage, kernel: NotebookKernel, packageName: String): Unit = {
    val pip = kernel match {
      case Python2 | PySpark2 => "pip2"
      case Python3 | PySpark3 => "pip3"
      case _ => throw new IllegalArgumentException(s"Can't pip install in a ${kernel.string} kernel")
    }

    val installOutput = notebookPage.executeCell(s"!$pip install $packageName")
    installOutput shouldBe 'defined
    installOutput.get should include (s"Collecting $packageName")
    installOutput.get should include ("Installing collected packages:")
    installOutput.get should include ("Successfully installed")
    installOutput.get should not include ("Exception:")
  }

  // https://github.com/aymericdamien/TensorFlow-Examples/blob/master/notebooks/1_Introduction/helloworld.ipynb
  def verifyTensorFlow(notebookPage: NotebookPage, kernel: NotebookKernel): Unit = {
    notebookPage.executeCell("import tensorflow as tf")
    notebookPage.executeCell("hello = tf.constant('Hello, TensorFlow!')") shouldBe None
    notebookPage.executeCell("sess = tf.Session()") shouldBe None
    val helloOutput = notebookPage.executeCell("print(sess.run(hello))")
    kernel match {
      case Python2 => helloOutput shouldBe Some("Hello, TensorFlow!")
      case Python3 => helloOutput shouldBe Some("b'Hello, TensorFlow!'")
      case other => fail(s"Unexpected kernel: $other")
    }
  }

}
