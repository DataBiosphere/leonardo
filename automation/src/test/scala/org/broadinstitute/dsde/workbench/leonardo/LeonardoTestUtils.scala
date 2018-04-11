package org.broadinstitute.dsde.workbench.leonardo

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File, FileOutputStream}
import java.nio.file.Files
import java.time.Instant

import cats.data.OptionT
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.dao.Google.{googleIamDAO, googleStorageDAO}
import org.broadinstitute.dsde.workbench.auth.{AuthToken, UserAuthToken}
import org.broadinstitute.dsde.workbench.config.{Config, Credentials}
import org.broadinstitute.dsde.workbench.service.{RestException, Sam}
import org.broadinstitute.dsde.workbench.service.test.WebBrowserSpec
import org.broadinstitute.dsde.workbench.leonardo.ClusterStatus.ClusterStatus
import org.broadinstitute.dsde.workbench.leonardo.StringValueClass.LabelMap
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.util.LocalFileUtil
import org.openqa.selenium.WebDriver
import org.scalactic.source.Position
import org.scalatest.{Matchers, Suite}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Minutes, Seconds, Span}

import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Random, Try}


case class TimeResult[R](result:R, duration:FiniteDuration)

trait LeonardoTestUtils extends WebBrowserSpec with Matchers with Eventually with LocalFileUtil with LazyLogging with ScalaFutures {
  this: Suite =>

  val swatTestBucket = "gs://leonardo-swat-test-bucket-do-not-delete"
  val incorrectJupyterExtensionUri = swatTestBucket + "/"
  val testJupyterExtensionUri = swatTestBucket + "/my_ext.tar.gz"

  // must align with run-tests.sh and hub-compose-fiab.yml
  val downloadDir = "chrome/downloads"

  val logDir = new File("output")
  logDir.mkdirs

  // Ron and Hermione are on the dev Leo whitelist, and Hermione is a Project Owner
  lazy val ronCreds: Credentials = Config.Users.NotebooksWhitelisted.getUserCredential("ron")
  lazy val hermioneCreds: Credentials = Config.Users.NotebooksWhitelisted.getUserCredential("hermione")

  lazy val ronAuthToken = UserAuthToken(ronCreds)
  lazy val hermioneAuthToken = UserAuthToken(hermioneCreds)
  lazy val ronEmail = ronCreds.email

  val clusterPatience = PatienceConfig(timeout = scaled(Span(30, Minutes)), interval = scaled(Span(20, Seconds)))
  val localizePatience = PatienceConfig(timeout = scaled(Span(1, Minutes)), interval = scaled(Span(1, Seconds)))
  val saPatience = PatienceConfig(timeout = scaled(Span(1, Minutes)), interval = scaled(Span(1, Seconds)))
  val storagePatience = PatienceConfig(timeout = scaled(Span(1, Minutes)), interval = scaled(Span(1, Seconds)))
  val tenSeconds = FiniteDuration(10, SECONDS)
  val startPatience = PatienceConfig(timeout = scaled(Span(5, Minutes)), interval = scaled(Span(1, Seconds)))

  // TODO: show diffs as screenshot or other test output?
  def compareFilesExcludingIPs(left: File, right: File): Unit = {

    def linesWithoutIPs(file: File) = {
      import scala.collection.JavaConverters._
      Files.readAllLines(file.toPath).asScala map { _.replaceAll("(\\d+.){3}\\d+", "<IP>") }
    }

    linesWithoutIPs(left) shouldEqual linesWithoutIPs(right)
  }

  def labelCheck(seen: LabelMap,
                 clusterName: ClusterName,
                 googleProject: GoogleProject,
                 creator: WorkbenchEmail,
                 clusterRequest: ClusterRequest): Unit = {

    // the SAs can vary here depending on which ServiceAccountProvider is used
    // set dummy values here and then remove them from the comparison
    // TODO: check for these values after tests are agnostic to ServiceAccountProvider ?

    val dummyClusterSa = WorkbenchEmail("dummy-cluster")
    val dummyNotebookSa = WorkbenchEmail("dummy-notebook")
    val expected = clusterRequest.labels ++ DefaultLabels(clusterName, googleProject, creator, Some(dummyClusterSa), Some(dummyNotebookSa), clusterRequest.jupyterExtensionUri, clusterRequest.jupyterUserScriptUri).toMap

    (seen - "clusterServiceAccount" - "notebookServiceAccount") shouldBe (expected - "clusterServiceAccount" - "notebookServiceAccount")
  }

  def clusterCheck(cluster: Cluster,
                   expectedProject: GoogleProject,
                   expectedName: ClusterName,
                   expectedStatuses: Iterable[ClusterStatus],
                   clusterRequest: ClusterRequest): Cluster = {

    // Always log cluster errors
    if (cluster.errors.nonEmpty) {
      logger.warn(s"Cluster ${cluster.googleProject}/${cluster.clusterName} returned the following errors: ${cluster.errors}")
    }

    withClue(s"Cluster ${cluster.googleProject}/${cluster.clusterName}: ") {
      expectedStatuses should contain (cluster.status)
    }

    cluster.googleProject shouldBe expectedProject
    cluster.clusterName shouldBe expectedName
    cluster.stagingBucket shouldBe 'defined

    implicit val patienceConfig: PatienceConfig = storagePatience
    googleStorageDAO.bucketExists(GcsBucketName(cluster.stagingBucket.get.value)).futureValue shouldBe true
    labelCheck(cluster.labels, expectedName, expectedProject, cluster.creator, clusterRequest)
    cluster
  }

  // creates a cluster and checks to see that it reaches the Running state
  def createAndMonitor(googleProject: GoogleProject, clusterName: ClusterName, clusterRequest: ClusterRequest)(implicit token: AuthToken): Cluster = {
    // Google doesn't seem to like simultaneous cluster creates.  Add 0-30 sec jitter
    Thread sleep Random.nextInt(30000)

    val clusterTimeResult = time(Leonardo.cluster.create(googleProject, clusterName, clusterRequest))
    val cluster = clusterTimeResult.result
    clusterCheck(cluster, googleProject, clusterName, Seq(ClusterStatus.Creating), clusterRequest)
    //TODO Uncomment the following line when the response time is truly less than 10s
    //clusterTimeResult.duration should be < tenSeconds
    logger.info("Time to get cluster create response::" + clusterTimeResult.duration)

    // verify with get()
    val creatingCluster = clusterCheck(Leonardo.cluster.get(googleProject, clusterName), googleProject, clusterName, Seq(ClusterStatus.Creating), clusterRequest)

    // wait for "Running" or error (fail fast)
    implicit val patienceConfig: PatienceConfig = clusterPatience
    val actualCluster = Try {
      eventually {
        clusterCheck(Leonardo.cluster.get(googleProject, clusterName), googleProject, clusterName, Seq(ClusterStatus.Running, ClusterStatus.Error), clusterRequest)
      }
    }

    // Save the cluster init log file whether or not the cluster created successfully
    implicit val ec = ExecutionContext.global
    saveDataprocLogFiles(creatingCluster).recover { case e =>
      logger.error(s"Error occurred saving Dataproc log files for cluster ${cluster.googleProject}/${cluster.clusterName}", e)
      None
    }.futureValue match {
      case Some((initLog, startupLog)) =>
        logger.info(s"Saved Dataproc init log file for cluster ${cluster.googleProject}/${cluster.clusterName} to ${initLog.getAbsolutePath}")
        logger.info(s"Saved Dataproc startup log file for cluster ${cluster.googleProject}/${cluster.clusterName} to ${startupLog.getAbsolutePath}")
      case None => logger.warn(s"Could not obtain Dataproc log files for cluster ${cluster.googleProject}/${cluster.clusterName}")
    }

    actualCluster.get
  }

  // deletes a cluster and checks to see that it reaches the Deleted state
  def deleteAndMonitor(googleProject: GoogleProject, clusterName: ClusterName)(implicit token: AuthToken): Unit = {
    try {
      Leonardo.cluster.delete(googleProject, clusterName) shouldBe
        "The request has been accepted for processing, but the processing has not been completed."
    } catch {
      // OK if cluster not found / already deleted
      case re: RestException if re.message.contains("\"statusCode\":404") => ()
      case e: Exception => throw e
    }

    // wait until not found or in "Deleted" state
    implicit val patienceConfig: PatienceConfig = clusterPatience
    eventually {
      val statusOpt = Leonardo.cluster.listIncludingDeleted().find(_.clusterName == clusterName).map(_.status)
      statusOpt getOrElse ClusterStatus.Deleted shouldBe ClusterStatus.Deleted
    }
  }

  def stopAndMonitor(googleProject: GoogleProject, clusterName: ClusterName)(implicit webDriver: WebDriver, token: AuthToken): Unit = {
    Leonardo.cluster.stop(googleProject, clusterName) shouldBe
      "The request has been accepted for processing, but the processing has not been completed."

    // verify with get()
    val stoppingCluster = Leonardo.cluster.get(googleProject, clusterName)
    stoppingCluster.status shouldBe ClusterStatus.Stopping

    // wait until in Stopped state
    implicit val patienceConfig: PatienceConfig = clusterPatience
    eventually {
      val status = Leonardo.cluster.get(googleProject, clusterName).status
      status shouldBe ClusterStatus.Stopped
    }

    // Verify notebook error
    val caught = the [RestException] thrownBy {
      Leonardo.notebooks.getApi(googleProject, clusterName)
    }
    caught.message shouldBe s"""{"statusCode":422,"source":"leonardo","causes":[],"exceptionClass":"org.broadinstitute.dsde.workbench.leonardo.service.ClusterPausedException","stackTrace":[],"message":"Cluster ${googleProject.value}/${clusterName.string} is stopped. Start your cluster before proceeding."}"""
  }

  def startAndMonitor(googleProject: GoogleProject, clusterName: ClusterName)(implicit webDriver: WebDriver, token: AuthToken): Unit = {
    Leonardo.cluster.start(googleProject, clusterName) shouldBe
      "The request has been accepted for processing, but the processing has not been completed."

    // verify with get()
    val stoppingCluster = Leonardo.cluster.get(googleProject, clusterName)
    stoppingCluster.status shouldBe ClusterStatus.Starting

    // wait until in Running state
    implicit val patienceConfig: PatienceConfig = clusterPatience
    eventually {
      val status = Leonardo.cluster.get(googleProject, clusterName).status
      status shouldBe ClusterStatus.Running
    }

    // TODO cluster is not proxyable yet even after Google says it's ok
    eventually {
      logger.info("Checking if cluster is proxyable yet")
      val getResult = Try(Leonardo.notebooks.getApi(googleProject, clusterName))
      getResult.isSuccess shouldBe true
      getResult.get should not include "ProxyException"

    }(startPatience, implicitly[Position])

    logger.info("Sleeping 1 minute to make sure restarted cluster is proxyable")
    Thread.sleep(60*1000)
  }

  def randomClusterName: ClusterName = ClusterName(s"automation-test-a${makeRandomId().toLowerCase}z")

  def defaultClusterRequest: ClusterRequest = ClusterRequest(Map("foo" -> makeRandomId()))

  def createNewCluster(googleProject: GoogleProject, name: ClusterName = randomClusterName, request: ClusterRequest = defaultClusterRequest)(implicit token: AuthToken): Cluster = {
    val cluster = createAndMonitor(googleProject, name, request)
    cluster.status shouldBe ClusterStatus.Running
    cluster
  }

  def verifyNotebookCredentials(notebookPage: NotebookPage, expectedEmail: WorkbenchEmail): Unit = {
    // verify oauth2client
    notebookPage.executeCell("from oauth2client.client import GoogleCredentials") shouldBe None
    notebookPage.executeCell("credentials = GoogleCredentials.get_application_default()") shouldBe None
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
    // verify oauth2client
    notebookPage.executeCell("from oauth2client.client import GoogleCredentials") shouldBe None
    notebookPage.executeCell("credentials = GoogleCredentials.get_application_default()") shouldBe None
    notebookPage.executeCell("print credentials.service_account_email") shouldBe Some("None")

    // verify FISS
    notebookPage.executeCell("import firecloud.api as fapi") shouldBe None
    notebookPage.executeCell("fiss_credentials, project = fapi.google.auth.default()") shouldBe None
    notebookPage.executeCell("print fiss_credentials.service_account_email") shouldBe Some("default")

    // verify Spark
    notebookPage.executeCell("hadoop_config = sc._jsc.hadoopConfiguration()") shouldBe None
    notebookPage.executeCell("print hadoop_config.get('google.cloud.auth.service.account.enable')") shouldBe Some("None")
    notebookPage.executeCell("print hadoop_config.get('google.cloud.auth.service.account.json.keyfile')") shouldBe Some("None")
  }

  def getAndVerifyPet(project: GoogleProject)(implicit token: AuthToken): WorkbenchEmail = {
    val samPetEmail = Sam.user.petServiceAccountEmail(project.value)
    val userStatus = Sam.user.status().get
    implicit val patienceConfig: PatienceConfig = saPatience
    val googlePetEmail = googleIamDAO.findServiceAccount(project, samPetEmail).futureValue.map(_.email)
    googlePetEmail shouldBe Some(samPetEmail)
    samPetEmail
  }

  def withNewCluster[T](googleProject: GoogleProject, name: ClusterName = randomClusterName, request: ClusterRequest = defaultClusterRequest)(testCode: Cluster => T)(implicit token: AuthToken): T = {
    val cluster = createNewCluster(googleProject, name, request)
    val testResult: Try[T] = Try {
      testCode(cluster)
    }
    // delete before checking testCode status, which may throw
    deleteAndMonitor(googleProject, cluster.clusterName)
    testResult.get
  }

  def withNewErroredCluster[T](googleProject: GoogleProject)(testCode: Cluster => T)(implicit token: AuthToken): T = {
    val name = ClusterName(s"automation-test-a${makeRandomId()}z")
    val request = ClusterRequest(Map("foo" -> makeRandomId()), Some(incorrectJupyterExtensionUri))
    val testResult: Try[T] = Try {
      val cluster = createAndMonitor(googleProject, name, request)
      cluster.status shouldBe ClusterStatus.Error
      cluster.errors should have size 1
      cluster.errors.head.errorMessage should include ("gs://")
      cluster.errors.head.errorCode should be (3)
      testCode(cluster)
    }

    // delete before checking testCode status, which may throw
    deleteAndMonitor(googleProject, name)
    testResult.get
  }

  def withNotebooksListPage[T](cluster: Cluster)(testCode: NotebooksListPage => T)(implicit webDriver: WebDriver, token: AuthToken): T = {
    val notebooksListPage = Leonardo.notebooks.get(cluster.googleProject, cluster.clusterName)
    testCode(notebooksListPage.open)
  }

  def withFileUpload[T](cluster: Cluster, file: File)(testCode: NotebooksListPage => T)(implicit webDriver: WebDriver, token: AuthToken): T = {
    withNotebooksListPage(cluster) { notebooksListPage =>
      notebooksListPage.upload(file)
      testCode(notebooksListPage)
    }
  }

  def withNotebookUpload[T](cluster: Cluster, file: File)(testCode: NotebookPage => T)(implicit webDriver: WebDriver, token: AuthToken): T = {
    withFileUpload(cluster, file) { notebooksListPage =>
      notebooksListPage.withOpenNotebook(file) { notebookPage =>
        testCode(notebookPage)
      }
    }
  }

  def withNewNotebook[T](cluster: Cluster, kernel: Kernel = Python2)(testCode: NotebookPage => T)(implicit webDriver: WebDriver, token: AuthToken): T = {
    withNotebooksListPage(cluster) { notebooksListPage =>
      notebooksListPage.withNewNotebook(kernel) { notebookPage =>
        testCode(notebookPage)
      }
    }
  }

  def withOpenNotebook[T](cluster: Cluster, notebookPath: File)(testCode: NotebookPage => T)(implicit webDriver: WebDriver, token: AuthToken): T = {
    withNotebooksListPage(cluster) { notebooksListPage =>
      notebooksListPage.withOpenNotebook(notebookPath) { notebookPage =>
        testCode(notebookPage)
      }
    }
  }

  def withDummyClientPage[T](cluster: Cluster)(testCode: DummyClientPage => T)(implicit webDriver: WebDriver, token: AuthToken): T = {
    // start a server to load the dummy client page
    val bindingFuture = Leonardo.dummyClient.startServer
    val testResult = Try {
      val dummyClientPage = Leonardo.dummyClient.get(cluster.googleProject, cluster.clusterName)
      testCode(dummyClientPage)
    }
    // stop the server
    Leonardo.dummyClient.stopServer(bindingFuture)
    testResult.get
  }

  def withNewGoogleBucket[T](googleProject: GoogleProject, bucketName: GcsBucketName = generateUniqueBucketName("leo-auto"))(testCode: GcsBucketName => T): T = {
    implicit val patienceConfig: PatienceConfig = storagePatience

    // Create google bucket and run test code
    googleStorageDAO.createBucket(googleProject, bucketName).futureValue
    val testResult: Try[T] = Try {
      testCode(bucketName)
    }
    // Clean up
    googleStorageDAO.deleteBucket(bucketName, recurse = true).futureValue

    // Return the test result, or throw error
    testResult.get
  }

  def withNewBucketObject[T](bucketName: GcsBucketName, objectName: GcsObjectName, fileContents: String, objectType: String)(testCode: GcsObjectName => T): T = {
    implicit val patienceConfig: PatienceConfig = storagePatience

    // Create google bucket and run test code
    googleStorageDAO.storeObject(bucketName, objectName, new ByteArrayInputStream(fileContents.getBytes), objectType).futureValue
    val testResult: Try[T] = Try {
      testCode(objectName)
    }
    // Clean up
    googleStorageDAO.removeObject(bucketName, objectName).futureValue

    // Return the test result, or throw error
    testResult.get
  }

  def withLocalizeDelocalizeFiles[T](cluster: Cluster, localizeFileName: String, localizeFileContents: String, delocalizeFileName: String, delocalizeFileContents: String)(testCode: (Map[String, String], GcsBucketName) => T)(implicit webDriver: WebDriver, token: AuthToken): T = {
    implicit val patienceConfig: PatienceConfig = storagePatience

    withNewGoogleBucket(cluster.googleProject) { bucketName =>
      // give the user's pet owner access to the bucket
      val petServiceAccount = Sam.user.petServiceAccountEmail(cluster.googleProject.value)
      googleStorageDAO.setBucketAccessControl(bucketName, GcsEntity(petServiceAccount, GcsEntityTypes.User), GcsRoles.Owner).futureValue

      // create a bucket object to localize
      val bucketObjectToLocalize = GcsObjectName(localizeFileName)
      withNewBucketObject(bucketName, bucketObjectToLocalize, localizeFileContents, "text/plain") { objectName =>
        // give the user's pet read access to the object
        googleStorageDAO.setObjectAccessControl(bucketName, objectName, GcsEntity(petServiceAccount, GcsEntityTypes.User), GcsRoles.Owner).futureValue

        // create a notebook file to delocalize
        withNewNotebook(cluster) { notebookPage =>
          notebookPage.executeCell(s"""! echo -n "$delocalizeFileContents" > $delocalizeFileName""")

          val localizeRequest = Map(
            localizeFileName -> GcsPath(bucketName, bucketObjectToLocalize).toUri,
            GcsPath(bucketName, GcsObjectName(delocalizeFileName)).toUri -> delocalizeFileName
          )

          val testResult = Try(testCode(localizeRequest, bucketName))

          // clean up files on the cluster
          // no need to clean up the bucket objects; that will happen as part of `withNewBucketObject`
          notebookPage.executeCell(s"""! rm -f $localizeFileName""")
          notebookPage.executeCell(s"""! rm -f $delocalizeFileName""")

          testResult.get
        }
      }
    }
  }

  def verifyLocalizeDelocalize(cluster: Cluster, localizeFileName: String, localizeFileContents: String, delocalizeBucketPath: GcsPath, delocalizeBucketContents: String)(implicit token: AuthToken): Unit = {
    implicit val patienceConfig: PatienceConfig = storagePatience

    // check localization.log for existence
    val localizationLog = Leonardo.notebooks.getContentItem(cluster.googleProject, cluster.clusterName, "localization.log", includeContent = true)
    localizationLog.content shouldBe defined

    // Save localization.log to test output to aid in debugging
    val downloadFile = new File(logDir, s"${cluster.googleProject.value}-${cluster.clusterName.string}-localization.log")
    val fos = new FileOutputStream(downloadFile)
    fos.write(localizationLog.content.get.getBytes)
    fos.close()
    logger.info(s"Saved localization log for cluster ${cluster.googleProject.value}/${cluster.clusterName.string} to ${downloadFile.getAbsolutePath}")

    // the localized file should exist on the notebook VM
    val item = Leonardo.notebooks.getContentItem(cluster.googleProject, cluster.clusterName, localizeFileName, includeContent = true)
    item.content shouldBe Some(localizeFileContents)

    // the delocalized file should exist in the Google bucket
    val data = googleStorageDAO.getObject(delocalizeBucketPath.bucketName, delocalizeBucketPath.objectName).futureValue
    data.map(_.toString) shouldBe Some(delocalizeBucketContents)
  }

  def verifyHailImport(notebookPage: NotebookPage, vcfPath: GcsPath, clusterName: ClusterName): Unit = {
    val hailTimeout = 5 minutes
    val welcomeToHail =
      """Welcome to
        |     __  __     <>__
        |    / /_/ /__  __/ /
        |   / __  / _ `/ / /
        |  /_/ /_/\_,_/_/_/""".stripMargin

    val vcfSummary =
      """Samples: 1092
        |        Variants: 855026
        |       Call Rate: 0.983877
        |         Contigs: ['20']
        |   Multiallelics: 0
        |            SNPs: 824953
        |            MNPs: 0
        |      Insertions: 12227
        |       Deletions: 17798
        | Complex Alleles: 48
        |    Star Alleles: 0
        |     Max Alleles: 2""".stripMargin

    notebookPage.executeCell("from hail import *") shouldBe None
    notebookPage.executeCell("hc = HailContext(sc)").get should include(welcomeToHail)

    notebookPage.executeCell(s"chr20vcf = '${vcfPath.toUri}'") shouldBe None
    notebookPage.executeCell("imported = hc.import_vcf(chr20vcf)", hailTimeout).get should include("Hail: INFO: Coerced almost-sorted dataset")

    notebookPage.executeCell("imported.summarize().report()", hailTimeout).get should include(vcfSummary)

    // show that the Hail log contains jobs that were run on preemptible nodes

    val preemptibleNodePrefix = clusterName.string + "-sw"
    notebookPage.executeCell(s"! grep Finished ~/hail.log | grep $preemptibleNodePrefix").get should include(preemptibleNodePrefix)
  }

  def uploadDownloadTest(cluster: Cluster, uploadFile: File, timeout: FiniteDuration)(assertion: (File, File) => Any)(implicit webDriver: WebDriver, token: AuthToken): Any = {
    cluster.status shouldBe ClusterStatus.Running
    uploadFile.exists() shouldBe true

    withNotebookUpload(cluster, uploadFile) { notebook =>
      notebook.runAllCells(timeout.toSeconds)
      notebook.download()
    }

    // sanity check the file downloaded correctly
    val downloadFile = new File(downloadDir, uploadFile.getName)
    downloadFile.exists() shouldBe true
    downloadFile.isFile() shouldBe true
    math.abs(System.currentTimeMillis - downloadFile.lastModified()) shouldBe < (timeout.toMillis)

    // move the file to a unique location so it won't interfere with other tests
    val uniqueDownFile = new File(downloadDir, s"${Instant.now().toString}-${uploadFile.getName}")
    moveFile(downloadFile, uniqueDownFile)

    // clean up after ourselves
    uniqueDownFile.deleteOnExit()

    // run the assertion
    assertion(uploadFile, uniqueDownFile)
  }

  def saveDataprocLogFiles(cluster: Cluster)(implicit executionContext: ExecutionContext): Future[Option[(File, File)]] = {
    def downloadLogFile(contentStream: ByteArrayOutputStream, fileName: String): File = {
      // .log suffix is needed so it shows up as a Jenkins build artifact
      val downloadFile = new File(logDir, s"${cluster.googleProject.value}-${cluster.clusterName.string}-${fileName}.log")
      val fos = new FileOutputStream(downloadFile)
      fos.write(contentStream.toByteArray)
      fos.close()
      downloadFile
    }

    val transformed = for {
      stagingBucketName <- OptionT.fromOption[Future](cluster.stagingBucket)
      stagingBucketObjects <- OptionT.liftF[Future, List[GcsObjectName]](googleStorageDAO.listObjectsWithPrefix(stagingBucketName, "google-cloud-dataproc-metainfo"))
      initLogFile <- OptionT.fromOption[Future](stagingBucketObjects.filter(_.value.endsWith("dataproc-initialization-script-0_output")).headOption)
      initContent <- OptionT(googleStorageDAO.getObject(stagingBucketName, initLogFile))
      initDownloadFile <- OptionT.pure[Future, File](downloadLogFile(initContent, new File(initLogFile.value).getName))
      startupLogFile <- OptionT.fromOption[Future](stagingBucketObjects.filter(_.value.endsWith("dataproc-startup-script_output")).headOption)
      startupContent <- OptionT(googleStorageDAO.getObject(stagingBucketName, startupLogFile))
      startupDownloadFile <- OptionT.pure[Future, File](downloadLogFile(startupContent, new File(startupLogFile.value).getName))
    } yield (initDownloadFile, startupDownloadFile)

    transformed.value
  }

  def time[R](block: => R): TimeResult[R] = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    val timediff = FiniteDuration(t1 - t0, NANOSECONDS)
    TimeResult(result, timediff)
  }
}
