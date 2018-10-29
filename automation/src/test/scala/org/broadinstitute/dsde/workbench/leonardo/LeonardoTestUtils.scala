package org.broadinstitute.dsde.workbench.leonardo

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File, FileOutputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.Base64

import cats.data.OptionT
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.ResourceFile
import org.broadinstitute.dsde.workbench.dao.Google.{googleIamDAO, googleStorageDAO}
import org.broadinstitute.dsde.workbench.auth.{AuthToken, AuthTokenScopes, UserAuthToken}
import org.broadinstitute.dsde.workbench.config.Credentials
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.service.{Orchestration, RestException, Sam}
import org.broadinstitute.dsde.workbench.service.test.WebBrowserSpec
import org.broadinstitute.dsde.workbench.leonardo.ClusterStatus.{ClusterStatus, deletableStatuses}
import org.broadinstitute.dsde.workbench.leonardo.Leonardo.ApiVersion
import org.broadinstitute.dsde.workbench.leonardo.Leonardo.ApiVersion.{V1, V2}
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
import scala.util.{Failure, Random, Success, Try}


case class TimeResult[R](result:R, duration:FiniteDuration)

trait LeonardoTestUtils extends WebBrowserSpec with Matchers with Eventually with LocalFileUtil with LazyLogging with ScalaFutures {
  this: Suite with BillingFixtures =>

  val logDir = new File("output")
  logDir.mkdirs

  // Ron and Hermione are on the dev Leo whitelist, and Hermione is a Project Owner
  lazy val ronCreds: Credentials = LeonardoConfig.Users.NotebooksWhitelisted.getUserCredential("ron")
  lazy val hermioneCreds: Credentials = LeonardoConfig.Users.NotebooksWhitelisted.getUserCredential("hermione")
  lazy val voldyCreds: Credentials = LeonardoConfig.Users.CampaignManager.getUserCredential("voldemort")


  lazy val ronAuthToken = UserAuthToken(ronCreds, AuthTokenScopes.userLoginScopes)
  lazy val hermioneAuthToken = UserAuthToken(hermioneCreds, AuthTokenScopes.userLoginScopes)
  lazy val voldyAuthToken = UserAuthToken(voldyCreds, AuthTokenScopes.userLoginScopes)
  lazy val ronEmail = ronCreds.email

  val clusterPatience = PatienceConfig(timeout = scaled(Span(30, Minutes)), interval = scaled(Span(20, Seconds)))
  val localizePatience = PatienceConfig(timeout = scaled(Span(1, Minutes)), interval = scaled(Span(1, Seconds)))
  val saPatience = PatienceConfig(timeout = scaled(Span(1, Minutes)), interval = scaled(Span(1, Seconds)))
  val storagePatience = PatienceConfig(timeout = scaled(Span(1, Minutes)), interval = scaled(Span(1, Seconds)))
  val startPatience = PatienceConfig(timeout = scaled(Span(5, Minutes)), interval = scaled(Span(1, Seconds)))
  val getAfterCreatePatience = PatienceConfig(timeout = scaled(Span(30, Seconds)), interval = scaled(Span(2, Seconds)))

  val multiExtensionClusterRequest = UserJupyterExtensionConfig(
    nbExtensions = Map("map" -> "gmaps"),
    serverExtensions = Map("jupyterlab" -> "jupyterlab"),
    combinedExtensions = Map("pizza" -> "pizzabutton")
  )
  val jupyterLabExtensionClusterRequest = UserJupyterExtensionConfig(
    serverExtensions = Map("jupyterlab" -> "jupyterlab")
  )

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
    val jupyterExtensions = clusterRequest.userJupyterExtensionConfig match {
      case Some(x) => x.nbExtensions ++ x.combinedExtensions ++ x.serverExtensions
      case None => Map()
    }
    val expected = clusterRequest.labels ++ DefaultLabels(clusterName, googleProject, creator, Some(dummyClusterSa), Some(dummyNotebookSa), clusterRequest.jupyterExtensionUri, clusterRequest.jupyterUserScriptUri).toMap ++ jupyterExtensions

    (seen - "clusterServiceAccount" - "notebookServiceAccount") shouldBe (expected - "clusterServiceAccount" - "notebookServiceAccount")
  }

  def verifyCluster(cluster: Cluster,
                    expectedProject: GoogleProject,
                    expectedName: ClusterName,
                    expectedStatuses: Iterable[ClusterStatus],
                    clusterRequest: ClusterRequest,
                    bucketCheck: Boolean = true): Cluster = {

    // Always log cluster errors
    if (cluster.errors.nonEmpty) {
      logger.warn(s"Cluster ${cluster.projectNameString} returned the following errors: ${cluster.errors}")
    }

    withClue(s"Cluster ${cluster.projectNameString}: ") {
      expectedStatuses should contain (cluster.status)
    }

    cluster.googleProject shouldBe expectedProject
    cluster.clusterName shouldBe expectedName
    
    val expectedStopAfterCreation = clusterRequest.stopAfterCreation.getOrElse(false)
    cluster.stopAfterCreation shouldBe expectedStopAfterCreation

    labelCheck(cluster.labels, expectedName, expectedProject, cluster.creator, clusterRequest)

    if (bucketCheck) {
      cluster.stagingBucket shouldBe 'defined

      implicit val patienceConfig: PatienceConfig = storagePatience
      googleStorageDAO.bucketExists(GcsBucketName(cluster.stagingBucket.get.value)).futureValue shouldBe true
    }

    cluster
  }

  def createCluster(googleProject: GoogleProject,
                    clusterName: ClusterName,
                    clusterRequest: ClusterRequest,
                    monitor: Boolean,
                    apiVersion: ApiVersion = V1)
                   (implicit token: AuthToken): Cluster = {
    // Google doesn't seem to like simultaneous cluster creates.  Add 0-30 sec jitter
    Thread sleep Random.nextInt(30000)

    val clusterTimeResult = time(Leonardo.cluster.create(googleProject, clusterName, clusterRequest, apiVersion))
    logger.info(s"Time it took to get cluster create response with " +
      s"API version $apiVersion: ${clusterTimeResult.duration}")

    // We will verify the create cluster response.
    // We don't want to check bucket for v2 (async) cluster creation API
    // since that info won't be known at v2 API request completion time
    val bucketCheck = if (apiVersion == V2) false else true
    verifyCluster(clusterTimeResult.result, googleProject, clusterName,
      Seq(ClusterStatus.Creating), clusterRequest, bucketCheck)

    // verify with get()
    val creatingCluster = eventually {
      verifyCluster(Leonardo.cluster.get(googleProject, clusterName), googleProject, clusterName,
        Seq(ClusterStatus.Creating), clusterRequest)
    }(getAfterCreatePatience, implicitly[Position])

    if (monitor) {
      // wait for "Running", "Stopped", or error (fail fast)
      implicit val patienceConfig: PatienceConfig = clusterPatience

      val expectedStatuses =
        if (clusterRequest.stopAfterCreation.getOrElse(false)) {
          Seq(ClusterStatus.Stopped, ClusterStatus.Error)
        } else {
          Seq(ClusterStatus.Running, ClusterStatus.Error)
        }

      val runningOrErroredCluster = Try {
        eventually {
          verifyCluster(Leonardo.cluster.get(googleProject, clusterName), googleProject, clusterName,
            expectedStatuses, clusterRequest)
        }
      }

      // Save the cluster init log file whether or not the cluster created successfully
      implicit val ec: ExecutionContextExecutor = ExecutionContext.global
      saveDataprocLogFiles(creatingCluster).recover { case e =>
        logger.error(s"Error occurred saving Dataproc log files for cluster ${creatingCluster.projectNameString}", e)
        throw e
      }.futureValue match {
        case Some((initLog, startupLog)) =>
          logger.info(s"Saved Dataproc init log file for cluster ${creatingCluster.projectNameString} to ${initLog.getAbsolutePath}")
          logger.info(s"Saved Dataproc startup log file for cluster ${creatingCluster.projectNameString} to ${startupLog.getAbsolutePath}")
        case None =>
          logger.warn(s"Could not obtain Dataproc log files for cluster ${creatingCluster.projectNameString}")
      }

      // If the cluster is running, grab the jupyter.log file for debugging.
      runningOrErroredCluster.foreach { cluster =>
        if (cluster.status == ClusterStatus.Running) {
          saveJupyterLogFile(cluster.clusterName, cluster.googleProject, "create") match {
            case Success(file) =>
              logger.info(s"Saved jupyter.log file for cluster ${cluster.projectNameString} to ${file.getAbsolutePath}")
            case Failure(e) =>
              logger.warn(s"Could not save jupyter.log file for cluster ${cluster.projectNameString} . Not failing test.", e)
          }
        }
      }

      runningOrErroredCluster.get
    } else {
      creatingCluster
    }
  }

  // creates a cluster and checks to see that it reaches the Running state
  def createAndMonitor(googleProject: GoogleProject, clusterName: ClusterName, clusterRequest: ClusterRequest)(implicit token: AuthToken): Cluster = {
    createCluster(googleProject, clusterName, clusterRequest, monitor = true)
  }

  def deleteCluster(googleProject: GoogleProject, clusterName: ClusterName, monitor: Boolean)(implicit token: AuthToken): Unit = {
    saveJupyterLogFile(clusterName, googleProject, "delete") match {
      case Success(file) =>
        logger.info(s"Saved jupyter.log file for cluster ${googleProject.value}/${clusterName.string} to ${file.getAbsolutePath}")
      case Failure(e) =>
        logger.warn(s"Could not save jupyter.log file for cluster ${googleProject.value}/${clusterName.string} . Not failing test.", e)
    }
    try {
      Leonardo.cluster.delete(googleProject, clusterName) shouldBe
        "The request has been accepted for processing, but the processing has not been completed."
    } catch {
      // OK if cluster not found / already deleted
      case re: RestException if re.message.contains("\"statusCode\":404") => ()
      case e: Exception => throw e
    }

    if (monitor) {
      monitorDelete(googleProject, clusterName)
    }
  }

  def monitorDelete(googleProject: GoogleProject, clusterName: ClusterName)(implicit token: AuthToken): Unit = {
    // wait until not found or in "Deleted" state
    implicit val patienceConfig: PatienceConfig = clusterPatience
    eventually {
      val allStatus: Set[ClusterStatus] = Leonardo.cluster.listIncludingDeleted()
        .filter(c => c.clusterName == clusterName && c.googleProject == googleProject)
        .map(_.status)
        .toSet

      val isDeleted = if (allStatus.isEmpty || allStatus == Set(ClusterStatus.Deleted)) {
        logger.info(s"Cluster ${googleProject.value}/${clusterName.string} is deleted")
        true
      } else {
        logger.info(s"Cluster ${googleProject.value}/${clusterName.string} is not deleted yet")
        false
      }

      isDeleted shouldBe true
    }
  }

  // deletes a cluster and checks to see that it reaches the Deleted state
  def deleteAndMonitor(googleProject: GoogleProject, clusterName: ClusterName)(implicit token: AuthToken): Unit = {
    deleteCluster(googleProject, clusterName, monitor = true)
  }

  def stopCluster(googleProject: GoogleProject, clusterName: ClusterName, monitor: Boolean)(implicit token: AuthToken): Unit = {
    Leonardo.cluster.stop(googleProject, clusterName) shouldBe
      "The request has been accepted for processing, but the processing has not been completed."

    // verify with get()
    val stoppingCluster = Leonardo.cluster.get(googleProject, clusterName)
    stoppingCluster.status shouldBe ClusterStatus.Stopping

    if (monitor) {
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
  }

  def stopAndMonitor(googleProject: GoogleProject, clusterName: ClusterName)(implicit token: AuthToken): Unit = {
    stopCluster(googleProject, clusterName, monitor = true)(token)
  }

  def startCluster(googleProject: GoogleProject, clusterName: ClusterName, monitor: Boolean)(implicit token: AuthToken): Unit = {
    Leonardo.cluster.start(googleProject, clusterName)(token) shouldBe
      "The request has been accepted for processing, but the processing has not been completed."

    // verify with get()
    val startingCluster = Leonardo.cluster.get(googleProject, clusterName)
    startingCluster.status shouldBe ClusterStatus.Starting

    if (monitor) {
      // wait until in Running state
      implicit val patienceConfig: PatienceConfig = clusterPatience
      eventually {
        val status = Leonardo.cluster.get(googleProject, clusterName).status
        status shouldBe ClusterStatus.Running
      }

      logger.info(s"Checking if cluster is proxyable yet")
      val getResult = Try(Leonardo.notebooks.getApi(googleProject, clusterName))
      getResult.isSuccess shouldBe true
      getResult.get should not include "ProxyException"

      // Grab the jupyter.log file for debugging.
      saveJupyterLogFile(startingCluster.clusterName, startingCluster.googleProject, "start") match {
        case Success(file) =>
          logger.info(s"Saved jupyter.log file for cluster ${startingCluster.projectNameString} to ${file.getAbsolutePath}")
        case Failure(e) =>
          logger.warn(s"Could not save jupyter.log file for cluster ${startingCluster.projectNameString} . Not failing test.", e)
      }
    }
  }

  def startAndMonitor(googleProject: GoogleProject, clusterName: ClusterName)(implicit token: AuthToken): Unit = {
    startCluster(googleProject, clusterName, monitor = true)(token)
  }

  def randomClusterName: ClusterName = ClusterName(s"automation-test-a${makeRandomId().toLowerCase}z")

  def defaultClusterRequest: ClusterRequest = ClusterRequest(Map("foo" -> makeRandomId()))

  def createNewCluster(googleProject: GoogleProject,
                       name: ClusterName = randomClusterName,
                       request: ClusterRequest = defaultClusterRequest,
                       monitor: Boolean = true,
                       apiVersion: ApiVersion = V1)
                      (implicit token: AuthToken): Cluster = {

    val cluster = createCluster(googleProject, name, request, monitor, apiVersion)

    if (monitor) {
      withClue(s"Monitoring Cluster status: $name") {
        val clusterShouldBeStopped = request.stopAfterCreation.getOrElse(false)
        val expectedStatus = if (clusterShouldBeStopped) ClusterStatus.Stopped else ClusterStatus.Running

        cluster.status shouldBe expectedStatus
      }
    } else {
      cluster.status shouldBe ClusterStatus.Creating
    }

    cluster
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


  def getAndVerifyPet(project: GoogleProject)(implicit token: AuthToken): WorkbenchEmail = {
    val samPetEmail = Sam.user.petServiceAccountEmail(project.value)
    val userStatus = Sam.user.status().get
    implicit val patienceConfig: PatienceConfig = saPatience
    val googlePetEmail = googleIamDAO.findServiceAccount(project, samPetEmail).futureValue.map(_.email)
    googlePetEmail shouldBe Some(samPetEmail)
    samPetEmail
  }

  // Wrapper for BillingFixtures.withCleanBillingProject which sets up a project with Hermione as owner, and Ron as a user
  // testCode is curried so the token can be made implicit:
  // https://stackoverflow.com/questions/14072061/function-literal-with-multiple-implicit-arguments
  def withProject(testCode: GoogleProject => UserAuthToken => Any): Unit = {
    withCleanBillingProject(hermioneCreds) { projectName =>
      val project = GoogleProject(projectName)
      Orchestration.billing.addUserToBillingProject(projectName, ronEmail, Orchestration.billing.BillingProjectRole.User)(hermioneAuthToken)
      testCode(project)(ronAuthToken)
    }
  }

  def withNewCluster[T](googleProject: GoogleProject,
                        name: ClusterName = randomClusterName,
                        request: ClusterRequest = defaultClusterRequest,
                        monitorCreate: Boolean = true,
                        monitorDelete: Boolean = false,
                        apiVersion: ApiVersion = V1)
                       (testCode: Cluster => T)
                       (implicit token: AuthToken): T = {
    val cluster = createNewCluster(googleProject, name, request, monitorCreate, apiVersion)
    val testResult: Try[T] = Try {
      testCode(cluster)
    }

    // make sure cluster is deletable
    if (!monitorCreate){
      implicit val patienceConfig: PatienceConfig = clusterPatience

      eventually {
        verifyCluster(Leonardo.cluster.get(googleProject, name), googleProject, name,
          deletableStatuses, request)
      }
    }

    // delete before checking testCode status, which may throw
    deleteCluster(googleProject, cluster.clusterName, monitorDelete)
    testResult.get
  }

  def withNewErroredCluster[T](googleProject: GoogleProject)(testCode: Cluster => T)(implicit token: AuthToken): T = {
    val name = ClusterName(s"automation-test-a${makeRandomId()}z")
    // Fail a cluster by providing a user script which returns exit status 1
    val hailUploadFile = ResourceFile("bucket-tests/invalid_user_script.sh")
    withResourceFileInBucket(googleProject, hailUploadFile, "text/plain") { bucketPath =>
      val request = ClusterRequest(jupyterUserScriptUri = Some(bucketPath.toUri))
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

  def withNotebookUpload[T](cluster: Cluster, file: File, timeout: FiniteDuration = 2.minutes)(testCode: NotebookPage => T)(implicit webDriver: WebDriver, token: AuthToken): T = {
    withFileUpload(cluster, file) { notebooksListPage =>
      notebooksListPage.withOpenNotebook(file, timeout) { notebookPage =>
        testCode(notebookPage)
      }
    }
  }

  def withNewNotebook[T](cluster: Cluster, kernel: Kernel = Python2, timeout: FiniteDuration = 2.minutes)(testCode: NotebookPage => T)(implicit webDriver: WebDriver, token: AuthToken): T = {
    withNotebooksListPage(cluster) { notebooksListPage =>
      notebooksListPage.withNewNotebook(kernel, timeout) { notebookPage =>
        testCode(notebookPage)
      }
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
    withNewBucketObject(bucketName, objectName, new ByteArrayInputStream(fileContents.getBytes), objectType)(testCode)
  }

  def withNewBucketObject[T](bucketName: GcsBucketName, objectName: GcsObjectName, localFile: File, objectType: String)(testCode: GcsObjectName => T): T = {
    withNewBucketObject(bucketName, objectName, new ByteArrayInputStream(Files.readAllBytes(localFile.toPath)), objectType)(testCode)
  }

  def withNewBucketObject[T](bucketName: GcsBucketName, objectName: GcsObjectName, data: ByteArrayInputStream, objectType: String)(testCode: GcsObjectName => T): T = {
    implicit val patienceConfig: PatienceConfig = storagePatience

    // Create google bucket and run test code
    googleStorageDAO.storeObject(bucketName, objectName, data, objectType).futureValue
    val testResult: Try[T] = Try {
      testCode(objectName)
    }
    // Clean up
    googleStorageDAO.removeObject(bucketName, objectName).futureValue

    // Return the test result, or throw error
    testResult.get
  }

  def withResourceFileInBucket[T](googleProject: GoogleProject, resourceFile: ResourceFile, objectType: String)(testCode: GcsPath => T)(implicit token: AuthToken): T = {
    implicit val patienceConfig: PatienceConfig = storagePatience

    withNewGoogleBucket(googleProject) { bucketName =>
      // give the user's pet owner access to the bucket
      val petServiceAccount = Sam.user.petServiceAccountEmail(googleProject.value)
      googleStorageDAO.setBucketAccessControl(bucketName, EmailGcsEntity(GcsEntityTypes.User, petServiceAccount), GcsRoles.Owner).futureValue

      withNewBucketObject(bucketName, GcsObjectName(resourceFile.getName), resourceFile, objectType) { bucketObject =>
        // give the user's pet read access to the object
        googleStorageDAO.setObjectAccessControl(bucketName, bucketObject, EmailGcsEntity(GcsEntityTypes.User, petServiceAccount), GcsRoles.Reader).futureValue

        testCode(GcsPath(bucketName, bucketObject))
      }
    }
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
    val item = Leonardo.notebooks.getContentItem(cluster.googleProject, cluster.clusterName, localizedFileName, includeContent = true)
    item.content shouldBe Some(localizedFileContents)

    // the delocalized file should exist in the Google bucket
    val bucketData = googleStorageDAO.getObject(delocalizedBucketPath.bucketName, delocalizedBucketPath.objectName).futureValue
    bucketData.map(_.toString) shouldBe Some(delocalizedBucketContents)

    // the data file should exist on the notebook VM
    val dataItem = Leonardo.notebooks.getContentItem(cluster.googleProject, cluster.clusterName, dataFileName, includeContent = true)
    dataItem.content shouldBe Some(dataFileContents)
  }

  def verifyAndSaveLocalizationLog(cluster: Cluster)(implicit token: AuthToken): File = {
    // check localization.log for existence
    val localizationLog = Leonardo.notebooks.getContentItem(cluster.googleProject, cluster.clusterName, "localization.log", includeContent = true)
    localizationLog.content shouldBe defined

    // Save localization.log to test output to aid in debugging
    val downloadFile = new File(logDir, s"${cluster.googleProject.value}-${cluster.clusterName.string}-localization.log")
    val fos = new FileOutputStream(downloadFile)
    fos.write(localizationLog.content.get.getBytes)
    fos.close()

    downloadFile
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

  def saveDataprocLogFiles(cluster: Cluster)(implicit executionContext: ExecutionContext): Future[Option[(File, File)]] = {
    def downloadLogFile(contentStream: ByteArrayOutputStream, fileName: String): File = {
      // .log suffix is needed so it shows up as a Jenkins build artifact
      val downloadFile = new File(logDir, s"${cluster.googleProject.value}-${cluster.clusterName.string}-$fileName.log")
      val fos = new FileOutputStream(downloadFile)
      fos.write(contentStream.toByteArray)
      fos.close()
      downloadFile
    }

    val transformed = for {
      stagingBucketName <- OptionT.fromOption[Future](cluster.stagingBucket)
      stagingBucketObjects <- OptionT.liftF[Future, List[GcsObjectName]](googleStorageDAO.listObjectsWithPrefix(stagingBucketName, "google-cloud-dataproc-metainfo"))
      initLogFile <- OptionT.fromOption[Future](stagingBucketObjects.find(_.value.endsWith("dataproc-initialization-script-0_output")))
      initContent <- OptionT(googleStorageDAO.getObject(stagingBucketName, initLogFile))
      initDownloadFile <- OptionT.pure[Future, File](downloadLogFile(initContent, new File(initLogFile.value).getName))
      startupLogFile <- OptionT.fromOption[Future](stagingBucketObjects.find(_.value.endsWith("dataproc-startup-script_output")))
      startupContent <- OptionT(googleStorageDAO.getObject(stagingBucketName, startupLogFile))
      startupDownloadFile <- OptionT.pure[Future, File](downloadLogFile(startupContent, new File(startupLogFile.value).getName))
    } yield (initDownloadFile, startupDownloadFile)

    transformed.value
  }

  def saveJupyterLogFile(clusterName: ClusterName, googleProject: GoogleProject, suffix: String)(implicit token: AuthToken): Try[File] = {
    Try {
      val jupyterLogOpt = Leonardo.notebooks.getContentItem(googleProject, clusterName, "jupyter.log", includeContent = true)
      val content = jupyterLogOpt.content.getOrElse(throw new RuntimeException(s"Could not download jupyter.log for cluster ${googleProject.value}/${clusterName.string}"))
      val downloadFile = new File(logDir, s"${googleProject.value}-${clusterName.string}-$suffix-jupyter.log")
      val fos = new FileOutputStream(downloadFile)
      fos.write(content.getBytes(StandardCharsets.UTF_8))
      fos.close()
      downloadFile
    }
  }

  def time[R](block: => R): TimeResult[R] = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    val timediff = FiniteDuration(t1 - t0, NANOSECONDS)
    TimeResult(result, timediff)
  }

  def pipInstall(notebookPage: NotebookPage, kernel: Kernel, packageName: String): Unit = {
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
  def verifyTensorFlow(notebookPage: NotebookPage, kernel: Kernel): Unit = {
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

  def noop[A](x: A): Unit = ()

}
