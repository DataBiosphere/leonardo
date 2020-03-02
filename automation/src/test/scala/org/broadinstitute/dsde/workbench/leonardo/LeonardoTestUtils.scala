package org.broadinstitute.dsde.workbench.leonardo

import java.io.{ByteArrayInputStream, File, FileOutputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import akka.actor.ActorSystem
import cats.effect.concurrent.Semaphore
import cats.effect.{Blocker, IO}
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.dsde.workbench.ResourceFile
import org.broadinstitute.dsde.workbench.auth.{AuthToken, AuthTokenScopes, UserAuthToken}
import org.broadinstitute.dsde.workbench.config.Credentials
import org.broadinstitute.dsde.workbench.dao.Google.{googleIamDAO, googleStorageDAO}
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.broadinstitute.dsde.workbench.leonardo.ClusterStatus.{deletableStatuses, ClusterStatus}
import org.broadinstitute.dsde.workbench.leonardo.notebooks.Notebook
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.service.test.{RandomUtil, WebBrowserSpec}
import org.broadinstitute.dsde.workbench.service.{RestException, Sam}
import org.broadinstitute.dsde.workbench.util._
import org.scalactic.source.Position
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Minutes, Seconds, Span}
import org.scalatest.{Matchers, Suite}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Failure, Random, Success, Try}

case class KernelNotReadyException(timeElapsed: Timeout)
    extends Exception(s"Jupyter kernel is NOT ready after waiting ${timeElapsed}")

case class TimeResult[R](result: R, duration: FiniteDuration)

trait LeonardoTestUtils
    extends WebBrowserSpec
    with Matchers
    with Eventually
    with LocalFileUtil
    with LazyLogging
    with ScalaFutures
    with Retry
    with RandomUtil {
  this: Suite =>

  val system: ActorSystem = ActorSystem("leotests")
  val logDir = new File("output")
  logDir.mkdirs

  def enableWelder: Boolean = true

  // Ron and Hermione are on the dev Leo whitelist, and Hermione is a Project Owner
  lazy val ronCreds: Credentials = LeonardoConfig.Users.NotebooksWhitelisted.getUserCredential("ron")
  lazy val hermioneCreds: Credentials = LeonardoConfig.Users.NotebooksWhitelisted.getUserCredential("hermione")
  lazy val voldyCreds: Credentials = LeonardoConfig.Users.CampaignManager.getUserCredential("voldemort")

  lazy val ronAuthToken = UserAuthToken(ronCreds, AuthTokenScopes.userLoginScopes)
  lazy val hermioneAuthToken = UserAuthToken(hermioneCreds, AuthTokenScopes.userLoginScopes)
  lazy val voldyAuthToken = UserAuthToken(voldyCreds, AuthTokenScopes.userLoginScopes)
  lazy val ronEmail = ronCreds.email

  val clusterPatience = PatienceConfig(timeout = scaled(Span(15, Minutes)), interval = scaled(Span(20, Seconds)))
  val clusterStopAfterCreatePatience =
    PatienceConfig(timeout = scaled(Span(30, Minutes)), interval = scaled(Span(20, Seconds)))
  val localizePatience = PatienceConfig(timeout = scaled(Span(1, Minutes)), interval = scaled(Span(1, Seconds)))
  val saPatience = PatienceConfig(timeout = scaled(Span(1, Minutes)), interval = scaled(Span(1, Seconds)))
  val storagePatience = PatienceConfig(timeout = scaled(Span(1, Minutes)), interval = scaled(Span(1, Seconds)))
  val startPatience = PatienceConfig(timeout = scaled(Span(5, Minutes)), interval = scaled(Span(1, Seconds)))
  val getAfterCreatePatience = PatienceConfig(timeout = scaled(Span(5, Minutes)), interval = scaled(Span(2, Seconds)))

  val multiExtensionClusterRequest = UserJupyterExtensionConfig(
    nbExtensions = Map("map" -> "gmaps"),
    combinedExtensions = Map("pizza" -> "pizzabutton")
  )
  val jupyterLabExtensionClusterRequest = UserJupyterExtensionConfig(
    serverExtensions = Map("jupyterlab" -> "jupyterlab")
  )

  implicit val cs = IO.contextShift(global)
  implicit val t = IO.timer(global)
  implicit def unsafeLogger = Slf4jLogger.getLogger[IO]
  val blocker = Blocker.liftExecutionContext(global)
  val google2StorageResource = GoogleStorageService.resource[IO](LeonardoConfig.GCS.pathToQAJson, blocker)
  val concurrentClusterCreationPermits
    : Semaphore[IO] = Semaphore[IO](5).unsafeRunSync() //Since we're using the same google project, we can reach bucket creation quota limit

  // TODO: move this to NotebookTestUtils and chance cluster-specific functions to only call if necessary after implementing RStudio
  def saveClusterLogFiles(googleProject: GoogleProject, clusterName: RuntimeName, paths: List[String], suffix: String)(
    implicit token: AuthToken
  ): Unit = {
    val fileResult = paths.traverse[Try, File] { path =>
      Try {
        val contentItem = Notebook.getContentItem(googleProject, clusterName, path, includeContent = true)
        val content = contentItem.content.getOrElse(
          throw new RuntimeException(
            s"Could not download ${path} for cluster ${googleProject.value}/${clusterName.asString}"
          )
        )
        val downloadFile = new File(logDir, s"${googleProject.value}-${clusterName.asString}-$suffix-${path}")
        val fos = new FileOutputStream(downloadFile)
        fos.write(content.getBytes(StandardCharsets.UTF_8))
        fos.close()
        downloadFile
      }
    }
    fileResult match {
      case Success(files) =>
        logger.info(
          s"Saved files [${files.map(_.getName).mkString(", ")}] for cluster ${googleProject.value}/${clusterName.asString}"
        )
      case Failure(e) =>
        logger.warn(
          s"Could not save files for cluster ${googleProject.value}/${clusterName.asString} . Not failing test.",
          e
        )
    }
  }

  // TODO: show diffs as screenshot or other test output?
  def compareFilesExcludingIPs(left: File, right: File): Unit = {

    def linesWithoutIPs(file: File) = {
      import scala.collection.JavaConverters._
      Files.readAllLines(file.toPath).asScala map { _.replaceAll("(\\d+.){3}\\d+", "<IP>") }
    }

    linesWithoutIPs(left) shouldEqual linesWithoutIPs(right)
  }

  def getExpectedToolLabel(imageUrl: String): String =
    if (imageUrl == LeonardoConfig.Leonardo.rstudioBaseImageUrl) "RStudio"
    else "Jupyter"

  def labelCheck(seen: LabelMap,
                 clusterName: RuntimeName,
                 googleProject: GoogleProject,
                 creator: WorkbenchEmail,
                 clusterRequest: ClusterRequest): Unit = {

    // the SAs can vary here depending on which ServiceAccountProvider is used
    // set dummy values here and then remove them from the comparison
    // TODO: check for these values after tests are agnostic to ServiceAccountProvider ?

    val dummyClusterSa = WorkbenchEmail("dummy-cluster")
    val dummyNotebookSa = WorkbenchEmail("dummy-notebook")
    val jupyterExtensions = clusterRequest.userJupyterExtensionConfig match {
      case Some(x) => x.nbExtensions ++ x.combinedExtensions ++ x.serverExtensions ++ x.labExtensions
      case None    => Map()
    }
    val expected = clusterRequest.labels ++ DefaultLabelsCopy(
      clusterName,
      googleProject,
      creator,
      Some(dummyClusterSa),
      Some(dummyNotebookSa),
      clusterRequest.jupyterExtensionUri,
      clusterRequest.jupyterUserScriptUri,
      clusterRequest.jupyterStartUserScriptUri,
      clusterRequest.toolDockerImage.map(getExpectedToolLabel).getOrElse("Jupyter")
    ).toMap ++ jupyterExtensions

    (seen - "clusterServiceAccount" - "notebookServiceAccount") shouldBe (expected - "clusterServiceAccount" - "notebookServiceAccount")
  }

  def verifyCluster(cluster: ClusterCopy,
                    expectedProject: GoogleProject,
                    expectedName: RuntimeName,
                    expectedStatuses: Iterable[ClusterStatus],
                    clusterRequest: ClusterRequest,
                    bucketCheck: Boolean = true): ClusterCopy = {
    // Always log cluster errors
    if (cluster.errors.nonEmpty) {
      logger.warn(s"ClusterCopy ${cluster.projectNameString} returned the following errors: ${cluster.errors}")
    }
    withClue(s"ClusterCopy ${cluster.projectNameString}: ") {
      expectedStatuses should contain(cluster.status)
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
                    clusterName: RuntimeName,
                    clusterRequest: ClusterRequest,
                    monitor: Boolean)(implicit token: AuthToken): ClusterCopy = {
    // Google doesn't seem to like simultaneous cluster creates.  Add 0-30 sec jitter
    Thread sleep Random.nextInt(30000)

    val clusterTimeResult = time(
      concurrentClusterCreationPermits
        .withPermit(IO(Leonardo.cluster.create(googleProject, clusterName, clusterRequest)))
        .unsafeRunSync()
    )
    logger.info(s"Time it took to get cluster create response with: ${clusterTimeResult.duration}")

    // We will verify the create cluster response.
    verifyCluster(clusterTimeResult.result,
                  googleProject,
                  clusterName,
                  List(ClusterStatus.Creating),
                  clusterRequest,
                  false)

    // verify with get()
    val creatingCluster = eventually {
      verifyCluster(Leonardo.cluster.get(googleProject, clusterName),
                    googleProject,
                    clusterName,
                    List(ClusterStatus.Creating),
                    clusterRequest)
    }(getAfterCreatePatience, implicitly[Position])

    if (monitor) {
      monitorCreate(googleProject, clusterName, clusterRequest, creatingCluster)
    } else {
      creatingCluster
    }
  }

  def monitorCreate(googleProject: GoogleProject,
                    clusterName: RuntimeName,
                    clusterRequest: ClusterRequest,
                    creatingCluster: ClusterCopy)(implicit token: AuthToken): ClusterCopy = {
    // wait for "Running", "Stopped", or error (fail fast)
    val stopAfterCreate = clusterRequest.stopAfterCreation.getOrElse(false)

    implicit val patienceConfig: PatienceConfig =
      if (stopAfterCreate) clusterStopAfterCreatePatience else clusterPatience

    val expectedStatuses =
      if (stopAfterCreate) {
        List(ClusterStatus.Stopped, ClusterStatus.Error)
      } else {
        List(ClusterStatus.Running, ClusterStatus.Error)
      }

    val runningOrErroredCluster = Try {
      eventually {
        val cluster = Leonardo.cluster.get(googleProject, clusterName)
        verifyCluster(cluster, googleProject, clusterName, expectedStatuses, clusterRequest, true)
      }
    }
    // Save the cluster init log file whether or not the cluster created successfully
    saveDataprocLogFiles(creatingCluster).timeout(5.minutes).unsafeRunSync()

    // If the cluster is running, grab the jupyter.log and welder.log files for debugging.
    runningOrErroredCluster.foreach { cluster =>
      if (cluster.status == ClusterStatus.Running) {
        saveClusterLogFiles(cluster.googleProject, cluster.clusterName, List("jupyter.log", "welder.log"), "create")
      }
    }

    runningOrErroredCluster.get
  }

  // creates a cluster and checks to see that it reaches the Running state
  def createAndMonitor(googleProject: GoogleProject, clusterName: RuntimeName, clusterRequest: ClusterRequest)(
    implicit token: AuthToken
  ): ClusterCopy =
    createCluster(googleProject, clusterName, clusterRequest, monitor = true)

  def deleteCluster(googleProject: GoogleProject, clusterName: RuntimeName, monitor: Boolean)(
    implicit token: AuthToken
  ): Unit = {
    //we cannot save the log if the cluster isn't running
    if (Leonardo.cluster.get(googleProject, clusterName).status == ClusterStatus.Running) {
      saveClusterLogFiles(googleProject, clusterName, List("jupyter.log", "welder.log"), "delete")
    }
    try {
      Leonardo.cluster.delete(googleProject, clusterName) shouldBe
        "The request has been accepted for processing, but the processing has not been completed."
    } catch {
      // OK if cluster not found / already deleted
      case re: RestException if re.message.contains("\"statusCode\":404") => ()
      case e: Exception                                                   => throw e
    }

    if (monitor) {
      monitorDelete(googleProject, clusterName)
    }
  }

  def monitorDelete(googleProject: GoogleProject, clusterName: RuntimeName)(implicit token: AuthToken): Unit = {
    // wait until not found or in "Deleted" state
    implicit val patienceConfig: PatienceConfig = clusterPatience
    eventually {
      val allStatus: Set[ClusterStatus] = Leonardo.cluster
        .listIncludingDeleted(googleProject)
        .filter(c => c.clusterName == clusterName && c.googleProject == googleProject)
        .map(_.status)
        .toSet

      val isDeleted = if (allStatus.isEmpty || allStatus == Set(ClusterStatus.Deleted)) {
        logger.info(s"ClusterCopy ${googleProject.value}/${clusterName.asString} is deleted")
        true
      } else {
        logger.info(s"ClusterCopy ${googleProject.value}/${clusterName.asString} is not deleted yet")
        false
      }

      isDeleted shouldBe true
    }
  }

  // deletes a cluster and checks to see that it reaches the Deleted state
  def deleteAndMonitor(googleProject: GoogleProject, clusterName: RuntimeName)(implicit token: AuthToken): Unit =
    deleteCluster(googleProject, clusterName, monitor = true)

  def stopCluster(googleProject: GoogleProject, clusterName: RuntimeName, monitor: Boolean)(
    implicit token: AuthToken
  ): Unit = {
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
      val caught = the[RestException] thrownBy {
        Notebook.getTree(googleProject, clusterName)
      }

      caught.message should include("\"statusCode\":422")
      caught.message should include(
        s"""ClusterCopy ${googleProject.value}/${clusterName.asString} is stopped. Start your cluster before proceeding."""
      )
    }
  }

  def stopAndMonitor(googleProject: GoogleProject, clusterName: RuntimeName)(implicit token: AuthToken): Unit =
    stopCluster(googleProject, clusterName, monitor = true)(token)

  def startCluster(googleProject: GoogleProject, clusterName: RuntimeName, monitor: Boolean)(
    implicit token: AuthToken
  ): Unit = {
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
      val getResult = Try(Notebook.getApi(googleProject, clusterName))
      getResult.isSuccess shouldBe true
      getResult.get should not include "ProxyException"

      // Grab the jupyter.log and welder.log files for debugging.
      saveClusterLogFiles(googleProject, clusterName, List("jupyter.log", "welder.log"), "start")
    }
  }

  def startAndMonitor(googleProject: GoogleProject, clusterName: RuntimeName)(implicit token: AuthToken): Unit =
    startCluster(googleProject, clusterName, monitor = true)(token)

  def randomClusterName: RuntimeName = RuntimeName(s"automation-test-a${makeRandomId().toLowerCase}z")

  def defaultClusterRequest: ClusterRequest =
    ClusterRequest(Map("foo" -> makeRandomId()),
                   enableWelder = Some(enableWelder),
                   toolDockerImage = Some(LeonardoConfig.Leonardo.baseImageUrl))

  def createNewCluster(googleProject: GoogleProject,
                       name: RuntimeName = randomClusterName,
                       request: ClusterRequest = defaultClusterRequest,
                       monitor: Boolean = true)(implicit token: AuthToken): ClusterCopy = {

    val cluster = createCluster(googleProject, name, request, monitor)

    if (monitor) {
      withClue(s"Monitoring ClusterCopy status: $name") {
        val clusterShouldBeStopped = request.stopAfterCreation.getOrElse(false)
        val expectedStatus = if (clusterShouldBeStopped) ClusterStatus.Stopped else ClusterStatus.Running

        cluster.status shouldBe expectedStatus
      }
    } else {
      cluster.status shouldBe ClusterStatus.Creating
    }

    cluster
  }

  def getAndVerifyPet(project: GoogleProject)(implicit token: AuthToken): WorkbenchEmail = {
    val samPetEmail = Sam.user.petServiceAccountEmail(project.value)
    val userStatus = Sam.user.status().get
    implicit val patienceConfig: PatienceConfig = saPatience
    val googlePetEmail = googleIamDAO.findServiceAccount(project, samPetEmail).futureValue.map(_.email)
    googlePetEmail shouldBe Some(samPetEmail)
    samPetEmail
  }

  def withNewCluster[T](googleProject: GoogleProject,
                        name: RuntimeName = randomClusterName,
                        request: ClusterRequest = defaultClusterRequest,
                        monitorCreate: Boolean = true,
                        monitorDelete: Boolean = false)(testCode: ClusterCopy => T)(implicit token: AuthToken): T = {
    val cluster = createNewCluster(googleProject, name, request, monitorCreate)
    val testResult: Try[T] = Try {
      testCode(cluster)
    }

    // make sure cluster is deletable
    if (!monitorCreate) {
      implicit val patienceConfig: PatienceConfig = clusterPatience

      eventually {
        verifyCluster(Leonardo.cluster.get(googleProject, name), googleProject, name, deletableStatuses, request)
      }
    }

    // delete before checking testCode status, which may throw
    deleteCluster(googleProject, cluster.clusterName, monitorDelete)
    testResult.get
  }

  def withNewErroredCluster[T](
    googleProject: GoogleProject
  )(testCode: ClusterCopy => T)(implicit token: AuthToken): T = {
    val name = RuntimeName(s"automation-test-a${makeRandomId()}z")
    // Fail a cluster by providing a user script which returns exit status 1
    val hailUploadFile = ResourceFile("bucket-tests/invalid_user_script.sh")
    withResourceFileInBucket(googleProject, hailUploadFile, "text/plain") { bucketPath =>
      val request = ClusterRequest(jupyterUserScriptUri = Some(bucketPath.toUri))
      val testResult: Try[T] = Try {
        val cluster = createAndMonitor(googleProject, name, request)
        cluster.status shouldBe ClusterStatus.Error
        cluster.errors should have size 1
        cluster.errors.head.errorMessage should include("gs://")
        cluster.errors.head.errorMessage should include("Userscript failed.")
        cluster.errors.head.errorCode should be(3)
        testCode(cluster)
      }

      // delete before checking testCode status, which may throw
      deleteCluster(googleProject, name, false)
      testResult.get
    }
  }

  def withRestartCluster[T](cluster: ClusterCopy)(testCode: ClusterCopy => T)(implicit token: AuthToken): T = {
    stopAndMonitor(cluster.googleProject, cluster.clusterName)
    val resolvedCluster = Leonardo.cluster.get(cluster.googleProject, cluster.clusterName)
    resolvedCluster.status shouldBe ClusterStatus.Stopped
    val testResult = Try {
      testCode(resolvedCluster)
    }
    startAndMonitor(cluster.googleProject, cluster.clusterName)
    testResult.get
  }

  def withNewGoogleBucket[T](
    googleProject: GoogleProject,
    bucketName: GcsBucketName = generateUniqueBucketName("leo-auto")
  )(testCode: GcsBucketName => T): T = {
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

  def withNewBucketObject[T](bucketName: GcsBucketName,
                             objectName: GcsObjectName,
                             fileContents: String,
                             objectType: String)(testCode: GcsObjectName => T): T =
    withNewBucketObject(bucketName, objectName, new ByteArrayInputStream(fileContents.getBytes), objectType)(testCode)

  def withNewBucketObject[T](bucketName: GcsBucketName, objectName: GcsObjectName, localFile: File, objectType: String)(
    testCode: GcsObjectName => T
  ): T =
    withNewBucketObject(bucketName,
                        objectName,
                        new ByteArrayInputStream(Files.readAllBytes(localFile.toPath)),
                        objectType)(testCode)

  def withNewBucketObject[T](bucketName: GcsBucketName,
                             objectName: GcsObjectName,
                             data: ByteArrayInputStream,
                             objectType: String)(testCode: GcsObjectName => T): T = {
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

  def withResourceFileInBucket[T](googleProject: GoogleProject, resourceFile: ResourceFile, objectType: String)(
    testCode: GcsPath => T
  )(implicit token: AuthToken): T = {
    implicit val patienceConfig: PatienceConfig = storagePatience

    withNewGoogleBucket(googleProject) { bucketName =>
      // give the user's pet owner access to the bucket
      val petServiceAccount = Sam.user.petServiceAccountEmail(googleProject.value)
      googleStorageDAO
        .setBucketAccessControl(bucketName, EmailGcsEntity(GcsEntityTypes.User, petServiceAccount), GcsRoles.Owner)
        .futureValue

      withNewBucketObject(bucketName, GcsObjectName(resourceFile.getName), resourceFile, objectType) { bucketObject =>
        // give the user's pet read access to the object
        googleStorageDAO
          .setObjectAccessControl(bucketName,
                                  bucketObject,
                                  EmailGcsEntity(GcsEntityTypes.User, petServiceAccount),
                                  GcsRoles.Reader)
          .futureValue

        testCode(GcsPath(bucketName, bucketObject))
      }
    }
  }

  def saveDataprocLogFiles(cluster: ClusterCopy): IO[Unit] =
    google2StorageResource.use { storage =>
      cluster.stagingBucket
        .traverse { stagingBucketName =>
          val downloadLogs = for {
            blob <- storage
              .listBlobsWithPrefix(stagingBucketName, "google-cloud-dataproc-metainfo", true)
              .filter(_.getName.endsWith("output"))
            blobName = blob.getName
            shortName = new File(blobName).getName
            path = new File(logDir, s"${cluster.googleProject.value}-${cluster.clusterName.asString}-${shortName}.log").toPath
            _ <- storage.downloadObject(blob.getBlobId, path)
          } yield shortName

          downloadLogs.compile.toList
        }
        .flatMap {
          case None => IO(logger.error(s"ClusterCopy ${cluster.projectNameString} does not have a staging bucket"))
          case Some(logs) if logs.isEmpty =>
            IO(logger.warn(s"Unable to find output logs for cluster ${cluster.projectNameString}"))
          case Some(logs) =>
            IO(logger.info(s"Downloaded output logs for cluster ${cluster.projectNameString}: ${logs.mkString(",")}"))
        }
    }

  def time[R](block: => R): TimeResult[R] = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    val timediff = FiniteDuration(t1 - t0, NANOSECONDS)
    TimeResult(result, timediff)
  }

  def noop[A](x: A): Unit = ()

}
