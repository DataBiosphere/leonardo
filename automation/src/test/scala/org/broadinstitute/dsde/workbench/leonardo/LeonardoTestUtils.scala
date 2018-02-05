package org.broadinstitute.dsde.workbench.leonardo

import java.io.File
import java.nio.file.Files

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.dao.Google.{googleIamDAO, googleStorageDAO}
import org.broadinstitute.dsde.workbench.auth.{AuthToken, UserAuthToken}
import org.broadinstitute.dsde.workbench.config.{Config, Credentials}
import org.broadinstitute.dsde.workbench.service.{Orchestration, Rawls, Sam}
import org.broadinstitute.dsde.workbench.service.APIException
import org.broadinstitute.dsde.workbench.service.test.WebBrowserSpec
import org.broadinstitute.dsde.workbench.leonardo.ClusterStatus.ClusterStatus
import org.broadinstitute.dsde.workbench.leonardo.StringValueClass.LabelMap
import org.broadinstitute.dsde.workbench.model.{WorkbenchEmail, google}
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject, ServiceAccountName, generateUniqueBucketName}
import org.broadinstitute.dsde.workbench.util.LocalFileUtil
import org.openqa.selenium.WebDriver
import org.scalatest.{Matchers, Suite}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Minutes, Seconds, Span}
import org.broadinstitute.dsde.workbench.dao.Google.googleStorageDAO
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName

import scala.util.{Random, Try}
import scala.util.control.NonFatal

trait LeonardoTestUtils extends WebBrowserSpec with Matchers with Eventually with LocalFileUtil with LazyLogging with ScalaFutures {
  this: Suite =>

  val swatTestBucket = "gs://leonardo-swat-test-bucket-do-not-delete"
  val incorrectJupyterExtensionUri = swatTestBucket + "/"

  // Ron and Hermione are on the dev Leo whitelist, and Hermione is a Project Owner
  lazy val ronCreds: Credentials = Config.Users.NotebooksWhitelisted.getUserCredential("ron")
  lazy val hermioneCreds: Credentials = Config.Users.NotebooksWhitelisted.getUserCredential("hermione")

  lazy val ronAuthToken = UserAuthToken(ronCreds)
  lazy val hermioneAuthToken = UserAuthToken(hermioneCreds)
  lazy val ronEmail = ronCreds.email

  val clusterPatience = PatienceConfig(timeout = scaled(Span(15, Minutes)), interval = scaled(Span(20, Seconds)))
  val localizePatience = PatienceConfig(timeout = scaled(Span(1, Minutes)), interval = scaled(Span(1, Seconds)))
  val saPatience = PatienceConfig(timeout = scaled(Span(1, Minutes)), interval = scaled(Span(1, Seconds)))
  val storagePatience = PatienceConfig(timeout = scaled(Span(1, Minutes)), interval = scaled(Span(1, Seconds)))

  // TODO: show diffs as screenshot or other test output?
  def compareFilesExcludingIPs(left: File, right: File): Unit = {

    def linesWithoutIPs(file: File) = {
      import scala.collection.JavaConverters._
      Files.readAllLines(left.toPath).asScala map { _.replaceAll("(\\d+.){3}\\d+", "<IP>") }
    }

    linesWithoutIPs(left) shouldEqual linesWithoutIPs(right)
  }

  def createNewBillingProject(): GoogleProject = {
    val ownerToken: AuthToken = hermioneAuthToken
    val billingProject = "leonardo-billing-spec-" + makeRandomId()

    logger.info(s"Creating billing project: $billingProject")
    Orchestration.billing.createBillingProject(billingProject, Config.Projects.billingAccountId)(ownerToken)

    GoogleProject(billingProject)
  }

  def cleanupBillingProject(billingProject: GoogleProject): Unit = {
    Try(Rawls.admin.deleteBillingProject(billingProject.value)(UserAuthToken(Config.Users.Admins.getUserCredential("dumbledore")))).recover { case NonFatal(e) =>
      logger.warn(s"Could not delete billing project $billingProject", e)
    }
  }

  def withNewBillingProject[T](testCode: GoogleProject => T): T = {
    // Create billing project and run test code
    val billingProject = createNewBillingProject()
    val testResult: Try[T] = Try {
      testCode(billingProject)
    }
    // Clean up billing project
    cleanupBillingProject(billingProject)

    // Return the test result, or throw error
    testResult.get
  }

  def labelCheck(seen: LabelMap,
                 requestedLabels: LabelMap,
                 clusterName: ClusterName,
                 googleProject: GoogleProject,
                 creator: WorkbenchEmail,
                 notebookExtension: Option[String] = None): Unit = {

    // we don't actually know the SA because it's the pet
    // set a dummy here and then remove it from the comparison

    val dummyPetSa = WorkbenchEmail("dummy")
    val expected = requestedLabels ++ DefaultLabels(clusterName, googleProject, creator, Some(dummyPetSa), None, notebookExtension).toMap

    (seen - "clusterServiceAccount") shouldBe (expected - "clusterServiceAccount")
  }

  def clusterCheck(cluster: Cluster,
                   requestedLabels: Map[String, String],
                   expectedProject: GoogleProject,
                   expectedName: ClusterName,
                   expectedStatuses: Iterable[ClusterStatus],
                   notebookExtension: Option[String] = None): Cluster = {

    expectedStatuses should contain (cluster.status)
    cluster.googleProject shouldBe expectedProject
    cluster.clusterName shouldBe expectedName
    cluster.stagingBucket shouldBe 'defined

    implicit val patienceConfig: PatienceConfig = storagePatience
    googleStorageDAO.bucketExists(google.GcsBucketName(cluster.stagingBucket.get.value)).futureValue shouldBe true
    labelCheck(cluster.labels, requestedLabels, expectedName, expectedProject, cluster.creator, notebookExtension)
    cluster
  }

  // creates a cluster and checks to see that it reaches the Running state
  def createAndMonitor(googleProject: GoogleProject, clusterName: ClusterName, clusterRequest: ClusterRequest)(implicit token: AuthToken): Cluster = {
    // Google doesn't seem to like simultaneous cluster creates.  Add 0-30 sec jitter
    Thread sleep Random.nextInt(30000)

    val cluster = Leonardo.cluster.create(googleProject, clusterName, clusterRequest)
    clusterCheck(cluster, clusterRequest.labels, googleProject, clusterName, Seq(ClusterStatus.Creating), clusterRequest.jupyterExtensionUri)

    // verify with get()
    clusterCheck(Leonardo.cluster.get(googleProject, clusterName), clusterRequest.labels, googleProject, clusterName, Seq(ClusterStatus.Creating), clusterRequest.jupyterExtensionUri)

    // wait for "Running" or error (fail fast)
    implicit val patienceConfig: PatienceConfig = clusterPatience
    val actualCluster = eventually {
      clusterCheck(Leonardo.cluster.get(googleProject, clusterName), clusterRequest.labels, googleProject, clusterName, Seq(ClusterStatus.Running, ClusterStatus.Error), clusterRequest.jupyterExtensionUri)
    }

    actualCluster
  }

  // deletes a cluster and checks to see that it reaches the Deleted state
  def deleteAndMonitor(googleProject: GoogleProject, clusterName: ClusterName)(implicit token: AuthToken): Unit = {
    try {
      Leonardo.cluster.delete(googleProject, clusterName) shouldBe
        "The request has been accepted for processing, but the processing has not been completed."
    } catch {
      // OK if cluster not found / already deleted
      case ae: APIException if ae.message.contains("\"statusCode\":404") => ()
      case e: Exception => throw e
    }

    // wait until not found or in "Deleted" state
    implicit val patienceConfig: PatienceConfig = clusterPatience
    eventually {
      val statusOpt = Leonardo.cluster.listIncludingDeleted().find(_.clusterName == clusterName).map(_.status)
      statusOpt getOrElse ClusterStatus.Deleted shouldBe ClusterStatus.Deleted
    }
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

  def getAndVerifyPet(project: GoogleProject)(implicit token: AuthToken): (ServiceAccountName, WorkbenchEmail) = {
    val samPetEmail = Sam.user.petServiceAccountEmail(project.value)
    val userStatus = Sam.user.status().get
    val petName = Sam.petName(userStatus.userInfo)
    implicit val patienceConfig: PatienceConfig = saPatience
    val googlePetEmail = googleIamDAO.findServiceAccount(project, petName).futureValue.map(_.email)
    googlePetEmail shouldBe Some(samPetEmail)
    (petName, samPetEmail)
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

  def withNewNotebook[T](cluster: Cluster)(testCode: NotebookPage => T)(implicit webDriver: WebDriver, token: AuthToken): T = {
    withNotebooksListPage(cluster) { notebooksListPage =>
      notebooksListPage.withNewNotebook { notebookPage =>
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
}
