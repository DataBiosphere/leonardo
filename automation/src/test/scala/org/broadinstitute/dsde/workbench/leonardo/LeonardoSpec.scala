package org.broadinstitute.dsde.workbench.leonardo

import java.io.File
import java.nio.file.Files
import java.time.Instant

import org.broadinstitute.dsde.firecloud.api.{Orchestration, Rawls, Sam}
import org.broadinstitute.dsde.workbench.api.APIException
import org.broadinstitute.dsde.workbench.{ResourceFile, WebBrowserSpec}
import org.broadinstitute.dsde.workbench.config.{AuthToken, WorkbenchConfig}
import org.broadinstitute.dsde.workbench.leonardo.ClusterStatus.ClusterStatus
import org.broadinstitute.dsde.workbench.leonardo.StringValueClass.LabelMap
import org.broadinstitute.dsde.workbench.util.LocalFileUtil
import org.broadinstitute.dsde.workbench.dao.Google.googleIamDAO
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{GoogleProject, ServiceAccountName}
import org.openqa.selenium.WebDriver
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.selenium.WebBrowser
import org.scalatest.time.{Minutes, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FreeSpec, Matchers, ParallelTestExecution}

import scala.util.control.NonFatal
import scala.util.{Random, Try}

class LeonardoSpec extends FreeSpec with Matchers with Eventually with ParallelTestExecution with BeforeAndAfterAll
  with WebBrowser with WebBrowserSpec with LocalFileUtil with ScalaFutures {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(timeout = scaled(Span(5, Seconds)))

  // Ron and Hermione are on the dev Leo whitelist.
  val ronAuthToken = AuthToken(LeonardoConfig.Users.ron)
  val hermioneAuthToken = AuthToken(LeonardoConfig.Users.hermione)

  // kudos to whoever named this Patience
  val clusterPatience = PatienceConfig(timeout = scaled(Span(15, Minutes)), interval = scaled(Span(20, Seconds)))

  // simultaneous requests by the same user to create their pet in Sam can cause contention
  // but this is not a realistic production scenario
  // so we avoid this by pre-creating

  override def beforeAll(): Unit = {
    // ensure pet is initialized in test env
    Sam.user.petServiceAccountEmail(project)(ronAuthToken)
    Sam.user.petServiceAccountEmail(project)(hermioneAuthToken)
  }

  val project = GoogleProject(LeonardoConfig.Projects.default)
  val sa = GoogleServiceAccount(LeonardoConfig.Leonardo.notebooksServiceAccountEmail)
  val incorrectJupyterExtensionUri = "gs://leonardo-swat-test-bucket-do-not-delete/"

  // must align with run-tests.sh and hub-compose-fiab.yml
  val downloadDir = "chrome/downloads"

  // ------------------------------------------------------

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
    val actualCluster = eventually {
      clusterCheck(Leonardo.cluster.get(googleProject, clusterName), clusterRequest.labels, googleProject, clusterName, Seq(ClusterStatus.Running, ClusterStatus.Error), clusterRequest.jupyterExtensionUri)
    } (clusterPatience)

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
    eventually {
      val statusOpt = Leonardo.cluster.listIncludingDeleted().find(_.clusterName == clusterName).map(_.status)
      statusOpt getOrElse ClusterStatus.Deleted shouldBe ClusterStatus.Deleted
    } (clusterPatience)
  }


  // TODO: show diffs as screenshot or other test output?
  def compareFilesExcludingIPs(left: File, right: File): Unit = {

    def linesWithoutIPs(file: File) = {
      import scala.collection.JavaConverters._
      Files.readAllLines(left.toPath).asScala map { _.replaceAll("(\\d+.){3}\\d+", "<IP>") }
    }

    linesWithoutIPs(left) shouldEqual linesWithoutIPs(right)
  }

  // ------------------------------------------------------

  def withNewCluster[T](googleProject: GoogleProject)(testCode: Cluster => T)(implicit token: AuthToken): T = {
    val name = ClusterName(s"automation-test-a${makeRandomId().toLowerCase}z")
    val request = ClusterRequest(None, Map("foo" -> makeRandomId()))

    val testResult: Try[T] = Try {
      val cluster = createAndMonitor(googleProject, name, request)
      cluster.status shouldBe ClusterStatus.Running
      testCode(cluster)
    }

    // delete before checking testCode status, which may throw
    deleteAndMonitor(googleProject, name)
    testResult.get
  }

  def withNewErroredCluster[T](googleProject: GoogleProject)(testCode: Cluster => T)(implicit token: AuthToken): T = {
    val name = ClusterName(s"automation-test-a${makeRandomId()}z")
    val request = ClusterRequest(None, Map("foo" -> makeRandomId()), Some(incorrectJupyterExtensionUri))
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

  def withNotebookUpload[T](cluster: Cluster, file: File)(testCode: NotebookPage => T)(implicit webDriver: WebDriver, token: AuthToken): T = {
    withNotebooksListPage(cluster) { notebooksListPage =>
      notebooksListPage.upload(file)

      val notebookPage = notebooksListPage.openNotebook(file)
      testCode(notebookPage)
    }
  }

  def withNewNotebook[T](cluster: Cluster)(testCode: NotebookPage => T)(implicit webDriver: WebDriver, token: AuthToken): T = {
    withNotebooksListPage(cluster) { notebooksListPage =>
      val notebookPage = notebooksListPage.newNotebook
      testCode(notebookPage)
    }
  }

  def withNewBillingProject[T](testCode: GoogleProject => T)(implicit token: AuthToken): T = {
    val billingProject = GoogleProject("leonardo-billing-spec" + makeRandomId())
    // Create billing project and run test code
    val testResult: Try[T] = Try {
      logger.info(s"Creating billing project: $billingProject")
      Orchestration.billing.createBillingProject(billingProject.value, WorkbenchConfig.Projects.billingAccountId)
      testCode(billingProject)
    }
    // Clean up billing project
    Try(Rawls.admin.deleteBillingProject(billingProject.value)(AuthToken(LeonardoConfig.Users.dumbledore))).recover { case NonFatal(e) =>
      logger.warn(s"Could not delete billing project $billingProject", e)
    }
    // Return the test result, or throw error
    testResult.get
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
    val samPetEmail = Sam.user.petServiceAccountEmail(project)
    val userStatus = Sam.user.status().get
    val petName = Sam.petName(userStatus.userInfo)
    val googlePetEmail = googleIamDAO.findServiceAccount(project, petName).futureValue.map(_.email)
    googlePetEmail shouldBe Some(samPetEmail)
    (petName, samPetEmail)
  }

  // ------------------------------------------------------

  "Leonardo" - {
    "should ping" in {
      implicit val token = ronAuthToken
      Leonardo.test.ping() shouldBe "OK"
    }

    "should create, monitor, and delete a cluster" in {
      implicit val token = ronAuthToken
      withNewCluster(project) { _ =>
        // no-op; just verify that it launches
      }
    }

    "should error on cluster create and delete the cluster" in {
      implicit val token = ronAuthToken
      withNewErroredCluster(project) { _ =>
        // no-op; just verify that it launches
      }
    }

    "should open the notebooks list page" in withWebDriver { implicit driver =>
      implicit val token = ronAuthToken
      withNewCluster(project) { cluster =>
        withNotebooksListPage(cluster) { _ =>
          // no-op; just verify that it opens
        }
      }
    }

    "should upload a notebook to Jupyter" in withWebDriver { implicit driver =>
      implicit val token = ronAuthToken
      val file = ResourceFile("diff-tests/import-hail.ipynb")

      withNewCluster(project) { cluster =>
        withNotebookUpload(cluster, file) { _ =>
          // no-op; just verify that it uploads
        }
      }
    }

    val fileName: String = "import-hail.ipynb"
    val upFile = ResourceFile(s"diff-tests/$fileName")

    // must align with run-tests.sh and hub-compose-fiab.yml
    val downloadDir = "chrome/downloads"
    val downFile = new File(downloadDir, fileName)
    downFile.mkdirs()

    "should verify notebook execution" in withWebDriver(downloadDir) { implicit driver =>
      implicit val token = ronAuthToken

      withNewCluster(project) { cluster =>
        withNotebookUpload(cluster, upFile) { notebook =>
          notebook.runAllCells(60)  // wait 60 sec for Kernel to init and Hail to load
          notebook.download()
        }
      }

      // move the file to a unique location so it won't interfere with other tests
      val uniqueDownFile = new File(downloadDir, s"import-hail-${Instant.now().toString}.ipynb")

      moveFile(downFile, uniqueDownFile)
      uniqueDownFile.deleteOnExit()

      // output for this notebook includes an IP address which can vary
      compareFilesExcludingIPs(upFile, uniqueDownFile)
    }

    "should execute cells" in withWebDriver { implicit driver =>
      implicit val token = ronAuthToken
      withNewCluster(project) { cluster =>
        withNewNotebook(cluster) { notebookPage =>
          notebookPage.executeCell("1+1") shouldBe Some("2")
          notebookPage.executeCell("2*3") shouldBe Some("6")
          notebookPage.executeCell("""print 'Hello Notebook!'""") shouldBe Some("Hello Notebook!")
        }
      }
    }

    "should put the pet's credentials on the cluster" in withWebDriver { implicit driver =>
      implicit val token = ronAuthToken

      /*
       * Pre-conditions: pet service account exists in this Google project and in Sam
       */
      val (petName, petEmail) = getAndVerifyPet(project)

      /*
       * Create a cluster
       */
      withNewCluster(project) { cluster =>
        // cluster should have been created with the pet service account
        cluster.serviceAccountInfo.clusterServiceAccount shouldBe Some(petEmail)
        cluster.serviceAccountInfo.notebookServiceAccount shouldBe None

        withNewNotebook(cluster) { notebookPage =>
          // should not have notebook credentials because Leo is not configured to use a notebook service account
          verifyNoNotebookCredentials(notebookPage)
        }
      }

      /*
       * Post-conditions: pet should still exist in this Google project
       */
      val googlePetEmail2 = googleIamDAO.findServiceAccount(project, petName).futureValue.map(_.email)
      googlePetEmail2 shouldBe Some(petEmail)
    }


    "should create a cluster in a different billing project" in withWebDriver { implicit driver =>
      // need to be project owner for this test
      implicit val token = hermioneAuthToken

      /*
       * Create a cluster in a different billing project
       */
      withNewBillingProject { billingProject =>
        withNewCluster(billingProject) { cluster =>
          // cluster should have been created with the pet service account
          // Can't actually verify this against a Google IAM call since the QA
          // service account doesn't have IAM permissions in other billing projects.
          cluster.serviceAccountInfo.clusterServiceAccount shouldBe 'defined
          cluster.serviceAccountInfo.notebookServiceAccount shouldBe None

          // Verify pet credentials from a notebook
          withNewNotebook(cluster) { notebookPage =>
            // should not have notebook credentials because Leo is not configured to use a notebook service account
            verifyNoNotebookCredentials(notebookPage)
          }
        }
      }
    }
  }
}
