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
import org.scalatest.{FreeSpec, Matchers, ParallelTestExecution}

import scala.util.control.NonFatal
import scala.util.{Random, Try}

class LeonardoSpec extends FreeSpec with Matchers with Eventually with ParallelTestExecution
  with WebBrowser with WebBrowserSpec with LocalFileUtil with ScalaFutures {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(timeout = scaled(Span(5, Seconds)))

  // Ron and Hermione are on the dev Leo whitelist.
  val ronAuthToken = AuthToken(LeonardoConfig.Users.ron)
  val hermioneAuthToken = AuthToken(LeonardoConfig.Users.hermione)

  // kudos to whoever named this Patience
  val clusterPatience = PatienceConfig(timeout = scaled(Span(15, Minutes)), interval = scaled(Span(20, Seconds)))
  val localizePatience = PatienceConfig(timeout = scaled(Span(1, Minutes)), interval = scaled(Span(1, Seconds)))

  val sa = GoogleServiceAccount(LeonardoConfig.Leonardo.notebooksServiceAccountEmail)
  val swatTestBucket = "gs://leonardo-swat-test-bucket-do-not-delete"
  val incorrectJupyterExtensionUri = swatTestBucket + "/"

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
    val request = ClusterRequest(Map("foo" -> makeRandomId()))

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

  def withNewBillingProject[T](testCode: GoogleProject => T): T = {
    val ownerToken: AuthToken = hermioneAuthToken
    val billingProject = GoogleProject("leonardo-billing-spec-" + makeRandomId())

    // Create billing project and run test code
    val testResult: Try[T] = Try {
      logger.info(s"Creating billing project: $billingProject")
      Orchestration.billing.createBillingProject(billingProject.value, WorkbenchConfig.Projects.billingAccountId)(ownerToken)
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
      withNewBillingProject { project =>
        implicit val token = ronAuthToken
        val ronEmail = LeonardoConfig.Users.ron.email
        val ownerToken = hermioneAuthToken
        Orchestration.billing.addUserToBillingProject(project.value, ronEmail, Orchestration.billing.BillingProjectRole.User)(ownerToken)

        withNewCluster(project) { _ =>
          // no-op; just verify that it launches
        }
      }
    }

    "should error on cluster create and delete the cluster" in {
      withNewBillingProject { project =>
        implicit val token = ronAuthToken
        val ronEmail = LeonardoConfig.Users.ron.email
        val ownerToken = hermioneAuthToken
        Orchestration.billing.addUserToBillingProject(project.value, ronEmail, Orchestration.billing.BillingProjectRole.User)(ownerToken)

        withNewErroredCluster(project) { _ =>
          // no-op; just verify that it launches
        }
      }
    }

    "should open the notebooks list page" in withWebDriver { implicit driver =>
      withNewBillingProject { project =>
        implicit val token = ronAuthToken
        val ronEmail = LeonardoConfig.Users.ron.email
        val ownerToken = hermioneAuthToken
        Orchestration.billing.addUserToBillingProject(project.value, ronEmail, Orchestration.billing.BillingProjectRole.User)(ownerToken)

        withNewCluster(project) { cluster =>
          withNotebooksListPage(cluster) { _ =>
            // no-op; just verify that it opens
          }
        }
      }
    }

    "should upload a notebook to Jupyter" in withWebDriver { implicit driver =>
      withNewBillingProject { project =>
        implicit val token = ronAuthToken
        val ronEmail = LeonardoConfig.Users.ron.email
        val ownerToken = hermioneAuthToken
        Orchestration.billing.addUserToBillingProject(project.value, ronEmail, Orchestration.billing.BillingProjectRole.User)(ownerToken)

        val file = ResourceFile("diff-tests/import-hail.ipynb")

        withNewCluster(project) { cluster =>
          withNotebookUpload(cluster, file) { _ =>
            // no-op; just verify that it uploads
          }
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
      withNewBillingProject { project =>
        implicit val token = ronAuthToken
        val ronEmail = LeonardoConfig.Users.ron.email
        val ownerToken = hermioneAuthToken
        Orchestration.billing.addUserToBillingProject(project.value, ronEmail, Orchestration.billing.BillingProjectRole.User)(ownerToken)

        withNewCluster(project) { cluster =>
          withNotebookUpload(cluster, upFile) { notebook =>
            notebook.runAllCells(60) // wait 60 sec for Kernel to init and Hail to load
            notebook.download()
          }
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
      withNewBillingProject { project =>
        implicit val token = ronAuthToken
        val ronEmail = LeonardoConfig.Users.ron.email
        val ownerToken = hermioneAuthToken
        Orchestration.billing.addUserToBillingProject(project.value, ronEmail, Orchestration.billing.BillingProjectRole.User)(ownerToken)

        withNewCluster(project) { cluster =>
          withNewNotebook(cluster) { notebookPage =>
            notebookPage.executeCell("1+1") shouldBe Some("2")
            notebookPage.executeCell("2*3") shouldBe Some("6")
            notebookPage.executeCell("""print 'Hello Notebook!'""") shouldBe Some("Hello Notebook!")
          }
        }
      }
    }

    "should localize files" in withWebDriver { implicit driver =>
      withNewBillingProject { project =>
        implicit val token = ronAuthToken

        val ronEmail = LeonardoConfig.Users.ron.email
        val ownerToken = hermioneAuthToken
        Orchestration.billing.addUserToBillingProject(project.value, ronEmail, Orchestration.billing.BillingProjectRole.User)(ownerToken)
        val file = ResourceFile("diff-tests/import-hail.ipynb")

        withNewCluster(project) { cluster =>
          withFileUpload(cluster, file) { _ =>
            //good data
            val goodLocalize = Map(
              "test.rtf" -> s"$swatTestBucket/test.rtf"
              //TODO: create a bucket and upload to there
              //"gs://new_bucket/import-hail.ipynb" -> "import-hail.ipynb"
            )

            eventually {
              Leonardo.notebooks.localize(cluster.googleProject, cluster.clusterName, goodLocalize)
              //the following line will barf with an exception if the file isn't there; that's enough
              Leonardo.notebooks.getContentItem(cluster.googleProject, cluster.clusterName, "test.rtf", includeContent = false)
            }(localizePatience)


            val localizationLog = Leonardo.notebooks.getContentItem(cluster.googleProject, cluster.clusterName, "localization.log")
            localizationLog.content shouldBe defined
            localizationLog.content.get shouldNot include("Exception")

            //bad data
            val badLocalize = Map("file.out" -> "gs://nobuckethere")
            Leonardo.notebooks.localize(cluster.googleProject, cluster.clusterName, badLocalize)
            val localizationLogAgain = Leonardo.notebooks.getContentItem(cluster.googleProject, cluster.clusterName, "localization.log")
            localizationLogAgain.content shouldBe defined
            localizationLogAgain.content.get should include("Exception")
          }
        }
      }
    }

    "should import AoU library" in withWebDriver { implicit driver =>
      withNewBillingProject { project =>
        implicit val token = ronAuthToken
        val ronEmail = LeonardoConfig.Users.ron.email
        val ownerToken = hermioneAuthToken
        Orchestration.billing.addUserToBillingProject(project.value, ronEmail, Orchestration.billing.BillingProjectRole.User)(ownerToken)
        withNewCluster(project) { cluster =>
          withNewNotebook(cluster) { notebookPage =>
            val importAoU =
              """import sys
                |import aou_workbench_client
                |modulename = 'aou_workbench_client'
                |if modulename not in sys.modules:
                |    print 'You have not imported the {} module'.format(modulename)
                |else:
                |    print 'AoU library installed'""".stripMargin

            notebookPage.executeCell(importAoU) shouldBe Some("AoU library installed")
          }
        }
      }
    }

    "should create a cluster in a different billing project and put the pet's credentials on the cluster" in withWebDriver { implicit driver =>
      withNewBillingProject { newProject =>

        implicit val token = ronAuthToken

        val ronEmail = LeonardoConfig.Users.ron.email
        val ownerToken = hermioneAuthToken
        Orchestration.billing.addUserToBillingProject(project.value, ronEmail, Orchestration.billing.BillingProjectRole.User)(ownerToken)
        // Pre-conditions: pet service account exists in this Google project and in Sam
        val (petName, petEmail) = getAndVerifyPet(newProject)

        // Create a cluster

        withNewCluster(newProject) { cluster =>
          // cluster should have been created with the pet service account
          cluster.serviceAccountInfo.clusterServiceAccount shouldBe Some(petEmail)
          cluster.serviceAccountInfo.notebookServiceAccount shouldBe None

          withNewNotebook(cluster) { notebookPage =>
            // should not have notebook credentials because Leo is not configured to use a notebook service account
            verifyNoNotebookCredentials(notebookPage)
          }
        }

        // Post-conditions: pet should still exist in this Google project

        val googlePetEmail2 = googleIamDAO.findServiceAccount(newProject, petName).futureValue.map(_.email)
        googlePetEmail2 shouldBe Some(petEmail)
      }
    }

    "should do cross domain cookie auth" in withWebDriver { implicit driver =>
      withNewBillingProject { project =>
        implicit val token = ronAuthToken
        val ronEmail = LeonardoConfig.Users.ron.email
        val ownerToken = hermioneAuthToken
        Orchestration.billing.addUserToBillingProject(project.value, ronEmail, Orchestration.billing.BillingProjectRole.User)(ownerToken)
        withNewCluster(project) { cluster =>
          withDummyClientPage(cluster) { dummyClientPage =>
            // opens the notebook list page without setting a cookie
            val notebooksListPage = dummyClientPage.openNotebook
            val notebookPage = notebooksListPage.newNotebook
            // execute some cells to make sure it works
            notebookPage.executeCell("1+1") shouldBe Some("2")
            notebookPage.executeCell("2*3") shouldBe Some("6")
            notebookPage.executeCell("""print 'Hello Notebook!'""") shouldBe Some("Hello Notebook!")
          }
        }
      }
    }

    "should allow BigQuerying in a new billing project" in withWebDriver { implicit driver =>
      withNewBillingProject { project =>

        // project owners have the bigquery role automatically, so this also tests granting it to users

        val ronEmail = LeonardoConfig.Users.ron.email
        val ownerToken = hermioneAuthToken
        Orchestration.billing.addUserToBillingProject(project.value, ronEmail, Orchestration.billing.BillingProjectRole.User)(ownerToken)
        Orchestration.billing.addGoogleRoleToBillingProjectUser(project.value, ronEmail, "bigquery.jobUser")(ownerToken)

        implicit val leoToken: AuthToken = ronAuthToken
        withNewCluster(project) { cluster =>
          withNewNotebook(cluster) { notebookPage =>
            val query = """! bq query --format=json "SELECT COUNT(*) AS scullion_count FROM publicdata.samples.shakespeare WHERE word='scullion'" """
            val expectedResult = """[{"scullion_count":"2"}]""".stripMargin

            val result = notebookPage.executeCell(query).get
            result should include("Current status: DONE")
            result should include(expectedResult)
          }
        }
      }
    }

  }
}
