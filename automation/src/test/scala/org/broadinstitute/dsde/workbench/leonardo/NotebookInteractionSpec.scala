package org.broadinstitute.dsde.workbench.leonardo

import java.io.File
import java.nio.file.Files

import org.broadinstitute.dsde.workbench.service.{Orchestration, RestException, Sam}
import org.broadinstitute.dsde.workbench.ResourceFile
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.dao.Google.googleStorageDAO
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.model.google.{GcsEntity, GcsEntityTypes, GcsObjectName, GcsPath, GcsRoles, GoogleProject}
import org.scalatest.{BeforeAndAfterAll, FreeSpec}

import scala.concurrent.duration._
import scala.collection.JavaConverters._
import scala.language.postfixOps

class NotebookInteractionSpec extends FreeSpec with LeonardoTestUtils with BeforeAndAfterAll with BillingFixtures {
  /*
   * This class creates a cluster in a new billing project and runs all tests inside the same cluster.
   */
  var gpAllocProject : ClaimedProject = _
  var billingProject : GoogleProject = _
  var ronCluster : Cluster = _

  implicit val ronToken: AuthToken = ronAuthToken

  override def beforeAll(): Unit = {
    super.beforeAll()

    gpAllocProject = claimGPAllocProject(hermioneCreds, List(ronEmail))
    billingProject = GoogleProject(gpAllocProject.projectName)
    ronCluster = try {

      createNewCluster(billingProject)(ronAuthToken)
    } catch {
      case e: Throwable =>
        logger.error(s"NotebookInteractionSpec: error occurred creating cluster in billing project ${billingProject}", e)
        // clean up billing project here because afterAll() doesn't run if beforeAll() throws an exception
        gpAllocProject.cleanup(hermioneCreds, List(ronEmail))
        throw e
    }
    new File(downloadDir).mkdirs()
  }

  override def afterAll(): Unit = {
    deleteAndMonitor(billingProject, ronCluster.clusterName)(ronAuthToken)
    gpAllocProject.cleanup(hermioneCreds, List(ronEmail))

    super.afterAll()
  }

  "Leonardo notebooks" - {
    val hailFileName: String = "import-hail.ipynb"
    val hailTutorialFileName: String = "hail-tutorial.ipynb"

    val hailUploadFile = ResourceFile(s"diff-tests/$hailFileName")
    val hailTutorialUploadFile = ResourceFile(s"diff-tests/$hailTutorialFileName")

    "should open the notebooks list page" in withWebDriver { implicit driver =>
      withNotebooksListPage(ronCluster) { _ =>
        // no-op; just verify that it opens
      }
    }

    "should upload notebook and verify execution" in withWebDriver(downloadDir) { implicit driver =>
      // output for this notebook includes an IP address which can vary
      uploadDownloadTest(ronCluster, hailUploadFile, 60.seconds)(compareFilesExcludingIPs)
    }

    // See https://hail.is/docs/stable/tutorials-landing.html
    // Note this is for the stable Hail version (0.1). The tutorial script has changed in Hail 0.2.
    "should run the Hail tutorial" in withWebDriver(downloadDir) { implicit driver =>
      uploadDownloadTest(ronCluster, hailTutorialUploadFile, 3.minutes) { (uploadFile, downloadFile) =>
        // There are many differences including timestamps, so we can't really compare uploadFile
        // and downloadFile correctly. For now just verify the absence of ClassCastExceptions, which is the
        // issue reported in https://github.com/DataBiosphere/leonardo/issues/222.
        val downloadFileContents: String = Files.readAllLines(downloadFile.toPath).asScala.mkString
        downloadFileContents should not include "ClassCastException"
      }
    }

    "should execute cells" in withWebDriver { implicit driver =>
      withNewNotebook(ronCluster) { notebookPage =>
        notebookPage.executeCell("1+1") shouldBe Some("2")
        notebookPage.executeCell("2*3") shouldBe Some("6")
        notebookPage.executeCell("""print 'Hello Notebook!'""") shouldBe Some("Hello Notebook!")
      }
    }

    "should localize files in async mode" in withWebDriver { implicit driver =>
      val localizeFileName = "localize_async.txt"
      val localizeFileContents = "Async localize test"
      val delocalizeFileName = "delocalize_async.txt"
      val delocalizeFileContents = "Async delocalize test"
      val localizeDataFileName = "localize_data_async.txt"
      val localizeDataContents = "Hello World"

      withLocalizeDelocalizeFiles(ronCluster, localizeFileName, localizeFileContents, delocalizeFileName, delocalizeFileContents, localizeDataFileName, localizeDataContents) { (localizeRequest, bucketName) =>
        // call localize; this should return 200
        Leonardo.notebooks.localize(ronCluster.googleProject, ronCluster.clusterName, localizeRequest, async = true)

        // check that the files are eventually at their destinations
        implicit val patienceConfig: PatienceConfig = localizePatience
        eventually {
          verifyLocalizeDelocalize(ronCluster, localizeFileName, localizeFileContents, GcsPath(bucketName, GcsObjectName(delocalizeFileName)), delocalizeFileContents, localizeDataFileName, localizeDataContents)
        }

        // call localize again with bad data. This should still return 200 since we're in async mode.
        val badLocalize = Map("file.out" -> "gs://nobuckethere")
        Leonardo.notebooks.localize(ronCluster.googleProject, ronCluster.clusterName, badLocalize, async = true)

        // it should not have localized this file
        val thrown = the [RestException] thrownBy {
          Leonardo.notebooks.getContentItem(ronCluster.googleProject, ronCluster.clusterName, "file.out", includeContent = false)
        }
        // why doesn't `RestException` have a status code field?
        thrown.message should include ("No such file or directory: file.out")
      }
    }

    "should localize files in sync mode" in withWebDriver { implicit driver =>
      val localizeFileName = "localize_sync.txt"
      val localizeFileContents = "Sync localize test"
      val delocalizeFileName = "delocalize_sync.txt"
      val delocalizeFileContents = "Sync delocalize test"
      val localizeDataFileName = "localize_data_aync.txt"
      val localizeDataContents = "Hello World"

      withLocalizeDelocalizeFiles(ronCluster, localizeFileName, localizeFileContents, delocalizeFileName, delocalizeFileContents, localizeDataFileName, localizeDataContents) { (localizeRequest, bucketName) =>
        // call localize; this should return 200
        Leonardo.notebooks.localize(ronCluster.googleProject, ronCluster.clusterName, localizeRequest, async = false)

        // check that the files are immediately at their destinations
        verifyLocalizeDelocalize(ronCluster, localizeFileName, localizeFileContents, GcsPath(bucketName, GcsObjectName(delocalizeFileName)), delocalizeFileContents, localizeDataFileName, localizeDataContents)

        // call localize again with bad data. This should still return 500 since we're in sync mode.
        val badLocalize = Map("file.out" -> "gs://nobuckethere")
        val thrown = the [RestException] thrownBy {
          Leonardo.notebooks.localize(ronCluster.googleProject, ronCluster.clusterName, badLocalize, async = false)
        }
        // why doesn't `RestException` have a status code field?
        thrown.message should include ("500 : Internal Server Error")
        thrown.message should include ("Error occurred localizing")
        thrown.message should include ("See localization.log for details.")

        // it should not have localized this file
        val contentThrown = the [RestException] thrownBy {
          Leonardo.notebooks.getContentItem(ronCluster.googleProject, ronCluster.clusterName, "file.out", includeContent = false)
        }
        contentThrown.message should include ("No such file or directory: file.out")
      }
    }

    "should do cross domain cookie auth" in withWebDriver { implicit driver =>
      withDummyClientPage(ronCluster) { dummyClientPage =>
        // opens the notebook list page without setting a cookie
        val notebooksListPage = dummyClientPage.openNotebook
        notebooksListPage.withNewNotebook() { notebookPage =>
          // execute some cells to make sure it works
          notebookPage.executeCell("1+1") shouldBe Some("2")
          notebookPage.executeCell("2*3") shouldBe Some("6")
          notebookPage.executeCell("""print 'Hello Notebook!'""") shouldBe Some("Hello Notebook!")
        }
      }
    }

    "should allow BigQuerying in a new billing project" in withWebDriver { implicit driver =>
      // project owners have the bigquery role automatically, so this also tests granting it to users
      val ownerToken = hermioneAuthToken
      Orchestration.billing.addGoogleRoleToBillingProjectUser(billingProject.value, ronEmail, "bigquery.jobUser")(ownerToken)

      withNewNotebook(ronCluster) { notebookPage =>
        val query = """! bq query --format=json "SELECT COUNT(*) AS scullion_count FROM publicdata.samples.shakespeare WHERE word='scullion'" """
        val expectedResult = """[{"scullion_count":"2"}]""".stripMargin

        val result = notebookPage.executeCell(query).get
        result should include("Current status: DONE")
        result should include(expectedResult)
      }
    }

    // requires a new cluster because we want to pass in a user script in the cluster request
    "should allow user to create a cluster with a script" in withWebDriver { implicit driver =>
      Orchestration.billing.addUserToBillingProject(billingProject.value, ronEmail, Orchestration.billing.BillingProjectRole.User)(hermioneAuthToken)

      //a cluster without the user script should not be able to import the arrow library
      withNewNotebook(ronCluster) { notebookPage =>
        notebookPage.executeCell("""print 'Hello Notebook!'""") shouldBe Some("Hello Notebook!")
        notebookPage.executeCell("""import arrow""").get should include("ImportError: No module named arrow")
      }
      // create a new bucket, add the user script to the bucket, create a new cluster using the URI of the user script and create a notebook that will check if the user script ran
      val gpAllocScriptProject = claimGPAllocProject(hermioneCreds)
      val billingScriptProject = GoogleProject(gpAllocScriptProject.projectName)
      withNewGoogleBucket(billingScriptProject) { bucketName =>
        val ronPetServiceAccount = Sam.user.petServiceAccountEmail(billingProject.value)(ronAuthToken)
        googleStorageDAO.setBucketAccessControl(bucketName, GcsEntity(ronPetServiceAccount, GcsEntityTypes.User), GcsRoles.Owner)

        val userScriptString = "#!/usr/bin/env bash\n\npip2 install arrow"
        val userScriptObjectName = GcsObjectName("user-script.sh")
        val userScriptUri = s"gs://${bucketName.value}/${userScriptObjectName.value}"

        withNewBucketObject(bucketName, userScriptObjectName, userScriptString, "text/plain") { objectName =>
          googleStorageDAO.setObjectAccessControl(bucketName, objectName, GcsEntity(ronPetServiceAccount, GcsEntityTypes.User), GcsRoles.Owner)
          val clusterName = ClusterName("user-script-cluster" + makeRandomId())
          withNewCluster(billingProject, clusterName, ClusterRequest(Map(), None, Option(userScriptUri))) { cluster =>
            Thread.sleep(10000)
            withNewNotebook(cluster) { notebookPage =>
              notebookPage.executeCell("""print 'Hello Notebook!'""") shouldBe Some("Hello Notebook!")
              notebookPage.executeCell("""import arrow""")
              notebookPage.executeCell("""arrow.get(727070400)""") shouldBe Some("<Arrow [1993-01-15T04:00:00+00:00]>")
            }
          }(ronAuthToken)
        }
      }
    }
    "should create a notebook with a working Python 3 kernel and import installed packages" in withWebDriver { implicit driver =>
      Orchestration.billing.addUserToBillingProject(billingProject.value, ronEmail, Orchestration.billing.BillingProjectRole.User)(hermioneAuthToken)

      withNewNotebook(ronCluster, Python3) { notebookPage =>
        val getPythonVersion =
          """import platform
            |print(platform.python_version())""".stripMargin
        val getBxPython =
          """import bx.bitset
            |bx.bitset.sys.copyright""".stripMargin
        val sparkJob =
          """import random
            |NUM_SAMPLES=20
            |def inside(p):
            |    x, y = random.random(), random.random()
            |    return x*x + y*y < 1
            |
            |count = sc.parallelize(range(0, NUM_SAMPLES)) \
            |             .filter(inside).count()
            |print("Pi is roughly %f" % (4.0 * count / NUM_SAMPLES))""".stripMargin

        notebookPage.executeCell("1+1") shouldBe Some("2")
        notebookPage.executeCell(getPythonVersion) shouldBe Some("3.4.2")
        notebookPage.executeCell(getBxPython).get should include("Copyright (c)")
        notebookPage.executeCell(sparkJob).get should include("Pi is roughly ")
      }
    }

    "should create a notebook with a working R kernel and import installed packages" in withWebDriver { implicit driver =>
      Orchestration.billing.addUserToBillingProject(billingProject.value, ronEmail, Orchestration.billing.BillingProjectRole.User)(hermioneAuthToken)

      withNewNotebook(ronCluster, RKernel) { notebookPage =>
        notebookPage.executeCell("library(SparkR)").get should include("SparkR")
        notebookPage.executeCell("sparkR.session()")
        notebookPage.executeCell("df <- as.DataFrame(faithful)")
        notebookPage.executeCell("head(df)").get should include ("3.600 79")

        val sparkJob =
          """samples <- 200
            |inside <- function(index) {
            |  set.seed(index)
            |  rand <- runif(2, 0.0, 1.0)
            |  sum(rand^2) < 1
            |}
            |res <- spark.lapply(c(1:samples), inside)
            |pi <- length(which(unlist(res)))*4.0/samples
            |cat("Pi is roughly", pi, "\n")""".stripMargin

        notebookPage.executeCell(sparkJob).get should include("Pi is roughly ")
      }
    }

    "should be able to install new R packages" in withWebDriver { implicit driver =>
      Orchestration.billing.addUserToBillingProject(billingProject.value, ronEmail, Orchestration.billing.BillingProjectRole.User)(hermioneAuthToken)

      withNewNotebook(ronCluster, RKernel) { notebookPage =>
        // httr is a simple http library for R
        // http://httr.r-lib.org//index.html

        // it may take a little while to install
        val installTimeout = 2 minutes

        notebookPage.executeCell("""install.packages("httr")""", installTimeout).get should include ("Installing package into '/home/jupyter-user/.rpackages'")

        val httpGetTest =
          """library(httr)
            |r <- GET("http://www.example.com")
            |status_code(r)
          """.stripMargin

        notebookPage.executeCell(httpGetTest) shouldBe Some("200")
      }
    }

    Seq(Python2, Python3).foreach { kernel =>
      s"should be able to pip install packages using ${kernel.string}" in withWebDriver { implicit driver =>
        withNewNotebook(ronCluster, kernel) { notebookPage =>
          // install tensorflow
          pipInstall(notebookPage, kernel, "tensorflow")
          notebookPage.saveAndCheckpoint()
        }

        // need to restart the kernel for the install to take effect
        withNewNotebook(ronCluster, kernel) { notebookPage =>
          // verify that tensorflow is installed
          verifyTensorFlow(notebookPage, kernel)
        }
      }
    }
  }
}
