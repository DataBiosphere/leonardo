package org.broadinstitute.dsde.workbench.leonardo

import java.io.File
import java.time.Instant

import org.broadinstitute.dsde.workbench.service.Orchestration
import org.broadinstitute.dsde.workbench.ResourceFile
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.model.google.{GcsObjectName, GoogleProject}
import org.scalatest.{BeforeAndAfterAll, FreeSpec}

class NotebookInteractionSpec extends FreeSpec with LeonardoTestUtils with BeforeAndAfterAll {
  /*
   * This class creates a cluster in a new billing project and runs all tests inside the same cluster.
   */
  var billingProject : GoogleProject = _
  var ronCluster : Cluster = _

  implicit val ronToken: AuthToken = ronAuthToken

  override def beforeAll(): Unit = {
    super.beforeAll()

    billingProject = createNewBillingProject()
    Orchestration.billing.addUserToBillingProject(billingProject.value, ronEmail, Orchestration.billing.BillingProjectRole.User)(hermioneAuthToken)
    ronCluster = createNewCluster(billingProject)(ronAuthToken)
  }

  override def afterAll(): Unit = {
    deleteAndMonitor(billingProject, ronCluster.clusterName)(ronAuthToken)
    cleanupBillingProject(billingProject)

    super.afterAll()
  }

  "Leonardo notebooks" - {
    val hailFileName: String = "import-hail.ipynb"
    val hailUploadFile = ResourceFile(s"diff-tests/$hailFileName")

    // must align with run-tests.sh and hub-compose-fiab.yml
    val downloadDir = "chrome/downloads"
    val hailDownloadFile = new File(downloadDir, hailFileName)
    hailDownloadFile.mkdirs()

    "should open the notebooks list page" in withWebDriver { implicit driver =>
      withNotebooksListPage(ronCluster) { _ =>
        // no-op; just verify that it opens
      }
    }

    "should upload notebook and verify execution" in withWebDriver(downloadDir) { implicit driver =>
      withNotebookUpload(ronCluster, hailUploadFile) { notebook =>
        notebook.runAllCells(60) // wait 60 sec for Kernel to init and Hail to load
        notebook.download()
      }

      // move the file to a unique location so it won't interfere with other tests
      val uniqueDownFile = new File(downloadDir, s"import-hail-${Instant.now().toString}.ipynb")

      moveFile(hailDownloadFile, uniqueDownFile)
      uniqueDownFile.deleteOnExit()

      // output for this notebook includes an IP address which can vary
      compareFilesExcludingIPs(hailUploadFile, uniqueDownFile)
    }

    "should execute cells" in withWebDriver { implicit driver =>
      withNewNotebook(ronCluster) { notebookPage =>
        notebookPage.executeCell("1+1") shouldBe Some("2")
        notebookPage.executeCell("2*3") shouldBe Some("6")
        notebookPage.executeCell("""print 'Hello Notebook!'""") shouldBe Some("Hello Notebook!")
      }
    }

    "should localize files" in withWebDriver { implicit driver =>
      withFileUpload(ronCluster, hailUploadFile) { _ =>
        //good data
        val goodLocalize = Map(
          "test.rtf" -> s"$swatTestBucket/test.rtf"
          //TODO: create a bucket and upload to there
          //"gs://new_bucket/import-hail.ipynb" -> "import-hail.ipynb"
        )

        implicit val patienceConfig: PatienceConfig = localizePatience
        eventually {
          Leonardo.notebooks.localize(ronCluster.googleProject, ronCluster.clusterName, goodLocalize)
          //the following line will barf with an exception if the file isn't there; that's enough
          Leonardo.notebooks.getContentItem(ronCluster.googleProject, ronCluster.clusterName, "test.rtf", includeContent = false)
        }


        val localizationLog = Leonardo.notebooks.getContentItem(ronCluster.googleProject, ronCluster.clusterName, "localization.log")
        localizationLog.content shouldBe defined
        localizationLog.content.get shouldNot include("Exception")

        //bad data
        val badLocalize = Map("file.out" -> "gs://nobuckethere")
        Leonardo.notebooks.localize(ronCluster.googleProject, ronCluster.clusterName, badLocalize)
        val localizationLogAgain = Leonardo.notebooks.getContentItem(ronCluster.googleProject, ronCluster.clusterName, "localization.log")
        localizationLogAgain.content shouldBe defined
        localizationLogAgain.content.get should include("Exception")
      }
    }

    "should import AoU library" in withWebDriver { implicit driver =>
      withNewNotebook(ronCluster) { notebookPage =>
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
      withNewGoogleBucket(billingProject) { bucketName =>
        val userScriptString = "#!/usr/bin/env bash\n\npip install arrow"
        val userScriptObjectName = GcsObjectName("user-script.sh")
        val userScriptUri = s"gs://${bucketName.value}/${userScriptObjectName.value}"

        withNewBucketObject(bucketName, userScriptObjectName, userScriptString, "text/plain") { objectName =>
          val clusterName = ClusterName("user-script-cluster" + makeRandomId())

          withNewCluster(billingProject, clusterName, ClusterRequest(Map(), None, Option(userScriptUri))) { cluster =>
            withNewNotebook(cluster) { notebookPage =>
              notebookPage.executeCell("""print 'Hello Notebook!'""") shouldBe Some("Hello Notebook!")
              notebookPage.executeCell("""import arrow""")
              notebookPage.executeCell("""arrow.get(727070400)""") shouldBe Some("<Arrow [1993-01-15T04:00:00+00:00]>")
            }
          }
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
          """import bx
            |bx.sys.copyright""".stripMargin //the most annoying package to install

        notebookPage.executeCell("1+1") shouldBe Some("2")
        notebookPage.executeCell(getPythonVersion) shouldBe Some("3.4.2")
        notebookPage.executeCell(getBxPython).get should include("Copyright (c)")
      }
    }

  }
}
