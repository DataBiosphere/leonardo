package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.service.Orchestration
import org.broadinstitute.dsde.workbench.service.util.Tags

import scala.concurrent.duration.DurationLong
import scala.language.postfixOps

class NotebookPyKernelSpec extends ClusterFixtureSpec {

  "Leonardo notebooks" - {

    "should open the notebooks list page" in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNotebooksListPage(clusterFixture.cluster) { notebooksListPage =>
          // noop just verify that it opens
        }
      }
    }

    "should do cross domain cookie auth" ignore { clusterFixture =>
      withWebDriver { implicit driver =>
        withDummyClientPage(clusterFixture.cluster) { dummyClientPage =>
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
    }

    "should execute cells" taggedAs Tags.SmokeTest in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster) { notebookPage =>
          notebookPage.executeCell("1+1") shouldBe Some("2")
          notebookPage.executeCell("2*3") shouldBe Some("6")
          notebookPage.executeCell("""print 'Hello Notebook!'""") shouldBe Some("Hello Notebook!")
        }
      }
    }

    "should include Content-Security-Policy in headers" in { clusterFixture =>
      val headers = Leonardo.notebooks.getApiHeaders(clusterFixture.billingProject, clusterFixture.cluster.clusterName)
      val contentSecurityHeader = headers.find(_.name == "Content-Security-Policy")
      contentSecurityHeader shouldBe 'defined
      contentSecurityHeader.get.value should include ("https://bvdp-saturn-prod.appspot.com")
    }

    "should allow BigQuerying in a new billing project" in { clusterFixture =>
      // project owners have the bigquery role automatically, so this also tests granting it to users
      val ownerToken = hermioneAuthToken
      Orchestration.billing.addGoogleRoleToBillingProjectUser(clusterFixture.billingProject.value, ronEmail, "bigquery.jobUser")(ownerToken)
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster) { notebookPage =>
          val query = """! bq query --format=json "SELECT COUNT(*) AS scullion_count FROM publicdata.samples.shakespeare WHERE word='scullion'" """
          val expectedResult = """[{"scullion_count":"2"}]""".stripMargin

          val result = notebookPage.executeCell(query, timeout = 5.minutes).get
          result should include("Current status: DONE")
          result should include(expectedResult)
        }
      }
    }

    "should update dateAccessed if the notebook is open" in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster) { notebookPage =>
          val firstApiCall = Leonardo.cluster.get(clusterFixture.billingProject, clusterFixture.cluster.clusterName)
          //Sleeping for 90s to simulate idle notebook
          logger.info("Sleeping for 90s to simulate idle notebook")
          Thread.sleep(90000)
          val secondApiCall = Leonardo.cluster.get(clusterFixture.billingProject, clusterFixture.cluster.clusterName)
          firstApiCall.dateAccessed should be < secondApiCall.dateAccessed
        }
      }
    }


    Seq(Python2, Python3).foreach { kernel =>
      s"should preinstall google cloud subpackages for ${kernel.string}" in { clusterFixture =>
        withWebDriver { implicit driver =>
          withNewNotebook(clusterFixture.cluster, kernel) { notebookPage =>
            //all other packages cannot be tested for their versions in this manner
            //warnings are ignored because they are benign warnings that show up for python2 because of compilation against an older numpy
            notebookPage.executeCell("import warnings; warnings.simplefilter('ignore')\nfrom google.cloud import bigquery\nprint(bigquery.__version__)") shouldBe Some("1.6.0")
            notebookPage.executeCell("from google.cloud import datastore\nprint(datastore.__version__)") shouldBe Some("1.7.0")
            notebookPage.executeCell("from google.cloud import storage\nprint(storage.__version__)") shouldBe Some("1.13.0")
          }
        }
      }
    }
  }
}
