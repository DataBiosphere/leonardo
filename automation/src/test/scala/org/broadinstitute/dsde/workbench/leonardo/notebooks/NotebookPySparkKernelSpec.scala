package org.broadinstitute.dsde.workbench.leonardo.notebooks

import org.broadinstitute.dsde.workbench.leonardo.ClusterFixtureSpec

import scala.language.postfixOps

class NotebookPySparkKernelSpec extends ClusterFixtureSpec with NotebookTestUtils {


  "Leonardo notebooks" - {

    val sparkJobToSucceed =
      """import random
        |NUM_SAMPLES=20
        |def inside(p):
        |    x, y = random.random(), random.random()
        |    return x*x + y*y < 1
        |
        |count = sc.parallelize(range(0, NUM_SAMPLES)) \
        |             .filter(inside).count()
        |print("Pi is roughly %f" % (4.0 * count / NUM_SAMPLES))""".stripMargin

    Seq(PySpark2, PySpark3).foreach { kernel =>

      s"should be able to run a Spark job with a ${kernel.string} kernel" in { clusterFixture =>
        withWebDriver { implicit driver =>
          withNewNotebook(clusterFixture.cluster, kernel) { notebookPage =>
            val cellResult = notebookPage.executeCell(sparkJobToSucceed).get
            cellResult should include("Pi is roughly ")
            cellResult.toLowerCase should not include "error"
          }
        }
      }
    }
  }

}
