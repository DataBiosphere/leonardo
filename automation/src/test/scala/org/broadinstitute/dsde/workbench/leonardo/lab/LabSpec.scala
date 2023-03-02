package org.broadinstitute.dsde.workbench.leonardo.lab

import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.RuntimeFixtureSpec
import org.scalatest.DoNotDiscover

/**
 * This spec verifies JupyterLab functionality.
 */
@DoNotDiscover
class LabSpec extends RuntimeFixtureSpec with LabTestUtils {
//   implicit def ronToken: AuthToken = ronAuthToken.unsafeRunSync()

//   "Leonardo lab" - {

//     // TODO: ignored until we officially support JupyterLab
//     "should execute cells" ignore { clusterFixture =>
//       withWebDriver { implicit driver =>
//         withNewLabNotebook(clusterFixture.runtime) { labNotebookPage =>
//           labNotebookPage.runCodeInEmptyCell("1+1") shouldBe Some("2")
//           labNotebookPage.runCodeInEmptyCell("2*3") shouldBe Some("6")
//           labNotebookPage.runCodeInEmptyCell("""print 'Hello Notebook!'""") shouldBe Some("Hello Notebook!")
//         }
//       }
//     }

// //    // TODO: enable when we support JupyterLab
// //    "should install user specified lab extensions" ignore { clusterFixture =>
// //      withProject { project => implicit token =>
// //        withNewCluster(project, request = defaultClusterRequest.copy(userJupyterExtensionConfig = Some(UserJupyterExtensionConfig(labExtensions = Map("jupyterlab-toc" -> "@jupyterlab/toc"))))) { cluster =>
// //          withWebDriver { implicit driver =>
// //            withNewNotebook(cluster) { notebookPage =>
// //              val query = """!jupyter labextension list"""
// //              val result = notebookPage.executeCell(query).get
// //              result should include("@jupyterlab/toc")
// //            }
// //          }
// //        }
// //      }
// //    }
// //
// //    // TODO: enable when we support JupyterLab
// //    "should install user specified lab extensions from a js file" ignore { clusterFixture =>
// //      withProject { project => implicit token =>
// //        val exampleLabExtensionFile = ResourceFile("bucket-tests/example_lab_extension.js")
// //        withResourceFileInBucket(project, exampleLabExtensionFile, "text/plain") { exampleLabExtensionBucketPath =>
// //          val clusterRequestWithLabExtension = defaultClusterRequest.copy(userJupyterExtensionConfig = Some(UserJupyterExtensionConfig(labExtensions = Map("example_lab_extension" -> exampleLabExtensionBucketPath.toUri))))
// //          withNewCluster(project, request = clusterRequestWithLabExtension) { cluster =>
// //            withWebDriver { implicit driver =>
// //              withNewNotebook(cluster) { notebookPage =>
// //                notebookPage.executeCell("!jupyter labextension list").get should include("example_lab_extension")
// //              }
// //            }
// //          }
// //        }
// //      }
// //    }

//   }
}
