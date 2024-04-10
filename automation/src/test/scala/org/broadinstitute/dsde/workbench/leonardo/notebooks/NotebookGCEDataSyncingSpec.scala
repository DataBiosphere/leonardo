package org.broadinstitute.dsde.workbench.leonardo.notebooks

import cats.effect.unsafe.implicits.global

import java.io.File
import java.math.BigInteger
import java.security.MessageDigest
import java.time.Instant
import org.broadinstitute.dsde.workbench.ResourceFile
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.leonardo._
import org.scalatest.{DoNotDiscover, ParallelTestExecution}
import org.scalatest.time.{Minutes, Seconds, Span}
import org.scalatest.tagobjects.Retryable

import scala.concurrent.duration._

/**
 * This spec verifies data syncing functionality, including notebook edit mode, playground mode,
 * and welder localization/delocalization.
 */
@DoNotDiscover
class NotebookGCEDataSyncingSpec extends RuntimeFixtureSpec with NotebookTestUtils with ParallelTestExecution {
  override def enableWelder: Boolean = true

  implicit def ronToken: AuthToken = ronAuthToken.unsafeRunSync()

  "NotebookGCEDataSyncingSpec" - {

    "Welder should be up" in { runtimeFixture =>
      val resp = Welder.getWelderStatus(runtimeFixture.runtime)
      resp.attempt.unsafeRunSync().isRight shouldBe true
    }

    "open notebook in edit mode should work" in { runtimeFixture =>
      val sampleNotebook = ResourceFile("bucket-tests/gcsFile.ipynb")
      val isEditMode = true
      val isRStudio = false

      withResourceFileInBucket(runtimeFixture.runtime.googleProject, sampleNotebook, "text/plain") { gcsPath =>
        withWelderInitialized(runtimeFixture.runtime, gcsPath, ".*.ipynb", isEditMode, isRStudio) { localizedFile =>
          withWebDriver { implicit driver =>
            withOpenNotebook(runtimeFixture.runtime, localizedFile, 5.minutes) { notebookPage =>
              notebookPage.modeExists() shouldBe true
              notebookPage.getMode() shouldBe NotebookMode.EditMode
              notebookPage.addCodeAndExecute("1+1")
              notebookPage.saveNotebook()

              val localContent: NotebookContentItem =
                JupyterServerClient.getNotebookItem(runtimeFixture.runtime.googleProject,
                                                    runtimeFixture.runtime.clusterName,
                                                    Welder.getLocalPath(gcsPath, isEditMode, isRStudio)
                )
              logger.info(s"[edit mode] local content is ${localContent}")
              val localContentSize: Int = localContent.size

              eventually(timeout(Span(5, Seconds))) {
                val remoteContentSize: Int = getObjectSize(gcsPath.bucketName, GcsBlobName(gcsPath.objectName.value))
                  .unsafeRunSync()

                logger.info(s"[edit mode] remote content size is ${remoteContentSize}")

                remoteContentSize shouldBe localContentSize
              }

              val hashedUser = MessageDigest
                .getInstance("SHA-256")
                .digest((gcsPath.bucketName + ":" + runtimeFixture.runtime.creator).getBytes("UTF-8"))
              val formattedHashedUser = Some(String.format("%064x", new BigInteger(1, hashedUser)))

              eventually(timeout(Span(4, Minutes)), interval(Span(30, Seconds))) {
                val gcsLockedBy: Option[String] =
                  getLockedBy(gcsPath.bucketName, GcsBlobName(gcsPath.objectName.value)).unsafeRunSync()
                val welderLockedBy: Option[String] =
                  Welder.getMetadata(runtimeFixture.runtime, gcsPath, isEditMode, isRStudio).lastLockedBy

                gcsLockedBy should not be None
                welderLockedBy should not be None
                gcsLockedBy shouldBe welderLockedBy
                gcsLockedBy shouldBe formattedHashedUser
              }
            }
          }
        }
      }
    }

    "open notebook in playground mode should work" taggedAs Retryable in { runtimeFixture =>
      val sampleNotebook = ResourceFile("bucket-tests/gcsFile2.ipynb")
      val isEditMode = false
      val isRStudio = false

      withResourceFileInBucket(runtimeFixture.runtime.googleProject, sampleNotebook, "text/plain") { gcsPath =>
        withWelderInitialized(runtimeFixture.runtime, gcsPath, ".*.ipynb", isEditMode, isRStudio) { localizedFile =>
          withWebDriver { implicit driver =>
            withOpenNotebook(runtimeFixture.runtime, localizedFile, 5.minutes) { notebookPage =>
              val originalRemoteContentSize: Int =
                getObjectSize(gcsPath.bucketName, GcsBlobName(gcsPath.objectName.value))
                  .unsafeRunSync()
              logger.info(s"[playground mode] original remote content size is ${originalRemoteContentSize}")

              val originalLocalContent: NotebookContentItem =
                JupyterServerClient.getNotebookItem(runtimeFixture.runtime.googleProject,
                                                    runtimeFixture.runtime.clusterName,
                                                    Welder.getLocalPath(gcsPath, isEditMode, isRStudio)
                )
              logger.info(s"[playground mode] original local content is ${originalLocalContent}")
              val originalLocalContentSize: Int = originalLocalContent.size

              originalRemoteContentSize shouldBe originalLocalContentSize +- 1

              notebookPage.modeExists() shouldBe true
              notebookPage.getMode() shouldBe NotebookMode.SafeMode
              notebookPage.addCodeAndExecute("1+1")
              notebookPage.saveNotebook()

              // sleep 4 minutes. We do this to ensure the assertions are true after a certain about of time.
              // eventually is not applicable as we want to ensure its true after the time period, not at some point until timeout
              logger.info("Waiting 4 minutes as lock takes time to be reflected in metadata")
              Thread.sleep(240000)

              eventually(timeout(Span(5, Seconds))) {
                val newLocalContent: NotebookContentItem =
                  JupyterServerClient.getNotebookItem(runtimeFixture.runtime.googleProject,
                                                      runtimeFixture.runtime.clusterName,
                                                      Welder.getLocalPath(gcsPath, isEditMode, isRStudio)
                  )
                val newLocalContentSize: Int = newLocalContent.size
                logger.info(s"[playground mode] new local content is ${newLocalContent}")
                val newRemoteContentSize = getObjectSize(gcsPath.bucketName, GcsBlobName(gcsPath.objectName.value))
                  .unsafeRunSync()
                logger.info(s"[playground mode] new remote content size is ${newRemoteContentSize}")

                newLocalContentSize should be > newRemoteContentSize
                originalRemoteContentSize shouldBe newRemoteContentSize
              }

              // some selectors are omitted to simplify the test with the assumption that if the majority are hidden, they all are
              val uiElementIds: List[String] = List("save-notbook",
                                                    "new_notebook",
                                                    "open_notebook",
                                                    "copy_notebook",
                                                    "save_notebook_as",
                                                    "save_checkpoint",
                                                    "restore_checkpoint",
                                                    "notification_notebook"
              )
              val areElementsHidden: Boolean = notebookPage.areElementsHidden(uiElementIds)

              areElementsHidden shouldBe true

              val gcsLockedBy: Option[String] =
                getLockedBy(gcsPath.bucketName, GcsBlobName(gcsPath.objectName.value)).unsafeRunSync()
              val welderLockedBy: Option[String] =
                Welder.getMetadata(runtimeFixture.runtime, gcsPath, isEditMode, isRStudio).lastLockedBy

              gcsLockedBy shouldBe None
              welderLockedBy shouldBe None
            }
          }
        }
      }
    }

    "Sync issues and make a copy handled transition correctly" in { runtimeFixture =>
      val fileName = "gcsFile3" // we store this portion separately as the name of the copy is computed off it
      val sampleNotebook = ResourceFile(s"bucket-tests/${fileName}.ipynb")
      val isEditMode = true
      val isRStudio = false

      withResourceFileInBucket(runtimeFixture.runtime.googleProject, sampleNotebook, "text/plain") { gcsPath =>
        withWelderInitialized(runtimeFixture.runtime, gcsPath, ".*.ipynb", isEditMode, isRStudio) { localizedFile =>
          withWebDriver { implicit driver =>
            withOpenNotebook(runtimeFixture.runtime, localizedFile, 5.minutes) { notebookPage =>
              val contents =
                "{\n      'cells': [],\n      'metadata': {\n        'kernelspec': {\n        'display_name': 'Python 3',\n        'language': 'python',\n        'name': 'python3'\n      },\n        'language_info': {\n        'codemirror_mode': {\n        'name': 'ipython',\n        'version': 3\n      },\n        'file_extension': '.py',\n        'mimetype': 'text/x-python',\n        'name': 'python',\n        'nbconvert_exporter': 'python',\n        'pygments_lexer': 'ipython3',\n        'version': '3.10.9'\n      }\n      },\n      'nbformat': 4,\n      'nbformat_minor': 2\n    }"
              setObjectContents(runtimeFixture.runtime.googleProject,
                                gcsPath.bucketName,
                                GcsBlobName(gcsPath.objectName.value),
                                contents
              ).unsafeRunSync

              val syncIssueElements =
                List(notebookPage.syncCopyButton, notebookPage.syncReloadButton, notebookPage.modalId)

              notebookPage.addCodeAndExecute("%autosave 0")

              eventually(timeout(Span(2, Minutes)), interval(Span(30, Seconds))) { // wait for checkMeta tick
                notebookPage areElementsPresent syncIssueElements shouldBe true
                notebookPage executeJavaScript "window.onbeforeunload = null;" // disables pesky chrome modal to confirm navigation. we are not testing chrome's implementation and confirming the modal proves problematic

                notebookPage makeACopyFromSyncIssue
              }

              eventually(timeout(Span(30, Seconds))) { // wait for the page to reload
                driver.getCurrentUrl should include(fileName + "-Copy")
              }
            }
          }
        }
      }
    }

    "Locked by another user and playground mode transition handled correctly" in { runtimeFixture =>
      val sampleNotebook = ResourceFile("bucket-tests/gcsFile4.ipynb")
      val isEditMode = true
      val isRStudio = false

      withResourceFileInBucket(runtimeFixture.runtime.googleProject, sampleNotebook, "text/plain") { gcsPath =>
        // we set the lock before the notebook is open to cause a conflict
        val newMeta = Map("lockExpiresAt" -> Instant.now().plusMillis(20.minutes.toMillis).toEpochMilli.toString,
                          "lastLockedBy" -> "NotMe"
        )
        setObjectMetadata(gcsPath.bucketName, GcsBlobName(gcsPath.objectName.value), newMeta).unsafeRunSync

        withWelderInitialized(runtimeFixture.runtime, gcsPath, ".*.ipynb", isEditMode, isRStudio) { localizedFile =>
          withWebDriver { implicit driver =>
            withOpenNotebook(runtimeFixture.runtime, localizedFile, 5.minutes) { notebookPage =>
              eventually(timeout(Span(2, Minutes))) { // wait for checkMeta tick
                val lockIssueElements =
                  List(notebookPage.lockPlaygroundButton, notebookPage.lockCopyButton, notebookPage.modalId)

                notebookPage.areElementsPresent(lockIssueElements) shouldBe true

                notebookPage.executeJavaScript(
                  "window.onbeforeunload = null;"
                ) // disables pesky chrome modal to confirm navigation. we are not testing chrome's implementation and confirming the modal proves problematic
                notebookPage.goToPlaygroundModeFromLockIssue
              }

              eventually(timeout(Span(30, Seconds))) { // wait for the page to reload
                driver.getCurrentUrl should include(Welder.localSafeModeBaseDirectory)
                notebookPage.getMode shouldBe NotebookMode.SafeMode
              }
            }
          }
        }
      }
    }

    // this test is important to make sure all components exit gracefully when their functionality is not needed
    "User should be able to create files outside of playground and safe mode" in { runtimeFixture =>
      val fileName = "mockUserFile.ipynb"

      val mockUserFile: File = JupyterServerClient.createFileAtJupyterRoot(runtimeFixture.runtime.googleProject,
                                                                           runtimeFixture.runtime.clusterName,
                                                                           fileName
      )

      withWebDriver { implicit driver =>
        withOpenNotebook(runtimeFixture.runtime, mockUserFile, 5.minutes) { notebookPage =>
          notebookPage.modeExists() shouldBe false
          notebookPage.areElementsPresent(List(notebookPage.noModeBannerId)) shouldBe true
          notebookPage.getMode() shouldBe NotebookMode.NoMode
        }
      }
    }

  }
}
