package org.broadinstitute.dsde.workbench.leonardo.notebooks

import java.io.File

import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.dao.Google.googleStorageDAO
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.leonardo.rstudio.RStudio
import org.broadinstitute.dsde.workbench.model.google.GcsEntityTypes.Group
import org.broadinstitute.dsde.workbench.model.google.GcsRoles.Reader
import org.broadinstitute.dsde.workbench.model.google.{parseGcsPath, EmailGcsEntity, GcsObjectName, GcsPath}
import org.broadinstitute.dsde.workbench.service.Sam
import org.broadinstitute.dsde.workbench.service.util.Tags
import org.scalatest.tagobjects.Retryable
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{DoNotDiscover, ParallelTestExecution}

import scala.concurrent.duration._
import scala.util.Try

/**
 * This spec verifies cluster status transitions like pause/resume and cluster PATCH.
 * It is similar in intent to ClusterStatusTransitionsSpec but uses notebooks for validation,
 * so lives in the notebooks sub-package.
 */
class NotebookClusterMonitoringSpec extends GPAllocFixtureSpec with ParallelTestExecution with NotebookTestUtils {

  "NotebookClusterMonitoringSpec" - {

    "should pause and resume a cluster" taggedAs Tags.SmokeTest in { billingProject =>
      implicit val ronToken: AuthToken = ronAuthToken

      // Create a cluster
      withNewCluster(billingProject) { cluster =>
        val printStr = "Pause/resume test"

        withWebDriver { implicit driver =>
          // Create a notebook and execute a cell
          withNewNotebook(cluster, kernel = Python3) { notebookPage =>
            notebookPage.executeCell(s"""print("$printStr")""") shouldBe Some(printStr)
          }

          // Stop the cluster
          stopAndMonitor(cluster.googleProject, cluster.clusterName)

          // Start the cluster
          startAndMonitor(cluster.googleProject, cluster.clusterName)

          // TODO make tests rename notebooks?
          val notebookPath = new File("Untitled.ipynb")
          // Use a longer timeout than default because opening notebooks after resume can be slow
          withOpenNotebook(cluster, notebookPath, 10.minutes) { notebookPage =>
            // old output should still exist
            val firstCell = notebookPage.firstCell
            notebookPage.cellOutput(firstCell) shouldBe Some(CellOutput(printStr, None))
            // execute a new cell to make sure the notebook kernel still works
            notebookPage.runAllCells()
            notebookPage.executeCell("sum(range(1,10))") shouldBe Some("45")
          }
        }
      }
    }

    // make sure adding a worker and changing the master machine type/disk works
    "should update the cluster to add/remove worker nodes and change master machine type/disk" in { billingProject =>
      implicit val ronToken: AuthToken = ronAuthToken

      val initialMachineConfig =
        MachineConfig(numberOfWorkers = Some(2), masterMachineType = Some("n1-standard-2"), masterDiskSize = Some(50))

      withNewCluster(billingProject, request = defaultClusterRequest.copy(machineConfig = Option(initialMachineConfig))) {
        cluster =>
          // update the cluster to add another worker node and increase the master disk
          val newMachineConfig = MachineConfig(numberOfWorkers = Some(3), masterDiskSize = Some(100))
          Leonardo.cluster.update(billingProject,
                                  cluster.clusterName,
                                  ClusterRequest(machineConfig = Option(newMachineConfig)))

          eventually(timeout(Span(60, Seconds)), interval(Span(5, Seconds))) {
            val status = Leonardo.cluster.get(billingProject, cluster.clusterName).status
            status shouldBe ClusterStatus.Updating
          }

          val timeToAddWorker = time {
            eventually(timeout(Span(420, Seconds)), interval(Span(30, Seconds))) {
              val clusterResponse = Leonardo.cluster.get(billingProject, cluster.clusterName)
              clusterResponse.machineConfig.numberOfWorkers shouldBe newMachineConfig.numberOfWorkers
              clusterResponse.machineConfig.masterMachineType shouldBe initialMachineConfig.masterMachineType
              clusterResponse.machineConfig.masterDiskSize shouldBe newMachineConfig.masterDiskSize
              clusterResponse.status shouldBe ClusterStatus.Running
            }
          }

          logger.info(
            s"Adding worker to ${cluster.projectNameString}} took ${timeToAddWorker.duration.toSeconds} seconds"
          )

          // now that we have confirmed that we can add a worker node, let's see what happens when we size it back down to 2 workers
          val twoWorkersConfig = newMachineConfig.copy(numberOfWorkers = Some(2))
          Leonardo.cluster.update(billingProject,
                                  cluster.clusterName,
                                  ClusterRequest(machineConfig = Option(twoWorkersConfig)))

          eventually(timeout(Span(60, Seconds)), interval(Span(5, Seconds))) {
            val status = Leonardo.cluster.get(billingProject, cluster.clusterName).status
            status shouldBe ClusterStatus.Updating
          }

          val timeToRemoveWorker = time {
            eventually(timeout(Span(420, Seconds)), interval(Span(30, Seconds))) {
              val clusterResponse = Leonardo.cluster.get(billingProject, cluster.clusterName)
              clusterResponse.machineConfig.numberOfWorkers shouldBe twoWorkersConfig.numberOfWorkers
              clusterResponse.machineConfig.masterMachineType shouldBe initialMachineConfig.masterMachineType
              clusterResponse.machineConfig.masterDiskSize shouldBe twoWorkersConfig.masterDiskSize
              clusterResponse.status shouldBe ClusterStatus.Running
            }
          }

          logger.info(
            s"Removing worker to ${cluster.projectNameString}} took ${timeToRemoveWorker.duration.toSeconds} seconds"
          )

          // finally, change the master machine type
          // Note this requires a cluster restart. A future enhancement may be for Leo to handle this internally.
          val newMachineTypeConfig = twoWorkersConfig.copy(masterMachineType = Some("n1-standard-4"))
          withRestartCluster(cluster) { cluster =>
            Leonardo.cluster.update(billingProject,
                                    cluster.clusterName,
                                    ClusterRequest(machineConfig = Option(newMachineTypeConfig)))
            // cluster status should still be Stopped
            val status = Leonardo.cluster.get(billingProject, cluster.clusterName).status
            status shouldBe ClusterStatus.Stopped
          }

          val clusterResponse = Leonardo.cluster.get(billingProject, cluster.clusterName)
          clusterResponse.machineConfig.numberOfWorkers shouldBe newMachineTypeConfig.numberOfWorkers
          clusterResponse.machineConfig.masterMachineType shouldBe newMachineTypeConfig.masterMachineType
          clusterResponse.machineConfig.masterDiskSize shouldBe newMachineTypeConfig.masterDiskSize
          clusterResponse.status shouldBe ClusterStatus.Running
      }
    }

    "should pause and resume a cluster with preemptible instances" in { billingProject =>
      implicit val ronToken: AuthToken = ronAuthToken

      withNewGoogleBucket(billingProject) { bucket =>
        implicit val patienceConfig: PatienceConfig = storagePatience

        val srcPath = parseGcsPath(
          "gs://genomics-public-data/1000-genomes/vcf/ALL.chr20.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf"
        ).right.get
        val destPath = GcsPath(bucket, GcsObjectName("chr20.vcf"))
        googleStorageDAO
          .copyObject(srcPath.bucketName, srcPath.objectName, destPath.bucketName, destPath.objectName)
          .futureValue

        val ronProxyGroup = Sam.user.proxyGroup(ronEmail)
        val ronPetEntity = EmailGcsEntity(Group, ronProxyGroup)
        googleStorageDAO
          .setObjectAccessControl(destPath.bucketName, destPath.objectName, ronPetEntity, Reader)
          .futureValue

        val request = defaultClusterRequest.copy(machineConfig = Option(
                    MachineConfig(
                      // need at least 2 regular workers to enable preemptibles
                      numberOfWorkers = Option(2),
                      numberOfPreemptibleWorkers = Option(10)
                    )
                  ), toolDockerImage = Some(LeonardoConfig.Leonardo.baseImageUrl))

        withNewCluster(billingProject, request = request) { cluster =>
          // Verify a Hail job uses preemptibles
          withWebDriver { implicit driver =>
            withNewNotebook(cluster, Python3) { notebookPage =>
              notebookPage.executeCell("""print("Hello Notebook!")""") shouldBe Some("Hello Notebook!")
              notebookPage.saveAndCheckpoint()
              notebookPage.shutdownKernel()
            }

            // Stop the cluster
            stopAndMonitor(cluster.googleProject, cluster.clusterName)

            // Start the cluster
            startAndMonitor(cluster.googleProject, cluster.clusterName)

            // Verify the Hail import again in a new notebook
            // Use a longer timeout than default because opening notebooks after resume can be slow
            withNewNotebook(cluster, timeout = 10.minutes) { notebookPage =>
              notebookPage.executeCell("sum(range(1,10))") shouldBe Some("45")

              // TODO: Hail verification is disabled here because Spark sometimes doesn't restart correctly
              // when a cluster is paused and then resumed. The issue is tracked here:
              // https://github.com/DataBiosphere/leonardo/issues/459
              // Re-enable this line once the above issue is fixed.
              //verifyHailImport(notebookPage, destPath, cluster.clusterName)
              logger.info(
                "ClusterMonitoringSpec: Hail verification is disabled after pause/resuming a cluster. See https://github.com/DataBiosphere/leonardo/issues/459."
              )
            }
          }
        }
      }
    }

    "should deploy welder on a cluster" taggedAs (Retryable) in { billingProject =>
      implicit val ronToken: AuthToken = ronAuthToken
      val deployWelderLabel = "saturnVersion" // matches deployWelderLabel in Leo reference.conf

      // Create a cluster with welder disabled

      withNewCluster(
        billingProject,
        request = defaultClusterRequest.copy(labels = Map(deployWelderLabel -> "true"), toolDockerImage = Some(LeonardoConfig.Leonardo.baseImageUrl), enableWelder = Some(false))
      ) { cluster =>
        withWebDriver { implicit driver =>
          // Verify welder is not running
          Welder.getWelderStatus(cluster).attempt.unsafeRunSync().isRight shouldBe false

          // Create a notebook and execute cells to create a local file
          withNewNotebook(cluster, kernel = Python3) { notebookPage =>
            notebookPage.executeCell(s"""! echo "foo" > foo.txt""") shouldBe None
            notebookPage.executeCell(s"""! cat foo.txt""") shouldBe Some("foo")
            notebookPage.saveAndCheckpoint()
          }
          // Stop the cluster
          stopAndMonitor(cluster.googleProject, cluster.clusterName)

          // Start the cluster
          startAndMonitor(cluster.googleProject, cluster.clusterName)

          // Verify welder is now running
          Welder.getWelderStatus(cluster).attempt.unsafeRunSync().isRight shouldBe true

          // Make a new notebook and verify the file still exists
          withNewNotebook(cluster, kernel = Python3) { notebookPage =>
            notebookPage.executeCell(s"""! cat foo.txt""") shouldBe Some("foo")
          }
        }
      }
    }

    "should update welder on a cluster" taggedAs Retryable in { billingProject =>
      implicit val ronToken: AuthToken = ronAuthToken
      val deployWelderLabel = "saturnVersion" // matches deployWelderLabel in Leo reference.conf

      // Create a cluster with welder disabled

      withNewCluster(
        billingProject,
        request = defaultClusterRequest.copy(labels = Map(deployWelderLabel -> "true"), welderDockerImage = Some(LeonardoConfig.Leonardo.oldWelderDockerImage), enableWelder = Some(true))
      ) { cluster =>
        // Verify welder is running with old version
        val statusResponse = Welder.getWelderStatus(cluster).attempt.unsafeRunSync()
        statusResponse.isRight shouldBe true
        val oldWelderHash = LeonardoConfig.Leonardo.oldWelderDockerImage.split(":")(1)
        statusResponse.toOption.get.gitHeadCommit should startWith(oldWelderHash)

        // Stop the cluster
        stopAndMonitor(cluster.googleProject, cluster.clusterName)

        // Start the cluster
        startAndMonitor(cluster.googleProject, cluster.clusterName)

        // Verify welder is now running
        val curWelderHash = LeonardoConfig.Leonardo.curWelderDockerImage.split(":")(1)
        val newStatusResponse = Welder.getWelderStatus(cluster).attempt.unsafeRunSync()
        newStatusResponse.isRight shouldBe true
        newStatusResponse.toOption.get.gitHeadCommit should startWith(curWelderHash)
      }
    }

    // TODO: remove this test once we stop supporting the legacy image
    "should set environment variables for old image" in { billingProject =>
      implicit val ronToken: AuthToken = ronAuthToken

      withNewCluster(billingProject,
                     request = defaultClusterRequest.copy(toolDockerImage = None, enableWelder = Some(true))) {
        cluster =>
          withWebDriver { implicit driver =>
            withNewNotebookInSubfolder(cluster, Python3) { notebookPage =>
              notebookPage.executeCell("import os")
              notebookPage.executeCell("os.getenv('GOOGLE_PROJECT')").get shouldBe s"'${billingProject.value}'"
              notebookPage.executeCell("os.getenv('WORKSPACE_NAMESPACE')").get shouldBe s"'${billingProject.value}'"
              notebookPage.executeCell("os.getenv('WORKSPACE_NAME')").get shouldBe "'Untitled Folder'"
              notebookPage.executeCell("os.getenv('OWNER_EMAIL')").get shouldBe s"'${ronEmail}'"
              // workspace bucket is not wired up in tests
              notebookPage.executeCell("os.getenv('WORKSPACE_BUCKET')") shouldBe None
            }
          }
      }
    }

    "should pause and resume an RStudio cluster" in { billingProject =>
      implicit val ronToken: AuthToken = ronAuthToken

      // Create a cluster
      withNewCluster(billingProject,
                     request = defaultClusterRequest.copy(toolDockerImage =
                                                                                 Some(LeonardoConfig.Leonardo.rstudioBaseImageUrl), enableWelder = Some(true))) { cluster =>
        // Make sure RStudio is up
        // See this ticket for adding more comprehensive selenium tests for RStudio:
        // https://broadworkbench.atlassian.net/browse/IA-697
        val getResult = Try(RStudio.getApi(cluster.googleProject, cluster.clusterName))
        getResult.isSuccess shouldBe true
        getResult.get should include("unsupported_browser")
        getResult.get should not include "ProxyException"

        // Stop the cluster
        stopAndMonitor(cluster.googleProject, cluster.clusterName)

        // Start the cluster
        startAndMonitor(cluster.googleProject, cluster.clusterName)

        // RStudio should still be up
        // TODO: also check that the session is preserved after IA-697 is done
        val getResultAfterResume = Try(RStudio.getApi(cluster.googleProject, cluster.clusterName))
        getResultAfterResume.isSuccess shouldBe true
        getResultAfterResume.get should include("unsupported_browser")
        getResultAfterResume.get should not include "ProxyException"
      }
    }

  }

}
