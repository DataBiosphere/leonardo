package org.broadinstitute.dsde.workbench.leonardo.notebooks

import java.io.File

import org.broadinstitute.dsde.workbench.ResourceFile
import org.broadinstitute.dsde.workbench.service.Sam
import org.broadinstitute.dsde.workbench.dao.Google.{googleIamDAO, googleStorageDAO}
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.leonardo.Leonardo.ApiVersion.V2
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.model.google.GcsEntityTypes.Group
import org.broadinstitute.dsde.workbench.model.google.GcsRoles.Reader
import org.broadinstitute.dsde.workbench.model.google.{EmailGcsEntity, GcsObjectName, GcsPath, parseGcsPath}
import org.broadinstitute.dsde.workbench.service.Sam
import org.broadinstitute.dsde.workbench.service.util.Tags
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{FreeSpec, ParallelTestExecution}

import scala.concurrent.duration._

class NotebookClusterMonitoringSpec extends FreeSpec with NotebookTestUtils with ParallelTestExecution with BillingFixtures {

  "Leonardo clusters" - {

    // default PetClusterServiceAccountProvider edition
    "should create a cluster in a different billing project using PetClusterServiceAccountProvider and put the pet's credentials on the cluster" in {
      withProject { project => implicit token =>
        // Pre-conditions: pet service account exists in this Google project and in Sam
        val petEmail = getAndVerifyPet(project)

        // Create a cluster
        withNewCluster(project, apiVersion = V2, monitorDelete = true) { cluster =>
          // cluster should have been created with the pet service account
          cluster.serviceAccountInfo.clusterServiceAccount shouldBe Some(petEmail)
          cluster.serviceAccountInfo.notebookServiceAccount shouldBe None

          withWebDriver { implicit driver =>
            withNewNotebook(cluster, PySpark2) { notebookPage =>
              // should not have notebook credentials because Leo is not configured to use a notebook service account
              verifyNoNotebookCredentials(notebookPage)
            }
          }
        }

        // Post-conditions: pet should still exist in this Google project

        implicit val patienceConfig: PatienceConfig = saPatience
        val googlePetEmail2 = googleIamDAO.findServiceAccount(project, petEmail).futureValue.map(_.email)
        googlePetEmail2 shouldBe Some(petEmail)
      }
    }

    // PetNotebookServiceAccountProvider edition.  IGNORE.
    "should create a cluster in a different billing project using PetNotebookServiceAccountProvider and put the pet's credentials on the cluster" ignore {
      withProject { project => implicit token =>
        // Pre-conditions: pet service account exists in this Google project and in Sam
        val petEmail = getAndVerifyPet(project)

        // Create a cluster

        withNewCluster(project) { cluster =>
          // cluster should have been created with the default cluster account
          cluster.serviceAccountInfo.clusterServiceAccount shouldBe None
          cluster.serviceAccountInfo.notebookServiceAccount shouldBe Some(petEmail)

          withWebDriver { implicit driver =>
            withNewNotebook(cluster) { notebookPage =>
              // should have notebook credentials
              verifyNotebookCredentials(notebookPage, petEmail)
            }
          }
        }

        // Post-conditions: pet should still exist in this Google project

        implicit val patienceConfig: PatienceConfig = saPatience
        val googlePetEmail2 = googleIamDAO.findServiceAccount(project, petEmail).futureValue.map(_.email)
        googlePetEmail2 shouldBe Some(petEmail)
      }
    }

    // TODO: we've noticed intermittent failures for this test. See:
    // https://github.com/DataBiosphere/leonardo/issues/204
    // https://github.com/DataBiosphere/leonardo/issues/228
    "should execute Hail with correct permissions on a cluster with preemptible workers" in {
      withProject { project => implicit token =>
        withNewGoogleBucket(project) { bucket =>
          implicit val patienceConfig: PatienceConfig = storagePatience

          val srcPath = parseGcsPath("gs://genomics-public-data/1000-genomes/vcf/ALL.chr20.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf").right.get
          val destPath = GcsPath(bucket, GcsObjectName("chr20.vcf"))
          googleStorageDAO.copyObject(srcPath.bucketName, srcPath.objectName, destPath.bucketName, destPath.objectName).futureValue

          val ronProxyGroup = Sam.user.proxyGroup(ronEmail)
          val ronPetEntity = EmailGcsEntity(Group, ronProxyGroup)
          googleStorageDAO.setObjectAccessControl(destPath.bucketName, destPath.objectName, ronPetEntity, Reader).futureValue

          val request = ClusterRequest(machineConfig = Option(MachineConfig(
            // need at least 2 regular workers to enable preemptibles
            numberOfWorkers = Option(2),
            numberOfPreemptibleWorkers = Option(10)
          )))

          withNewCluster(project, request = request) { cluster =>
            withWebDriver { implicit driver =>
              withNewNotebook(cluster, PySpark3) { notebookPage =>
                verifyHailImport(notebookPage, destPath, cluster)
              }
            }
          }
        }
      }
    }

    "should pause and resume a cluster" taggedAs Tags.SmokeTest in {
      withProject { project => implicit token =>
        // Create a cluster
        withNewCluster(project, apiVersion = V2) { cluster =>
          val printStr = "Pause/resume test"

          withWebDriver { implicit driver =>
            // Create a notebook and execute a cell
            withNewNotebook(cluster, kernel = Python3) { notebookPage =>
              notebookPage.executeCell(s"""print("$printStr")""") shouldBe Some(printStr)
              notebookPage.saveAndCheckpoint()
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
              notebookPage.cellOutput(firstCell) shouldBe Some(printStr)
              // execute a new cell to make sure the notebook kernel still works
              notebookPage.runAllCells()
              notebookPage.executeCell("sum(range(1,10))") shouldBe Some("45")
            }
          }
        }
      }
    }

    // make sure adding a worker and changing the master machine type/disk works
    "should update the cluster to add/remove worker nodes and change master machine type/disk" in {
      withProject { project => implicit token =>
        val initialMachineConfig = MachineConfig(numberOfWorkers = Some(2), masterMachineType = Some("n1-standard-2"), masterDiskSize = Some(50))

        withNewCluster(project, request = defaultClusterRequest.copy(machineConfig = Option(initialMachineConfig))) { cluster =>
          // update the cluster to add another worker node and increase the master disk
          val newMachineConfig = MachineConfig(numberOfWorkers = Some(3), masterDiskSize = Some(100))
          Leonardo.cluster.update(project, cluster.clusterName, ClusterRequest(machineConfig = Option(newMachineConfig)))

          eventually(timeout(Span(60, Seconds)), interval(Span(5, Seconds))) {
            val status = Leonardo.cluster.get(project, cluster.clusterName).status
            status shouldBe ClusterStatus.Updating
          }

          val timeToAddWorker = time {
            eventually(timeout(Span(420, Seconds)), interval(Span(30, Seconds))) {
              val clusterResponse = Leonardo.cluster.get(project, cluster.clusterName)
              clusterResponse.machineConfig.numberOfWorkers shouldBe newMachineConfig.numberOfWorkers
              clusterResponse.machineConfig.masterMachineType shouldBe initialMachineConfig.masterMachineType
              clusterResponse.machineConfig.masterDiskSize shouldBe newMachineConfig.masterDiskSize
              clusterResponse.status shouldBe ClusterStatus.Running
            }
          }

          logger.info(s"Adding worker to ${cluster.projectNameString}} took ${timeToAddWorker.duration.toSeconds} seconds")

          // now that we have confirmed that we can add a worker node, let's see what happens when we size it back down to 2 workers
          val twoWorkersConfig = newMachineConfig.copy(numberOfWorkers = Some(2))
          Leonardo.cluster.update(project, cluster.clusterName, ClusterRequest(machineConfig = Option(twoWorkersConfig)))

          eventually(timeout(Span(60, Seconds)), interval(Span(5, Seconds))) {
            val status = Leonardo.cluster.get(project, cluster.clusterName).status
            status shouldBe ClusterStatus.Updating
          }

          val timeToRemoveWorker = time {
            eventually(timeout(Span(420, Seconds)), interval(Span(30, Seconds))) {
              val clusterResponse = Leonardo.cluster.get(project, cluster.clusterName)
              clusterResponse.machineConfig.numberOfWorkers shouldBe twoWorkersConfig.numberOfWorkers
              clusterResponse.machineConfig.masterMachineType shouldBe initialMachineConfig.masterMachineType
              clusterResponse.machineConfig.masterDiskSize shouldBe twoWorkersConfig.masterDiskSize
              clusterResponse.status shouldBe ClusterStatus.Running
            }
          }

          logger.info(s"Removing worker to ${cluster.projectNameString}} took ${timeToRemoveWorker.duration.toSeconds} seconds")

          // finally, change the master machine type
          // Note this requires a cluster restart. A future enhancement may be for Leo to handle this internally.
          val newMachineTypeConfig = twoWorkersConfig.copy(masterMachineType = Some("n1-standard-4"))
          withRestartCluster(cluster) { cluster =>
            Leonardo.cluster.update(project, cluster.clusterName, ClusterRequest(machineConfig = Option(newMachineTypeConfig)))
            // cluster status should still be Stopped
            val status = Leonardo.cluster.get(project, cluster.clusterName).status
            status shouldBe ClusterStatus.Stopped
          }

          val clusterResponse = Leonardo.cluster.get(project, cluster.clusterName)
          clusterResponse.machineConfig.numberOfWorkers shouldBe newMachineTypeConfig.numberOfWorkers
          clusterResponse.machineConfig.masterMachineType shouldBe newMachineTypeConfig.masterMachineType
          clusterResponse.machineConfig.masterDiskSize shouldBe newMachineTypeConfig.masterDiskSize
          clusterResponse.status shouldBe ClusterStatus.Running
        }
      }
    }

    "should pause and resume a cluster with preemptible instances" in {
      withProject { project => implicit token =>
        withNewGoogleBucket(project) { bucket =>
          implicit val patienceConfig: PatienceConfig = storagePatience

          val srcPath = parseGcsPath("gs://genomics-public-data/1000-genomes/vcf/ALL.chr20.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf").right.get
          val destPath = GcsPath(bucket, GcsObjectName("chr20.vcf"))
          googleStorageDAO.copyObject(srcPath.bucketName, srcPath.objectName, destPath.bucketName, destPath.objectName).futureValue

          val ronProxyGroup = Sam.user.proxyGroup(ronEmail)
          val ronPetEntity = EmailGcsEntity(Group, ronProxyGroup)
          googleStorageDAO.setObjectAccessControl(destPath.bucketName, destPath.objectName, ronPetEntity, Reader).futureValue

          val request = ClusterRequest(machineConfig = Option(MachineConfig(
            // need at least 2 regular workers to enable preemptibles
            numberOfWorkers = Option(2),
            numberOfPreemptibleWorkers = Option(10)
          )))

          withNewCluster(project, request = request) { cluster =>
            // Verify a Hail job uses preemptibles
            withWebDriver { implicit driver =>
              withNewNotebook(cluster, PySpark3) { notebookPage =>
                verifyHailImport(notebookPage, destPath, cluster)
                notebookPage.saveAndCheckpoint()
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
                logger.info("ClusterMonitoringSpec: Hail verification is disabled after pause/resuming a cluster. See https://github.com/DataBiosphere/leonardo/issues/459.")

                notebookPage.saveAndCheckpoint()
              }
            }
          }
        }
      }
    }

    //Test to check if extensions are installed correctly
    //Using nbtranslate extension from here:
    //https://github.com/ipython-contrib/jupyter_contrib_nbextensions/tree/master/src/jupyter_contrib_nbextensions/nbextensions/nbTranslate
    "should install a single user specified notebook extension" in {
      withProject { project => implicit token =>
        val translateExtensionFile = ResourceFile("bucket-tests/translate_nbextension.tar.gz")
        withResourceFileInBucket(project, translateExtensionFile, "application/x-gzip") { translateExtensionBucketPath =>
          val clusterName = ClusterName("user-jupyter-ext" + makeRandomId())
          withNewCluster(project, clusterName, ClusterRequest(Map(), Option(translateExtensionBucketPath.toUri), None)) { cluster =>
            withWebDriver { implicit driver =>
              withNewNotebook(cluster) { notebookPage =>
                notebookPage.executeCell("1 + 1") shouldBe Some("2")
                //Check if the mark up was translated correctly
                notebookPage.translateMarkup("Hello") should include("Bonjour")
              }
            }
          }
        }
      }
    }

    "should install multiple user specified notebook extensions" in {
      withProject { project => implicit token =>
        val translateExtensionFile = ResourceFile("bucket-tests/translate_nbextension.tar.gz")
        withResourceFileInBucket(project, translateExtensionFile, "application/x-gzip") { translateExtensionBucketPath =>
          val clusterName = ClusterName("user-jupyter-ext" + makeRandomId())
          val extensionConfig = multiExtensionClusterRequest.copy(nbExtensions = multiExtensionClusterRequest.nbExtensions + ("translate" -> translateExtensionBucketPath.toUri))
          withNewCluster(project, clusterName, ClusterRequest(userJupyterExtensionConfig = Some(extensionConfig)), apiVersion = V2) { cluster =>
            withWebDriver { implicit driver =>
              withNewNotebook(cluster, Python3) { notebookPage =>
                //Check if the mark up was translated correctly
                val nbExt = notebookPage.executeCell("! jupyter nbextension list")
                nbExt.get should include("jupyter-gmaps/extension  enabled")
                nbExt.get should include("pizzabutton/index  enabled")
                nbExt.get should include("translate_nbextension/main  enabled")
                val serverExt = notebookPage.executeCell("! jupyter serverextension list")
                serverExt.get should include("pizzabutton  enabled")
                serverExt.get should include("jupyterlab  enabled")
              }
            }
          }
        }
      }
    }

    "should localize/delocalize" taggedAs Tags.SmokeTest in {
      withProject { project => implicit token =>
        withNewCluster(project, request = ClusterRequest()) { cluster =>
          withWebDriver { implicit driver =>
            // Check that localization works
            // See https://github.com/DataBiosphere/leonardo/issues/417, where installing JupyterLab
            // broke the initialization of jupyter_localize_extension.py.
            val localizeFileName = "localize_sync.txt"
            val localizeFileContents = "Sync localize test"
            val delocalizeFileName = "delocalize_sync.txt"
            val delocalizeFileContents = "Sync delocalize test"
            val localizeDataFileName = "localize_data_aync.txt"
            val localizeDataContents = "Hello World"

            withLocalizeDelocalizeFiles(cluster, localizeFileName, localizeFileContents, delocalizeFileName, delocalizeFileContents, localizeDataFileName, localizeDataContents) { (localizeRequest, bucketName, notebookPage) =>
              // call localize; this should return 200
              Notebook.localize(cluster.googleProject, cluster.clusterName, localizeRequest, async = false)

              // check that the files are immediately at their destinations
              verifyLocalizeDelocalize(cluster, localizeFileName, localizeFileContents, GcsPath(bucketName, GcsObjectName(delocalizeFileName)), delocalizeFileContents, localizeDataFileName, localizeDataContents)
            }
          }
        }
      }
    }

    "should give cluster user-specified scopes" taggedAs Tags.SmokeTest in {
      withProject { project => implicit token =>
        withNewCluster(project, request = ClusterRequest(scopes = Some(Set("https://www.googleapis.com/auth/userinfo.email", "https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/source.read_only")))) { cluster =>
          withWebDriver { implicit driver =>
            withNewNotebook(cluster) { notebookPage =>
              val query = """! bq query --disable_ssl_validation --format=json "SELECT COUNT(*) AS scullion_count FROM publicdata.samples.shakespeare WHERE word='scullion'" """

              val result = notebookPage.executeCell(query, timeout = 5.minutes).get
              result should include("Invalid credential")
            }
          }
        }
      }
    }

    "should download file as pdf" ignore {
      withProject { project => implicit token =>
        withNewGoogleBucket(project) { bucketName =>
          val enablePdfDownloadScript = ResourceFile("bucket-tests/enable_download_as_pdf.sh")
          withResourceFileInBucket(project, enablePdfDownloadScript, "text/plain") { bucketPath =>
            val clusterName = ClusterName("user-script-cluster" + makeRandomId())
            withNewCluster(project, clusterName, ClusterRequest(Map(), None, Option(bucketPath.toUri)), apiVersion = V2) { cluster =>
              val download = createDownloadDirectory()
              withWebDriver(download) { implicit driver =>
                withNewNotebook(cluster) { notebookPage =>
                  notebookPage.executeCell("1+1") shouldBe Some("2")
                  notebookPage.downloadAsPdf()
                  val notebookName = notebookPage.currentUrl.substring(notebookPage.currentUrl.lastIndexOf('/') + 1, notebookPage.currentUrl.lastIndexOf('?')).replace(".ipynb", ".pdf")
                  // sanity check the file downloaded correctly
                  val downloadFile = new File(download, notebookName)
                  downloadFile.deleteOnExit()
                  logger.info(s"download: $downloadFile")
                  implicit val patienceConfig: PatienceConfig = getAfterCreatePatience
                  eventually {
                    assert(downloadFile.exists(), s"Timed out (${patienceConfig.timeout} seconds) waiting for file.exists $downloadFile")
                    assert(downloadFile.isFile(), s"Timed out (${patienceConfig.timeout} seconds) waiting for file.isFile $downloadFile")
                  }
                }
              }
            }(ronAuthToken)
          }
        }
      }
    }

    "should throw 401 for invalid token" in {
      withProject { project => implicit token =>
        withNewCluster(project, monitorCreate = true, apiVersion = V2) { cluster =>
          withWebDriver { implicit driver =>
            val thrown = the[Exception] thrownBy {
              Notebook.setCookie(cluster.googleProject, cluster.clusterName)(voldyAuthToken, driver)
            }
            thrown.getMessage should include(""""statusCode":401""")
          }
        }
      }
    }
  }

}
