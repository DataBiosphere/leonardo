package org.broadinstitute.dsde.workbench.leonardo

import java.io.File

import org.broadinstitute.dsde.workbench.ResourceFile
import org.broadinstitute.dsde.workbench.service.Sam
import org.broadinstitute.dsde.workbench.dao.Google.{googleIamDAO, googleStorageDAO}
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.model.google.GcsEntityTypes.Group
import org.broadinstitute.dsde.workbench.model.google.GcsRoles.Reader
import org.broadinstitute.dsde.workbench.model.google.{EmailGcsEntity, GcsObjectName, GcsPath, parseGcsPath}
import org.scalatest.{FreeSpec, ParallelTestExecution}

import scala.util.Try

class ClusterMonitoringSpec extends FreeSpec with LeonardoTestUtils with ParallelTestExecution with BillingFixtures {

  "Leonardo clusters" - {

    // default PetClusterServiceAccountProvider edition
    "should create a cluster in a different billing project using PetClusterServiceAccountProvider and put the pet's credentials on the cluster" in {
      withProject { project => implicit token =>
        // Pre-conditions: pet service account exists in this Google project and in Sam
        val petEmail = getAndVerifyPet(project)

        // Create a cluster
        withNewCluster(project) { cluster =>
          // cluster should have been created with the pet service account
          cluster.serviceAccountInfo.clusterServiceAccount shouldBe Some(petEmail)
          cluster.serviceAccountInfo.notebookServiceAccount shouldBe None

          withWebDriver { implicit driver =>
            withNewNotebook(cluster) { notebookPage =>
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
              withNewNotebook(cluster) { notebookPage =>
                verifyHailImport(notebookPage, destPath, cluster.clusterName)
              }
            }
          }
        }
      }
    }

    "should pause and resume a cluster" in {
      withProject { project => implicit token =>
        // Create a cluster
        withNewCluster(project, monitorDelete = false) { cluster =>
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
            withOpenNotebook(cluster, notebookPath) { notebookPage =>
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

          withNewCluster(project, request = request, monitorDelete = false) { cluster =>
            // Verify a Hail job uses preemptibes
            withWebDriver { implicit driver =>
              withNewNotebook(cluster) { notebookPage =>
                verifyHailImport(notebookPage, destPath, cluster.clusterName)
                notebookPage.saveAndCheckpoint()
              }

              // Stop the cluster
              stopAndMonitor(cluster.googleProject, cluster.clusterName)

              // Start the cluster
              startAndMonitor(cluster.googleProject, cluster.clusterName)

              // Verify the Hail import again in a new notebook
              withNewNotebook(cluster) { notebookPage =>
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
          withNewCluster(project, clusterName, ClusterRequest(userJupyterExtensionConfig = Some(extensionConfig))) { cluster =>
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

    "should install JupyterLab" in {
      withProject { project => implicit token =>
        withNewCluster(project, request = ClusterRequest(userJupyterExtensionConfig = Some(jupyterLabExtensionClusterRequest))) { cluster =>
          withWebDriver { implicit driver =>
            // Check that the /lab URL is accessible
            val getResult = Try(Leonardo.lab.getApi(project, cluster.clusterName))
            getResult.isSuccess shouldBe true
            getResult.get should not include "ProxyException"

            // Check that localization still works
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
              Leonardo.notebooks.localize(cluster.googleProject, cluster.clusterName, localizeRequest, async = false)

              // check that the files are immediately at their destinations
              verifyLocalizeDelocalize(cluster, localizeFileName, localizeFileContents, GcsPath(bucketName, GcsObjectName(delocalizeFileName)), delocalizeFileContents, localizeDataFileName, localizeDataContents)
            }
          }
        }
      }
    }
  }

}
