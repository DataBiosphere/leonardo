package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.service.{Orchestration, Sam}
import org.broadinstitute.dsde.workbench.dao.Google.{googleIamDAO, googleStorageDAO}
import org.broadinstitute.dsde.workbench.model.google.GcsEntityTypes.Group
import org.broadinstitute.dsde.workbench.model.google.GcsRoles.Reader
import org.broadinstitute.dsde.workbench.model.google.{GcsEntity, GcsObjectName, GcsPath, parseGcsPath}
import org.scalatest.{FreeSpec, ParallelTestExecution}

class ClusterMonitoringSpec extends FreeSpec with LeonardoTestUtils with ParallelTestExecution {
  "Leonardo clusters" - {

    "should create, monitor, delete, recreate, and re-delete a cluster" in {
      withNewBillingProject { project =>
        Orchestration.billing.addUserToBillingProject(project.value, ronEmail, Orchestration.billing.BillingProjectRole.User)(hermioneAuthToken)
        implicit val token = ronAuthToken
        val nameToReuse = randomClusterName

        // create, monitor, delete once
        withNewCluster(project, nameToReuse)(_ => ())

        // create, monitor, delete again with same name
        withNewCluster(project, nameToReuse)(_ => ())
      }
    }

    "should error on cluster create and delete the cluster" in {
      withNewBillingProject { project =>
        Orchestration.billing.addUserToBillingProject(project.value, ronEmail, Orchestration.billing.BillingProjectRole.User)(hermioneAuthToken)
        implicit val token = ronAuthToken
        withNewErroredCluster(project) { _ =>
          // no-op; just verify that it launches
        }
      }
    }

    // default PetClusterServiceAccountProvider edition
    "should create a cluster in a different billing project using PetClusterServiceAccountProvider and put the pet's credentials on the cluster" in withWebDriver { implicit driver =>
      withNewBillingProject { project =>
        Orchestration.billing.addUserToBillingProject(project.value, ronEmail, Orchestration.billing.BillingProjectRole.User)(hermioneAuthToken)

        implicit val token = ronAuthToken
        // Pre-conditions: pet service account exists in this Google project and in Sam
        val (petName, petEmail) = getAndVerifyPet(project)

        // Create a cluster

        withNewCluster(project) { cluster =>
          // cluster should have been created with the pet service account
          cluster.serviceAccountInfo.clusterServiceAccount shouldBe Some(petEmail)
          cluster.serviceAccountInfo.notebookServiceAccount shouldBe None

          withNewNotebook(cluster) { notebookPage =>
            // should not have notebook credentials because Leo is not configured to use a notebook service account
            verifyNoNotebookCredentials(notebookPage)
          }
        }

        // Post-conditions: pet should still exist in this Google project

        implicit val patienceConfig: PatienceConfig = saPatience
        val googlePetEmail2 = googleIamDAO.findServiceAccount(project, petName).futureValue.map(_.email)
        googlePetEmail2 shouldBe Some(petEmail)
      }
    }

    // PetNotebookServiceAccountProvider edition.  IGNORE.
    "should create a cluster in a different billing project using PetNotebookServiceAccountProvider and put the pet's credentials on the cluster" ignore withWebDriver { implicit driver =>
      withNewBillingProject { project =>
        Orchestration.billing.addUserToBillingProject(project.value, ronEmail, Orchestration.billing.BillingProjectRole.User)(hermioneAuthToken)

        implicit val token = ronAuthToken
        // Pre-conditions: pet service account exists in this Google project and in Sam
        val (petName, petEmail) = getAndVerifyPet(project)

        // Create a cluster

        withNewCluster(project) { cluster =>
          // cluster should have been created with the default cluster account
          cluster.serviceAccountInfo.clusterServiceAccount shouldBe None
          cluster.serviceAccountInfo.notebookServiceAccount shouldBe Some(petEmail)

          withNewNotebook(cluster) { notebookPage =>
            // should have notebook credentials
            verifyNotebookCredentials(notebookPage, petEmail)
          }
        }

        // Post-conditions: pet should still exist in this Google project

        implicit val patienceConfig: PatienceConfig = saPatience
        val googlePetEmail2 = googleIamDAO.findServiceAccount(project, petName).futureValue.map(_.email)
        googlePetEmail2 shouldBe Some(petEmail)
      }
    }

    "should execute Hail with correct permissions on a cluster with preemptible workers" in withWebDriver { implicit driver =>
      withNewBillingProject { project =>
        Orchestration.billing.addUserToBillingProject(project.value, ronEmail, Orchestration.billing.BillingProjectRole.User)(hermioneAuthToken)

        withNewGoogleBucket(project) { bucket =>
          val srcPath = parseGcsPath("gs://genomics-public-data/1000-genomes/vcf/ALL.chr20.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf").right.get
          val destPath = GcsPath(bucket, GcsObjectName("chr20.vcf"))
          googleStorageDAO.copyObject(srcPath.bucketName, srcPath.objectName, destPath.bucketName, destPath.objectName)

          implicit val token = ronAuthToken
          val ronProxyGroup = Sam.user.proxyGroup(ronEmail)
          val ronPetEntity = GcsEntity(ronProxyGroup, Group)
          googleStorageDAO.setObjectAccessControl(destPath.bucketName, destPath.objectName, ronPetEntity, Reader)

          val request = ClusterRequest(machineConfig = Option(MachineConfig(
            // need at least 2 regular workers to enable preemptibles
            numberOfWorkers = Option(2),
            numberOfPreemptibleWorkers = Option(10)
          )))

          withNewCluster(project, request = request) { cluster =>
            withNewNotebook(cluster) { notebookPage =>
              verifyHailImport(notebookPage, destPath, cluster.clusterName)
            }
          }
        }
      }
    }


  }

}
