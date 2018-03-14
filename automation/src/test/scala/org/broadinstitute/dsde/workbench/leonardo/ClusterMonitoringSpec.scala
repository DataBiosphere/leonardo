package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.service.{Orchestration, Sam}
import org.broadinstitute.dsde.workbench.dao.Google.{googleIamDAO, googleStorageDAO}
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterRequest
import org.broadinstitute.dsde.workbench.leonardo.model.google.MachineConfig
import org.broadinstitute.dsde.workbench.model.google.GcsEntityTypes.Group
import org.broadinstitute.dsde.workbench.model.google.GcsRoles.Reader
import org.broadinstitute.dsde.workbench.model.google.{GcsEntity, GcsObjectName, GcsPath, GoogleProject, parseGcsPath}
import org.scalatest.{FreeSpec, ParallelTestExecution}

class ClusterMonitoringSpec extends FreeSpec with LeonardoTestUtils with ParallelTestExecution with BillingFixtures {
  "Leonardo clusters" - {

    /*
    [info] - should create, monitor, delete, recreate, and re-delete a cluster *** FAILED ***
[info]   com.fasterxml.jackson.databind.exc.MismatchedInputException: Cannot construct instance of
`org.broadinstitute.dsde.workbench.model.google.GcsBucketName` (although at least one Creator exists):
no String-argument constructor/factory method to deserialize from String value ('automation-test-ai2xqdpyz--99c63a33-ce31-452b-b21d-536a8681f650')

[info]  at [Source: (String)"{"stagingBucket":"automation-test-ai2xqdpyz--99c63a33-ce31-452b-b21d-536a8681f650",
"errors":[],"machineConfig":{"numberOfWorkers":0,"masterMachineType":"n1-standard-4","masterDiskSize":500},
"creator":"ron.weasley@test.firecloud.org","googleProject":"gpalloc-dev-master-8ozyecb","labels":{"creator":"ron.weasley@test.firecloud.org",
"googleProject":"gpalloc-dev-master-8ozyecb","clusterServiceAccount":"pet-114763077412354570085@gpalloc-dev-master-8ozyecb.iam.gserviceaccount.com",
"clusterName":"automa"[truncated 562 chars]; line: 1, column: 18]


(through reference chain: org.broadinstitute.dsde.workbench.leonardo.model.Cluster["stagingBucket"])
[info]   at com.fasterxml.jackson.databind.exc.MismatchedInputException.from(MismatchedInputException.java:63)

     */


    "should create, monitor, delete, recreate, and re-delete a cluster" in {
      withCleanBillingProject(hermioneCreds) { projectName =>
        Orchestration.billing.addUserToBillingProject(projectName, ronEmail, Orchestration.billing.BillingProjectRole.User)(hermioneAuthToken)
        val project = GoogleProject(projectName)
        implicit val token = ronAuthToken
        val nameToReuse = randomClusterName

        // create, monitor, delete once
        withNewCluster(project, nameToReuse)(_ => ())

        // create, monitor, delete again with same name
        withNewCluster(project, nameToReuse)(_ => ())
      }
    }
/*
[akka.actor.ActorSystemImpl(leonardo)] Rejected(List(MalformedRequestContentRejection(Expected bucket URI, got: {"bucketName":{"value":"gs://leonardo-swat-test-bucket-do-not-delete"},"objectName":{"value":"","timeCreated":{"nano":0,"epochSecond":-31557014167219200}}},
      spray.json.DeserializationException: Expected bucket URI, got: {"bucketName":{"value":"gs://leonardo-swat-test-bucket-do-not-delete"},"objectName":{"value":"","timeCreated":{"nano":0,"epochSecond":-31557014167219200}}}),
      TransformationRejection(akka.http.scaladsl.server.directives.BasicDirectives$$Lambda$1576/1488646382@5fb653bb), MethodRejection(HttpMethod(GET)), MethodRejection(HttpMethod(DELETE))))
*/

     /*
    [INFO] [19:19:53.323] [leonardo-akka.actor.default-dispatcher-3] o.b.d.w.l.m.ClusterMonitorSupervisor - Monitoring cluster gpalloc-dev-master-cgukad9/automation-test-adognzxtz for initialization.
      [INFO] [19:20:00.965] [leonardo-akka.actor.default-dispatcher-3] o.b.d.w.l.m.ClusterMonitorSupervisor - Monitoring cluster gpalloc-dev-master-8ozyecb/automation-test-ai2xqdpyz for initialization.
      [INFO] [19:20:03.262] [leonardo-akka.actor.default-dispatcher-23] o.b.d.w.l.m.ClusterMonitorActor - Cluster gpalloc-dev-master-cgukad9/automation-test-adognzxtz is not ready yet (Creating). Checking again in 1 minute.
    [INFO] [19:20:07.014] [leonardo-akka.actor.default-dispatcher-18] o.b.d.w.l.m.ClusterMonitorActor - Cluster gpalloc-dev-master-8ozyecb/automation-test-ai2xqdpyz is not ready yet (Creating). Checking again in 1 minute.
    [INFO] [19:21:03.806] [leonardo-akka.actor.default-dispatcher-6] o.b.d.w.l.m.ClusterMonitorActor - Cluster gpalloc-dev-master-cgukad9/automation-test-adognzxtz is not ready yet (Creating). Checking again in 1 minute.
    [WARN] [19:21:07.387] [leonardo-akka.actor.default-dispatcher-4] o.b.d.w.l.m.ClusterMonitorActor - Cluster gpalloc-dev-master-8ozyecb/automation-test-ai2xqdpyz is in an error state with ClusterErrorDetails(3,Some(The resource 'pet-114763077412354570085@gpalloc-dev-master-8ozyecb.iam.gserviceaccount.com' of type 'serviceAccount' was not found.))'. Unable to recreate cluster.
    [INFO] [19:22:03.950] [leonardo-akka.actor.default-dispatcher-6] o.b.d.w.l.m.ClusterMonitorActor - Cluster gpalloc-dev-master-cgukad9/automation-test-adognzxtz is not ready yet (Creating). Checking again in 1 minute.
    [INFO] [19:23:04.411] [leonardo-akka.actor.default-dispatcher-19] o.b.d.w.l.m.ClusterMonitorActor - Cluster gpalloc-dev-master-cgukad9/automation-test-adognzxtz is not ready yet (Creating). Checking again in 1 minute.
    [ERROR] [03/13/2018 19:23:53.613] [leonardo-akka.actor.default-dispatcher-19] [akka.actor.ActorSystemImpl(leonardo)] Rejected(List(MalformedRequestContentRejection(Expected bucket URI, got: {"bucketName":{"value":"gs://leonardo-swat-test-bucket-do-not-delete"},"objectName":{"value":"","timeCreated":{"epochSecond":-31557014167219200,"nano":0}}},spray.json.DeserializationException: Expected bucket URI, got: {"bucketName":{"value":"gs://leonardo-swat-test-bucket-do-not-delete"},"objectName":{"value":"","timeCreated":{"epochSecond":-31557014167219200,"nano":0}}}), TransformationRejection(akka.http.scaladsl.server.directives.BasicDirectives$$Lambda$1576/1488646382@34c51fd9), MethodRejection(HttpMethod(GET)), MethodRejection(HttpMethod(DELETE))))
    [INFO] [19:24:05.510] [leonardo-akka.actor.default-dispatcher-19] o.b.d.w.l.m.ClusterMonitorActor - Cluster gpalloc-dev-master-cgukad9/automation-test-adognzxtz is not ready yet (Creating). Checking again in 1 minute.
    [INFO] [19:25:06.250] [leonardo-akka.actor.default-dispatcher-32] o.b.d.w.l.m.ClusterMonitorActor - Cluster gpalloc-dev-master-cgukad9/automation-test-adognzxtz is not ready yet (Creating). Checking again in 1 minute.
    */


    "should error on cluster create and delete the cluster" in {
      withCleanBillingProject(hermioneCreds) { projectName =>
        Orchestration.billing.addUserToBillingProject(projectName, ronEmail, Orchestration.billing.BillingProjectRole.User)(hermioneAuthToken)
        implicit val token = ronAuthToken
        withNewErroredCluster(GoogleProject(projectName)) { _ =>
          // no-op; just verify that it launches
        }
      }
    }

/*
      [info] - should create a cluster in a different billing project using PetClusterServiceAccountProvider and put the pet's credentials on the cluster *** FAILED ***
      [info]   com.fasterxml.jackson.databind.exc.MismatchedInputException: Cannot construct instance of `org.broadinstitute.dsde.workbench.model.google.GcsBucketName` (although at least one Creator exists): no String-argument constructor/factory method to deserialize from String value ('automation-test-adognzxtz--18d1627b-de05-4e5b-b72b-86791b37561a')
    [info]  at [Source: (String)"{"stagingBucket":"automation-test-adognzxtz--18d1627b-de05-4e5b-b72b-86791b37561a","errors":[],"machineConfig":{"numberOfWorkers":0,"masterMachineType":"n1-standard-4","masterDiskSize":500},"creator":"ron.weasley@test.firecloud.org","googleProject":"gpalloc-dev-master-cgukad9","labels":{"creator":"ron.weasley@test.firecloud.org","googleProject":"gpalloc-dev-master-cgukad9","clusterServiceAccount":"pet-114763077412354570085@gpalloc-dev-master-cgukad9.iam.gserviceaccount.com","clusterName":"automa"[truncated 562 chars]; line: 1, column: 18] (through reference chain: org.broadinstitute.dsde.workbench.leonardo.model.Cluster["stagingBucket"])
*/


    // [info] Object expected in field 'labels'



    // default PetClusterServiceAccountProvider edition
    "should create a cluster in a different billing project using PetClusterServiceAccountProvider and put the pet's credentials on the cluster" in withWebDriver { implicit driver =>
      withCleanBillingProject(hermioneCreds) { projectName =>
        val project = GoogleProject(projectName)

        Orchestration.billing.addUserToBillingProject(projectName, ronEmail, Orchestration.billing.BillingProjectRole.User)(hermioneAuthToken)

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
      withCleanBillingProject(hermioneCreds) { projectName =>
        val project = GoogleProject(projectName)

        Orchestration.billing.addUserToBillingProject(projectName, ronEmail, Orchestration.billing.BillingProjectRole.User)(hermioneAuthToken)

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

    // TODO: we've noticed intermittent failures for this test. See:
    // https://github.com/DataBiosphere/leonardo/issues/204
    // https://github.com/DataBiosphere/leonardo/issues/228
    "should execute Hail with correct permissions on a cluster with preemptible workers" ignore withWebDriver { implicit driver =>
      withCleanBillingProject(hermioneCreds) { projectName =>
        val project = GoogleProject(projectName)

        Orchestration.billing.addUserToBillingProject(projectName, ronEmail, Orchestration.billing.BillingProjectRole.User)(hermioneAuthToken)

        withNewGoogleBucket(project) { bucket =>
          implicit val patienceConfig: PatienceConfig = storagePatience

          val srcPath = parseGcsPath("gs://genomics-public-data/1000-genomes/vcf/ALL.chr20.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf").right.get
          val destPath = GcsPath(bucket, GcsObjectName("chr20.vcf"))
          googleStorageDAO.copyObject(srcPath.bucketName, srcPath.objectName, destPath.bucketName, destPath.objectName).futureValue

          implicit val token = ronAuthToken
          val ronProxyGroup = Sam.user.proxyGroup(ronEmail)
          val ronPetEntity = GcsEntity(ronProxyGroup, Group)
          googleStorageDAO.setObjectAccessControl(destPath.bucketName, destPath.objectName, ronPetEntity, Reader).futureValue

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
