package org.broadinstitute.dsde.workbench.leonardo.util

import cats.effect.std.Semaphore
import cats.effect.{IO, Resource}
import com.azure.identity.ClientSecretCredentialBuilder
import org.broadinstitute.dsde.workbench.azure._
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{NamespaceName, ServiceAccountName}
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.{makeApp, makeKubeCluster, makeNodepool}
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.AppSamResourceId
import org.broadinstitute.dsde.workbench.leonardo.TestUtils._
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.broadinstitute.dsde.workbench.leonardo.{
  AppName,
  AppResources,
  AppStatus,
  AppType,
  Chart,
  CloudContext,
  KubernetesClusterStatus,
  LeonardoTestSuite,
  ManagedIdentityName,
  Namespace,
  NamespaceId,
  NodepoolStatus
}
import org.broadinstitute.dsp.{ChartName, ChartVersion, HelmInterpreter, Release}
import org.http4s.Uri
import org.scalatest.Ignore
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Not a true unit test; for now this is a manual test to test AKS deployment.
 */
@Ignore
class AKSInterpreterSpec extends AnyFlatSpecLike with TestComponent with LeonardoTestSuite {
  // These are secret credentials!!
  val appRegConfig = AzureAppRegistrationConfig(
    ClientId("client-id"),
    ClientSecret("client-secret"),
    ManagedAppTenantId("tenant-id")
  )

  // This is attached to a workspace
  val cloudContext = AzureCloudContext(
    TenantId("tenant-id"),
    SubscriptionId("sub-id"),
    ManagedResourceGroupName("mrg-name")
  )

  // This is retrieved from Sam
  val uamiName = ManagedIdentityName("uami-name")

  // This will be created by front Leo
  val appSamResourceId = AppSamResourceId("sam-id")

  "AKSInterpreter" should "create an app" in {
    // Create resources in the DB
    val savedCluster1 = makeKubeCluster(1)
      .copy(
        cloudContext = CloudContext.Azure(cloudContext),
        status = KubernetesClusterStatus.Running
      )
      .save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).copy(status = NodepoolStatus.Running).save()
    val savedApp1 = makeApp(1, savedNodepool1.id)
      .copy(
        appName = AppName("coa-app"),
        status = AppStatus.Running,
        appType = AppType.Cromwell,
        chart = Chart(ChartName("/Users/rtitle/git/broadinstitute/cromwhelm/coa-helm"), ChartVersion("0.2.114")),
        release = Release("rel-1"),
        samResourceId = appSamResourceId,
        appResources = AppResources(
          namespace = Namespace(NamespaceId(-1), NamespaceName("ns-1")),
          disk = None,
          services = List.empty,
          kubernetesServiceAccountName = Some(ServiceAccountName("ksa-1"))
        )
      )
      .save()

    aksInterp
      .use { interp =>
        // Install the app
        interp.createAndPollApp(
          CreateAKSAppParams(
            savedApp1.id,
            savedApp1.appName,
            cloudContext,
            uamiName
          )
        )
      }
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  private def aksInterp: Resource[IO, AKSInterpreter[IO]] =
    for {
      containerService <- AzureContainerService.fromAzureAppRegistrationConfig[IO](appRegConfig)
      relayService <- AzureRelayService.fromAzureAppRegistrationConfig[IO](appRegConfig)
      helmConcurrency <- Resource.eval(Semaphore[IO](20L))
      helmClient = new HelmInterpreter[IO](helmConcurrency)
      config = AKSInterpreterConfig(
        TerraAppSetupChartConfig(ChartName("terra-app-setup-charts/terra-app-setup"), ChartVersion("0.0.3-SNAP")),
        Uri.unsafeFromString("https://sam.dsde-dev.broadinstitute.org/")
      )
      credential = new ClientSecretCredentialBuilder()
        .clientId(appRegConfig.clientId.value)
        .clientSecret(appRegConfig.clientSecret.value)
        .tenantId(appRegConfig.managedAppTenantId.value)
        .build

    } yield new AKSInterpreter(config, credential, helmClient, containerService, relayService)
}
