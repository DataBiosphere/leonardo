package org.broadinstitute.dsde.workbench.leonardo
package util

import cats.effect.IO
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.azure._
import org.broadinstitute.dsde.workbench.azure.mock.{FakeAzureContainerService, FakeAzureRelayService}
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{NamespaceName, ServiceAccountName}
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.{makeApp, makeKubeCluster, makeNodepool}
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.AppSamResourceId
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.config.SamConfig
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.broadinstitute.dsde.workbench.leonardo.http.ConfigReader
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsp.Release
import org.broadinstitute.dsp.mocks.MockHelm
import org.scalatest.flatspec.AnyFlatSpecLike

import java.nio.file.Files
import java.util.Base64
import scala.concurrent.ExecutionContext.Implicits.global
import scala.jdk.CollectionConverters.ListHasAsScala

class AKSInterpreterSpec extends AnyFlatSpecLike with TestComponent with LeonardoTestSuite {

  val config = AKSInterpreterConfig(ConfigReader.appConfig.terraAppSetupChart,
                                    ConfigReader.appConfig.azure.coaAppConfig,
                                    SamConfig("https://sam")
  )

  val aksInterp = new AKSInterpreter[IO](
    config,
    MockHelm,
    MockAzureContainerService,
    FakeAzureRelayService
  )

  val cloudContext = AzureCloudContext(
    TenantId("tenant"),
    SubscriptionId("sub"),
    ManagedResourceGroupName("mrg")
  )

  val lzResources = LandingZoneResources(AKSClusterName("cluster"),
                                         BatchAccountName("batch"),
                                         RelayNamespace("relay"),
                                         StorageAccountName("storage"),
                                         SubnetName("subnet")
  )

  "AKSInterpreter" should "get a helm auth context" in {
    val res = for {
      authContext <- aksInterp.getHelmAuthContext(lzResources.clusterName, cloudContext, NamespaceName("ns"))
    } yield {
      authContext.namespace.asString shouldBe "ns"
      authContext.kubeApiServer.asString shouldBe "server"
      authContext.kubeToken.asString shouldBe "token"
      Files.exists(authContext.caCertFile.path) shouldBe true
      Files.readAllLines(authContext.caCertFile.path).asScala.mkString shouldBe "cert"
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "build coa override values" in {
    val overrides = aksInterp.buildCromwellChartOverrideValues(Release("rel-1"),
                                                               cloudContext,
                                                               AppSamResourceId("sam"),
                                                               lzResources,
                                                               RelayHybridConnectionName("hc"),
                                                               PrimaryKey("pk")
    )
    overrides.asString shouldBe
      "config.resourceGroup=mrg," +
      "config.batchAccountName=batch," +
      "config.batchNodesSubnetId=subnet," +
      "relaylistener.connectionString=Endpoint=sb://relay.servicebus.windows.net/;SharedAccessKeyName=listener;SharedAccessKey=pk;EntityPath=hc," +
      "relaylistener.connectionName=hc,relaylistener.endpoint=https://relay.servicebus.windows.net," +
      "relaylistener.targetHost=http://coa-rel-1-reverse-proxy-service:8000/," +
      "relaylistener.samUrl=https://sam," +
      "relaylistener.samResourceId=sam," +
      "relaylistener.samResourceType=controlled-application-private-workspace-resource," +
      "persistence.storageResourceGroup=mrg," +
      "persistence.storageAccount=storage," +
      "fullnameOverride=coa-rel-1"
  }

  it should "create and poll a coa app" in isolatedDbTest {
    val res = for {
      cluster <- IO(makeKubeCluster(1).copy(cloudContext = CloudContext.Azure(cloudContext)).save())
      nodepool <- IO(makeNodepool(1, cluster.id).save())
      app = makeApp(1, nodepool.id).copy(
        appResources = AppResources(
          namespace = Namespace(
            NamespaceId(-1),
            NamespaceName("ns-1")
          ),
          disk = None,
          services = List.empty,
          kubernetesServiceAccountName = Some(ServiceAccountName("ksa-1"))
        )
      )
      saveApp <- IO(app.save())
      params = CreateAKSAppParams(saveApp.id, saveApp.appName, cloudContext)
      _ <- aksInterp.createAndPollApp(params)
    } yield {
      // TODO (TOAZ-229): verify app status reaches Running once polling is implemented
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

}

object MockAzureContainerService extends FakeAzureContainerService {
  override def getClusterCredentials(name: AKSClusterName, cloudContext: AzureCloudContext)(implicit
    ev: Ask[IO, TraceId]
  ): IO[AKSCredentials] =
    IO.pure(
      AKSCredentials(AKSServer("server"),
                     AKSToken("token"),
                     AKSCertificate(Base64.getEncoder.encodeToString("cert".getBytes()))
      )
    )
}
