package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import cats.effect.IO
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.google2.mock.{
  FakeComputeOperationFuture,
  FakeGoogleComputeService,
  FakeGooglePublisher,
  FakeGoogleResourceService,
  FakeGoogleStorageInterpreter
}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.{
  allowListAuthProvider,
  autoFreezeConfig,
  azureServiceConfig,
  dataprocConfig,
  leoKubernetesConfig
}
import org.broadinstitute.dsde.workbench.leonardo.MockAuthProvider.serviceAccountProvider
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.config.Config.{gkeCustomAppConfig, imageConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.{MockDockerDAO, MockWsmDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.broadinstitute.dsde.workbench.leonardo.util.QueueFactory
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.scalatest.flatspec.AnyFlatSpec

final class ResourcesServiceInterpSpec extends AnyFlatSpec with LeonardoTestSuite with TestComponent {
  val publisherQueue = QueueFactory.makePublisherQueue()
//  val runtimeService = new RuntimeServiceInterp(
//    RuntimeServiceConfig(
//      Config.proxyConfig.proxyUrlBase,
//      imageConfig,
//      autoFreezeConfig,
//      dataprocConfig,
//      Config.gceConfig,
//      azureServiceConfig
//    ),
//    ConfigReader.appConfig.persistentDisk,
//    allowListAuthProvider,
//    serviceAccountProvider,
//    new MockDockerDAO,
//    FakeGoogleStorageInterpreter,
//    FakeGoogleComputeService,
//    publisherQueue
//  )
//  val noLabelsGoogleResourceService = new FakeGoogleResourceService {
//    override def getLabels(project: GoogleProject)(implicit ev: Ask[IO, TraceId]): IO[Option[Map[String, String]]] =
//      IO(None)
//  }
//  val wsmDao = new MockWsmDAO
//  val appService = new LeoAppServiceInterp[IO](
//    AppServiceConfig(enableCustomAppCheck = true, enableSasApp = true, leoKubernetesConfig),
//    allowListAuthProvider,
//    serviceAccountProvider,
//    QueueFactory.makePublisherQueue(),
//    FakeGoogleComputeService,
//    noLabelsGoogleResourceService,
//    gkeCustomAppConfig,
//    wsmDao
//  )
//  val diskService = new DiskServiceInterp(
//    ConfigReader.appConfig.persistentDisk.copy(dontCloneFromTheseGoogleFolders = dontCloneFromTheseGoogleFolders),
//    allowListAuthProvider,
//    serviceAccountProvider,
//    publisherQueue,
//    MockGoogleDiskService,
//    googleProjectDAO
//  )
//  val resourcesService = new ResourcesServiceInterp(allowListAuthProvider, runtimeService, appService, diskService)

  it should "queue delete apps, runtimes and disks messages and mark them all as deleted when deleteInCloud flag is false and deleteDisk is true" in isolatedDbTest {}
  it should "queue delete apps and runtimes messages, mark them as deleted, but leave all disks when deleteInCloud flag is false and deleteDisk is true" in isolatedDbTest {}
  it should "not queue messages only and mark all resources as deleted when deleteInCloud flag is false" in isolatedDbTest {}
  it should "error out if resources are not in a deletable status" in isolatedDbTest {}
  it should "error out if user does not have read access to the project" in isolatedDbTest {}
  it should "error out if user does not have delete permission on the resources" in isolatedDbTest {}
}
