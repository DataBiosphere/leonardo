// AN-570
//package org.broadinstitute.dsde.workbench.leonardo
//package http
//package service
//
//import cats.effect.IO
//import cats.mtl.Ask
//import com.azure.resourcemanager.compute.models.VirtualMachineSizeTypes
//import org.broadinstitute.dsde.workbench.google2.MachineTypeName
//import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
//import org.broadinstitute.dsde.workbench.model.UserInfo
//
//object MockRuntimeV2Interp extends RuntimeV2Service[IO] {
//
//  override def listRuntimes(
//    userInfo: UserInfo,
//    workspaceId: Option[WorkspaceId],
//    cloudProvider: Option[CloudProvider],
//    params: Map[String, String]
//  )(implicit as: Ask[IO, AppContext]): IO[Vector[ListRuntimeResponse2]] =
//    IO.pure(
//      Vector(
//        ListRuntimeResponse2(
//          CommonTestData.testCluster.id,
//          Some(CommonTestData.workspaceId),
//          CommonTestData.testCluster.samResource,
//          RuntimeName("azureruntime1"),
//          CloudContext.Azure(azureCloudContext),
//          CommonTestData.testCluster.auditInfo,
//          RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A0.toString),
//                                    Some(DiskId(-1)),
//                                    None
//          ),
//          CommonTestData.testCluster.proxyUrl,
//          CommonTestData.testCluster.status,
//          CommonTestData.testCluster.labels,
//          CommonTestData.testCluster.patchInProgress
//        )
//      )
//    )
//
//  override def startRuntime(userInfo: UserInfo, runtimeName: RuntimeName, workspaceId: WorkspaceId)(implicit
//    as: Ask[IO, AppContext]
//  ): IO[Unit] = IO.unit
//
//  override def stopRuntime(userInfo: UserInfo, runtimeName: RuntimeName, workspaceId: WorkspaceId)(implicit
//    as: Ask[IO, AppContext]
//  ): IO[Unit] = IO.unit
//}
