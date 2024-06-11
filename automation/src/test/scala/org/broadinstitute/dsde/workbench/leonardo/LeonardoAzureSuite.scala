package org.broadinstitute.dsde.workbench.leonardo

import cats.effect.IO
import org.broadinstitute.dsde.rawls.model.AzureManagedAppCoordinates
import org.broadinstitute.dsde.workbench.leonardo.azure.{AzureAutopauseSpec, AzureDiskSpec, AzureRuntimeSpec}
import org.broadinstitute.dsde.workbench.pipeline.Pipeline.BILLING_PROJECT
import org.broadinstitute.dsde.workbench.pipeline.TestUser.Hermione
import org.broadinstitute.dsde.workbench.service.Rawls
import org.scalatest._
import org.scalatest.freespec.FixtureAnyFreeSpecLike

import java.util.UUID

final case class AzureBillingProjectName(value: String) extends AnyVal
trait AzureBilling extends FixtureAnyFreeSpecLike {
  this: TestSuite =>
  import io.circe.{parser, Decoder}
  import org.broadinstitute.dsde.workbench.auth.AuthToken
  import org.broadinstitute.dsde.workbench.azure.{AzureCloudContext, ManagedResourceGroupName, SubscriptionId, TenantId}
  override type FixtureParam = WorkspaceResponse
  val azureProjectKey = "leonardo.azureProject"

  implicit val azureManagedAppCoordinates: AzureManagedAppCoordinates = AzureManagedAppCoordinates(
    UUID.fromString("fad90753-2022-4456-9b0a-c7e5b934e408"),
    UUID.fromString("f557c728-871d-408c-a28b-eb6b2141a087"),
    "e2e-xmx74y",
    Some(UUID.fromString("997743f4-1bee-43af-90be-29ae0a47fdfc"))
  )

  override def withFixture(test: OneArgTest): Outcome = {
    def runTestAndCheckOutcome(workspace: WorkspaceResponse) =
      super.withFixture(test.toNoArgTest(workspace))

    implicit val accessToken = Hermione.authToken().unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    try
      sys.props.get(azureProjectKey) match {
        case None => throw new RuntimeException("leonardo.azureProject system property is not set")
        case Some(projectName) =>
          withRawlsWorkspace(AzureBillingProjectName(projectName)) { workspace =>
            runTestAndCheckOutcome(workspace)
          }
      }
    catch {
      case e: org.broadinstitute.dsde.workbench.service.RestException
          if e.message == "Project cannot be deleted because it contains workspaces." =>
        println(
          s"Exception occurred in test, but it is classed as a non-fatal cleanup error (likely in `withTemporaryAzureBillingProject`): $e"
        )
        Succeeded
      case e: Throwable => throw e
    }
  }

  def withRawlsWorkspace[T](
    projectName: AzureBillingProjectName
  )(testCode: WorkspaceResponse => T)(implicit authToken: AuthToken): T = {
    // hardcode this if you want to use a static workspace
    //        val workspaceName = "ddf3f5fa-a80e-4b2f-ab6e-9bd07817fad1-azure-test-workspace"

    val workspaceName = generateWorkspaceName()

    println(s"withRawlsWorkspace: Calling create rawls workspace with name ${workspaceName}")

    Rawls.workspaces.create(
      projectName.value,
      workspaceName,
      Set.empty,
      Map("disableAutomaticAppCreation" -> "true")
    )

    val response =
      workspaceResponse(projectName.value, workspaceName).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    println(s"withRawlsWorkspace: Rawls workspace get called, response: ${response}")

    testCode(response)
  }

  private def workspaceResponse(projectName: String, workspaceName: String)(implicit
    authToken: AuthToken
  ): IO[WorkspaceResponse] = for {
    responseString <- IO.pure(Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName))
    json <- IO.fromEither(parser.parse(responseString))
    parsedResponse <- IO.fromEither(json.as[WorkspaceResponse])
  } yield parsedResponse

  case class WorkspaceResponse(accessLevel: Option[String],
                               canShare: Option[Boolean],
                               canCompute: Option[Boolean],
                               catalog: Option[Boolean],
                               workspace: WorkspaceDetails,
                               owners: Option[Set[String]],
                               azureContext: Option[AzureCloudContext]
  )

  implicit val azureContextDecoder: Decoder[AzureCloudContext] = Decoder.instance { c =>
    for {
      tenantId <- c.downField("tenantId").as[String]
      subscriptionId <- c.downField("subscriptionId").as[String]
      resourceGroupId <- c.downField("managedResourceGroupId").as[String]
    } yield AzureCloudContext(TenantId(tenantId),
                              SubscriptionId(subscriptionId),
                              ManagedResourceGroupName(resourceGroupId)
    )
  }

  implicit val workspaceDetailsDecoder: Decoder[WorkspaceDetails] = Decoder.forProduct10(
    "namespace",
    "name",
    "workspaceId",
    "bucketName",
    "createdDate",
    "lastModified",
    "createdBy",
    "isLocked",
    "billingAccountErrorMessage",
    "errorMessage"
  )(WorkspaceDetails.apply)

  implicit val workspaceResponseDecoder: Decoder[WorkspaceResponse] = Decoder.forProduct7(
    "accessLevel",
    "canShare",
    "canCompute",
    "catalog",
    "workspace",
    "owners",
    "azureContext"
  )(WorkspaceResponse.apply)

  // Note this isn't the full model available, we only need a few fields
  // The full model lives in rawls here https://github.com/broadinstitute/rawls/blob/develop/model/src/main/scala/org/broadinstitute/dsde/rawls/model/WorkspaceModel.scala#L712
  case class WorkspaceDetails(
    namespace: String,
    name: String,
    workspaceId: String,
    bucketName: String,
    createdDate: String,
    lastModified: String,
    createdBy: String,
    isLocked: Boolean = false,
    billingAccountErrorMessage: Option[String] = None,
    errorMessage: Option[String] = None
  )

  private def generateWorkspaceName(): String =
    s"${UUID.randomUUID().toString()}-azure-test-workspace"
}

final class LeonardoAzureSuite
    extends Suites(
      new AzureRuntimeSpec,
      new AzureDiskSpec,
      new AzureAutopauseSpec
    )
    with TestSuite
    with AzureBilling
    with ParallelTestExecution
    with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    val res = for {
      _ <- IO(println("in beforeAll for AzureBillingBeforeAndAfter"))
      _ <- IO(super.beforeAll())
      _ <- IO(sys.props.put(azureProjectKey, BILLING_PROJECT))
      // hardcode this if you want to use a static billing project
      //  _ <- IO(sys.props.put(azureProjectKey, "tmp-billing-project-beddf71a74"))
    } yield ()
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }
}
