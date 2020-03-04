package org.broadinstitute.dsde.workbench.leonardo
package dao

import java.util.UUID

import cats.effect.IO
import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.leonardo.dao.MockSamDAO._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.util.health.StatusCheckResponse
import org.http4s.headers.Authorization
import org.http4s.{AuthScheme, Credentials, EntityDecoder}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

class MockSamDAO extends SamDAO[IO] {
  val billingProjects: mutable.Map[(GoogleProject, Authorization), Set[String]] = new TrieMap()
  val notebookClusters: mutable.Map[(ClusterInternalId, Authorization), Set[String]] = new TrieMap()
  val projectOwners: mutable.Map[Authorization, Set[SamProjectPolicy]] = new TrieMap()
  val clusterCreators: mutable.Map[Authorization, Set[SamNotebookClusterPolicy]] = new TrieMap()
  val petSA = WorkbenchEmail("pet-1234567890@test-project.iam.gserviceaccount.com")
  implicit val traceId = ApplicativeAsk.const[IO, TraceId](TraceId(UUID.randomUUID())) //we don't care much about traceId in unit tests, hence providing a constant UUID here

  override def hasResourcePermission(resourceId: String,
                                     action: String,
                                     resourceTypeName: ResourceTypeName,
                                     authHeader: Authorization)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Boolean] =
    resourceTypeName match {
      case ResourceTypeName.BillingProject =>
        val res = billingProjects
          .get((GoogleProject(resourceId), authHeader)) //look it up: Option[Set]
          .map(_.contains(action)) //open the option to peek the set: Option[Bool]
          .getOrElse(false) //unpack the resulting option and handle the project never having existed
        IO.pure(res)
      case ResourceTypeName.NotebookCluster =>
        val res = notebookClusters
          .get((RuntimeInternalId(resourceId), authHeader)) //look it up: Option[Set]
          .map(_.contains(action)) //open the option to peek the set: Option[Bool]
          .getOrElse(false) //unpack the resulting option and handle the project never having existed
        IO.pure(res)
    }

  override def getResourcePolicies[A](
    authHeader: Authorization,
    resourseTypeName: ResourceTypeName
  )(implicit decoder: EntityDecoder[IO, List[A]], ev: ApplicativeAsk[IO, TraceId]): IO[List[A]] =
    resourseTypeName match {
      case ResourceTypeName.NotebookCluster =>
        IO.pure(clusterCreators.get(authHeader).map(_.toList).getOrElse(List.empty)).map(_.asInstanceOf[List[A]])
      case ResourceTypeName.BillingProject =>
        IO.pure(projectOwners.get(authHeader).map(_.toList).getOrElse(List.empty)).map(_.asInstanceOf[List[A]])
    }

  override def createClusterResource(internalId: ClusterInternalId,
                                     creatorEmail: WorkbenchEmail,
                                     googleProject: GoogleProject,
                                     clusterName: ClusterName)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    IO(
      notebookClusters += (internalId, userEmailToAuthorization(creatorEmail)) -> Set("status",
                                                                                      "connect",
                                                                                      "sync",
                                                                                      "delete",
                                                                                      "read_policies")
    )

  override def deleteClusterResource(internalId: ClusterInternalId,
                                     userEmail: WorkbenchEmail,
                                     creatorEmail: WorkbenchEmail,
                                     googleProject: GoogleProject,
                                     clusterName: ClusterName)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    IO(notebookClusters.remove((internalId, userEmailToAuthorization(userEmail))))

  override def getPetServiceAccount(authorization: Authorization, googleProject: GoogleProject)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Option[WorkbenchEmail]] =
    IO.pure(Some(petSA))

  override def getUserProxy(
    userEmail: WorkbenchEmail
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Option[WorkbenchEmail]] =
    IO.pure(Some(WorkbenchEmail("PROXY_1234567890@dev.test.firecloud.org")))

  override def getCachedPetAccessToken(userEmail: WorkbenchEmail, googleProject: GoogleProject)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Option[String]] = IO.pure(Some("token"))

  override def getStatus(implicit ev: ApplicativeAsk[IO, TraceId]): IO[StatusCheckResponse] =
    IO.pure(StatusCheckResponse(true, Map.empty))
}

object MockSamDAO {
  def userEmailToAuthorization(workbenchEmail: WorkbenchEmail): Authorization =
    Authorization(Credentials.Token(AuthScheme.Bearer, s"TokenFor${workbenchEmail}"))
}
