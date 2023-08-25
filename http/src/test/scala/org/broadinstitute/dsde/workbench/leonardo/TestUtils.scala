package org.broadinstitute.dsde.workbench.leonardo

import akka.actor.ActorSystem
import cats.effect.{Async, IO}
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.http.SslContextReader

import javax.net.ssl.SSLContext
import org.broadinstitute.dsde.workbench.leonardo.model.LeoException
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalactic.Equality
import org.scalatest.matchers.should.Matchers

object TestUtils extends Matchers {
  implicit val appContext: Ask[IO, AppContext] = AppContext
    .lift[IO](None, "")
    .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

  // When in scope, Equality instances override Scalatest's default equality ignoring the id field
  // while comparing clusters as we typically don't care about the database assigned id field
  // http://www.scalactic.org/user_guide/CustomEquality
  implicit val clusterEq: Equality[Runtime] =
    new Equality[Runtime] {
      private val FixedId = 0

      def areEqual(a: Runtime, b: Any): Boolean =
        b match {
          case c: Runtime => a.copy(id = FixedId) == c.copy(id = FixedId)
          case _          => false
        }
    }

  implicit val leonardoExceptionEq: Equality[LeoException] =
    (a: LeoException, b: Any) =>
      b match {
        case bb: LeoException =>
          a.message == bb.message && a.statusCode == bb.statusCode && a.cause == bb.cause
        case _ => false
      }

  implicit def eitherEq[A, B](implicit ea: Equality[A], eb: Equality[B]): Equality[Either[A, B]] =
    (a: Either[A, B], b: Any) =>
      (a, b) match {
        case (Left(aa), Left(bb)) =>
          aa === bb
        case (Right(aa), Right(bb)) => aa === bb
        case _                      => false
      }

  // these are not applied recursively, hence the need to dig into the nodepool Ids
  implicit val kubeClusterEq: Equality[KubernetesCluster] =
    new Equality[KubernetesCluster] {
      private val FixedId = KubernetesClusterLeoId(0)
      private val FixedNodepoolId = NodepoolLeoId(0)
      def areEqual(a: KubernetesCluster, b: Any): Boolean =
        b match {
          case c: KubernetesCluster =>
            a.copy(id = FixedId,
                   nodepools = a.nodepools.map(n => n.copy(id = FixedNodepoolId, clusterId = FixedId))
            ) ===
              c.copy(id = FixedId, nodepools = c.nodepools.map(n => n.copy(id = FixedNodepoolId, clusterId = FixedId)))
          case _ => false
        }
    }

  implicit val namespaceEq: Equality[Namespace] =
    new Equality[Namespace] {
      private val FixedId = NamespaceId(0)

      def areEqual(a: Namespace, b: Any): Boolean =
        b match {
          case c: Namespace => a.copy(id = FixedId) == c.copy(id = FixedId)
          case _            => false
        }
    }

  implicit val nodepoolEq: Equality[Nodepool] =
    new Equality[Nodepool] {
      private val FixedId = NodepoolLeoId(0)

      def areEqual(a: Nodepool, b: Any): Boolean =
        b match {
          case c: Nodepool => a.copy(id = FixedId) == c.copy(id = FixedId)
          case _           => false
        }
    }

  implicit val appEq: Equality[App] =
    new Equality[App] {
      private val FixedId = AppId(0)
      private val FixedNamespaceId = NamespaceId(0)
      private val FixedDiskId = DiskId(0)

      def areEqual(a: App, b: Any): Boolean =
        b match {
          case c: App =>
            a.copy(
              id = FixedId,
              appResources = a.appResources.copy(
                namespace = a.appResources.namespace.copy(id = FixedNamespaceId),
                services = fixIdsForServices(a.appResources.services),
                disk = a.appResources.disk.map(d => d.copy(id = FixedDiskId))
              )
            ) ===
              c.copy(
                id = FixedId,
                appResources = c.appResources.copy(
                  namespace = c.appResources.namespace.copy(id = FixedNamespaceId),
                  services = fixIdsForServices(c.appResources.services),
                  disk = c.appResources.disk.map(d => d.copy(id = FixedDiskId))
                )
              )
          case _ => false
        }
    }

  private def fixIdForService(service: KubernetesService): KubernetesService = {
    val FixedServiceId = ServiceId(0)
    service.copy(
      id = FixedServiceId,
      config = service.config
    )
  }

  private def fixIdsForServices(services: List[KubernetesService]): List[KubernetesService] =
    services.map(fixIdForService).sortBy(_.config.name.value)

  implicit val serviceEq: Equality[KubernetesService] =
    (a: KubernetesService, b: Any) =>
      b match {
        case c: KubernetesService => fixIdForService(a) === fixIdForService(c)
        case _                    => false
      }

  implicit val namespaceListEq: Equality[List[Namespace]] =
    (as: List[Namespace], bs: Any) =>
      bs match {
        case cs: List[_] => isNamespaceListEquivalent(as, cs)
        case _           => false
      }

  private def isNamespaceListEquivalent(cs1: Traversable[_], cs2: Traversable[_]): Boolean = {
    val dummyId = NamespaceId(0)

    val fcs1 = cs1 map { case c: Namespace => c.copy(id = dummyId) }
    val fcs2 = cs2 map { case c: Namespace => c.copy(id = dummyId) }

    fcs1 == fcs2
  }

  implicit val clusterSeqEq: Equality[Seq[Runtime]] =
    (as: Seq[Runtime], bs: Any) =>
      bs match {
        case cs: Seq[_] => isEquivalent(as, cs)
        case _          => false
      }

  implicit val clusterSetEq: Equality[Set[Runtime]] =
    (as: Set[Runtime], bs: Any) =>
      bs match {
        case cs: Set[_] => isEquivalent(as, cs)
        case _          => false
      }

  implicit val diskEq: Equality[PersistentDisk] =
    new Equality[PersistentDisk] {
      private val FixedId = DiskId(0)

      def areEqual(a: PersistentDisk, b: Any): Boolean =
        b match {
          case c: PersistentDisk => a.copy(id = FixedId) == c.copy(id = FixedId)
          case _                 => false
        }
    }

  // Equivalence means clusters have the same fields when ignoring the id field
  private def isEquivalent(cs1: Traversable[_], cs2: Traversable[_]): Boolean = {
    val DummyId = 0

    val fcs1 = cs1 map { case c: Runtime => c.copy(id = DummyId) }
    val fcs2 = cs2 map { case c: Runtime => c.copy(id = DummyId) }

    fcs1 == fcs2
  }

  def stripFieldsForListCluster: Runtime => Runtime = { cluster =>
    cluster.copy(errors = List.empty, userJupyterExtensionConfig = None, runtimeImages = Set.empty, scopes = Set.empty)
  }

  def sslContext(implicit as: ActorSystem): SSLContext =
    SslContextReader.getSSLContext[IO]().unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

  /**
   * By default Mockito will return `null` when mock is invoked but the function was not stubbed. In Java, this
   * typically leads to a NPE and stack trace that can easily be followed to find the invocation. In Scala and
   * especially using Cats the stack traces are useless and there is just an error that says "fa is null" but
   * nothing useful about where that fa came from.
   *
   * This default answer will instead return an F that raises and exception detailing which stub is missing.
   * Example:
   * val authProviderMock = mock[LeoAuthProvider[IO]](defaultMockitoAnswer[IO])
   */
  def defaultMockitoAnswer[F[_]](implicit F: Async[F]): Answer[F[_]] =
    (invocation: InvocationOnMock) =>
      F.raiseError(new Exception("invocation call has not been stubbed: " + invocation.toString))
}
