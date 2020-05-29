package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.leonardo.http.service.CreateRuntimeResponse
import org.scalactic.Equality
import org.scalatest.{Assertion, Matchers}

object TestUtils extends Matchers {
  // When in scope, Equality instances override Scalatest's default equality ignoring the id field
  // while comparing clusters as we typically don't care about the database assigned id field
  // http://www.scalactic.org/user_guide/CustomEquality
  implicit val clusterEq = {
    new Equality[Runtime] {
      private val FixedId = 0

      def areEqual(a: Runtime, b: Any): Boolean =
        b match {
          case c: Runtime => a.copy(id = FixedId) == c.copy(id = FixedId)
          case _          => false
        }
    }
  }

  //these are not applied recursively, hence the need to dig into the nodepool Ids
  implicit val kubeClusterEq = {
    new Equality[KubernetesCluster] {
      private val FixedId = KubernetesClusterLeoId(0)
      private val FixedNodepoolId = NodepoolLeoId(0)
      def areEqual(a: KubernetesCluster, b: Any): Boolean =
        b match {
          case c: KubernetesCluster =>
            a.copy(id = FixedId, nodepools = a.nodepools.map(n => n.copy(id = FixedNodepoolId, clusterId = FixedId))) ===
              c.copy(id = FixedId, nodepools = c.nodepools.map(n => n.copy(id = FixedNodepoolId, clusterId = FixedId)))
          case _ => false
        }
    }
  }

  implicit val nodepoolEq = {
    new Equality[Nodepool] {
      private val FixedId = NodepoolLeoId(0)

      def areEqual(a: Nodepool, b: Any): Boolean =
        b match {
          case c: Nodepool => a.copy(id = FixedId) == c.copy(id = FixedId)
          case _           => false
        }
    }
  }

  implicit val clusterSeqEq = {
    new Equality[Seq[Runtime]] {
      def areEqual(as: Seq[Runtime], bs: Any): Boolean =
        bs match {
          case cs: Seq[_] => isEquivalent(as, cs)
          case _          => false
        }
    }
  }

  implicit val clusterSetEq = {
    new Equality[Set[Runtime]] {
      def areEqual(as: Set[Runtime], bs: Any): Boolean =
        bs match {
          case cs: Set[_] => isEquivalent(as, cs)
          case _          => false
        }
    }
  }

  implicit val diskEq = {
    new Equality[PersistentDisk] {
      private val FixedId = DiskId(0)

      def areEqual(a: PersistentDisk, b: Any): Boolean =
        b match {
          case c: PersistentDisk => a.copy(id = FixedId) == c.copy(id = FixedId)
          case _                 => false
        }
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
    cluster.copy(dataprocInstances = Set.empty,
                 runtimeImages = Set.empty,
                 errors = List.empty,
                 scopes = Set.empty,
                 userJupyterExtensionConfig = None)
  }

  def compareClusterAndCreateClusterAPIResponse(c: Runtime, createCluster: CreateRuntimeResponse): Assertion = {
    c.id shouldBe createCluster.id
    c.samResource shouldBe createCluster.samResource
    c.runtimeName shouldBe createCluster.clusterName
    c.googleProject shouldBe createCluster.googleProject
    c.serviceAccount shouldBe createCluster.serviceAccountInfo
    c.asyncRuntimeFields shouldBe createCluster.asyncRuntimeFields
    c.auditInfo shouldBe createCluster.auditInfo
    c.proxyUrl shouldBe createCluster.clusterUrl
    c.status shouldBe createCluster.status
    c.labels shouldBe createCluster.labels
    c.jupyterUserScriptUri shouldBe createCluster.jupyterUserScriptUri
    c.jupyterStartUserScriptUri shouldBe createCluster.jupyterStartUserScriptUri
    c.errors shouldBe createCluster.errors
    c.dataprocInstances shouldBe createCluster.dataprocInstances
    c.userJupyterExtensionConfig shouldBe createCluster.userJupyterExtensionConfig
    c.autopauseThreshold shouldBe createCluster.autopauseThreshold
    c.defaultClientId shouldBe createCluster.defaultClientId
    c.stopAfterCreation shouldBe createCluster.stopAfterCreation
    c.runtimeImages shouldBe createCluster.clusterImages
    c.scopes shouldBe createCluster.scopes
    c.welderEnabled shouldBe createCluster.welderEnabled
    c.customEnvironmentVariables shouldBe createCluster.customClusterEnvironmentVariables
  }
}
