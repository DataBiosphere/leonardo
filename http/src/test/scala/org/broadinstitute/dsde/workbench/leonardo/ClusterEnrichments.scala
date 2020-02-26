package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.leonardo.http.service.CreateRuntimeAPIResponse
import org.scalactic.Equality
import org.scalatest.{Assertion, Matchers}

object ClusterEnrichments extends Matchers {
  // When in scope, Equality instances override Scalatest's default equality ignoring the id field
  // while comparing clusters as we typically don't care about the database assigned id field
  // http://www.scalactic.org/user_guide/CustomEquality
  implicit val clusterEq = {
    new Equality[Cluster] {
      private val FixedId = 0

      def areEqual(a: Cluster, b: Any): Boolean =
        b match {
          case c: Cluster => a.copy(id = FixedId) == c.copy(id = FixedId)
          case _          => false
        }
    }
  }

  implicit val clusterSeqEq = {
    new Equality[Seq[Cluster]] {
      def areEqual(as: Seq[Cluster], bs: Any): Boolean =
        bs match {
          case cs: Seq[_] => isEquivalent(as, cs)
          case _          => false
        }
    }
  }

  implicit val clusterSetEq = {
    new Equality[Set[Cluster]] {
      def areEqual(as: Set[Cluster], bs: Any): Boolean =
        bs match {
          case cs: Set[_] => isEquivalent(as, cs)
          case _          => false
        }
    }
  }

  // Equivalence means clusters have the same fields when ignoring the id field
  private def isEquivalent(cs1: Traversable[_], cs2: Traversable[_]): Boolean = {
    val DummyId = 0

    val fcs1 = cs1 map { case c: Cluster => c.copy(id = DummyId) }
    val fcs2 = cs2 map { case c: Cluster => c.copy(id = DummyId) }

    fcs1 == fcs2
  }

  def stripFieldsForListCluster: Cluster => Cluster = { cluster =>
    cluster.copy(dataprocInstances = Set.empty,
                 runtimeImages = Set.empty,
                 errors = List.empty,
                 scopes = Set.empty,
                 userJupyterExtensionConfig = None)
  }

  def compareClusterAndCreateClusterAPIResponse(c: Cluster, createCluster: CreateRuntimeAPIResponse): Assertion = {
    c.id shouldBe createCluster.id
    c.internalId shouldBe createCluster.internalId
    c.runtimeName shouldBe createCluster.clusterName
    c.googleProject shouldBe createCluster.googleProject
    c.serviceAccountInfo shouldBe createCluster.serviceAccountInfo
    c.asyncRuntimeFields shouldBe createCluster.asyncRuntimeFields
    c.auditInfo shouldBe createCluster.auditInfo
    c.dataprocProperties shouldBe createCluster.dataprocProperties
    c.proxyUrl shouldBe createCluster.clusterUrl
    c.status shouldBe createCluster.status
    c.labels shouldBe createCluster.labels
    c.jupyterExtensionUri shouldBe createCluster.jupyterExtensionUri
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
    c.customClusterEnvironmentVariables shouldBe createCluster.customClusterEnvironmentVariables
  }
}
