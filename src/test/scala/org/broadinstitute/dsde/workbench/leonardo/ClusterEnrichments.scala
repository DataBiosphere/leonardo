package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, ClusterRequest}
import org.scalactic.Equality
import spray.json.RootJsonWriter
import org.broadinstitute.dsde.workbench.leonardo.model.LeonardoJsonSupport._
import org.broadinstitute.dsde.workbench.leonardo.model.google.GoogleJsonSupport.MachineConfigFormat

object ClusterEnrichments {
  // When in scope, Equality instances override Scalatest's default equality ignoring the id field
  // while comparing clusters as we typically don't care about the database assigned id field
  // http://www.scalactic.org/user_guide/CustomEquality
  implicit val clusterEq = {
    new Equality[Cluster] {
      private val FixedId = 0

      def areEqual(a: Cluster, b: Any): Boolean = {
        b match {
          case c: Cluster => a.copy(id = FixedId) == c.copy(id = FixedId)
          case _ => false
        }
      }
    }
  }

  implicit val clusterSeqEq = {
    new Equality[Seq[Cluster]] {
      def areEqual(as: Seq[Cluster], bs: Any): Boolean = {
        bs match {
          case cs: Seq[_] => isEquivalent(as, cs)
          case _ => false
        }
      }
    }
  }

  implicit val clusterSetEq = {
    new Equality[Set[Cluster]] {
      def areEqual(as: Set[Cluster], bs: Any): Boolean = {
        bs match {
          case cs: Set[_] => isEquivalent(as, cs)
          case _ => false
        }
      }
    }
  }

  // Equivalence means clusters have the same fields when ignoring the id field
  private def isEquivalent(cs1: Traversable[_], cs2: Traversable[_]): Boolean = {
    val DummyId = 0

    val fcs1 = cs1 map {case c: Cluster => c.copy(id = DummyId)}
    val fcs2 = cs2 map {case c: Cluster => c.copy(id = DummyId)}

    fcs1 == fcs2
  }

  def stripFieldsForListCluster: Cluster => Cluster = { cluster =>
    cluster.copy(
      instances = Set.empty,
      clusterImages = Set.empty,
      errors = List.empty,
      scopes = Set.empty,
      userJupyterExtensionConfig = None)
  }

  implicit val clusterRequestWriter: RootJsonWriter[ClusterRequest] = jsonFormat13(ClusterRequest)
}
