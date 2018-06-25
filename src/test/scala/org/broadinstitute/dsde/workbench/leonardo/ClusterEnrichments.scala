package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.leonardo.model.Cluster
import org.scalactic.Equality

object ClusterEnrichments {
  // When in scope, it overrides Scalatest's default equality ignoring the id field
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
          case cs: Seq[_] => as.zip(cs) forall { case (first, second) => clusterEq.areEqual(first, second)}
          case _ => false
        }
      }
    }
  }
}
