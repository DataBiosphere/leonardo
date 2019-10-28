package org.broadinstitute.dsde.workbench.leonardo

import monocle.Lens
import monocle.macros.GenLens
import org.broadinstitute.dsde.workbench.leonardo.model._

object LeoLenses {
  val clusterToClusterImages: Lens[Cluster, Set[ClusterImage]] = GenLens[Cluster](_.clusterImages)
}
