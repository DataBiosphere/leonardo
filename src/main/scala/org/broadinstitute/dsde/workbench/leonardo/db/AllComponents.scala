package org.broadinstitute.dsde.workbench.leonardo.db

// a trait combining all of the individual LeoComponent traits
trait AllComponents extends ClusterComponent
  with LabelComponent
  with ClusterErrorComponent
  with InstanceComponent
  with ExtensionComponent
  with ClusterImageComponent
