package org.broadinstitute.dsde.workbench

package object leonardo {
  type LabelMap = Map[String, String]
  //this value is the default for autopause, if none is specified. An autopauseThreshold of 0 indicates no autopause
  final val autoPauseOffValue = 0

  /** Aliases */
  type Cluster = Runtime
  type ClusterName = RuntimeName
  type ClusterInternalId = RuntimeInternalId
  type ClusterImage = RuntimeImage
  type ClusterError = RuntimeError
  type Instance = DataprocInstance
  type InstanceKey = DataprocInstanceKey
}
