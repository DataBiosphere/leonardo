package org.broadinstitute.dsde.workbench.leonardo
package config

final case class RefererConfig(validHosts: Set[String], enabled: Boolean, originStrict: Boolean = false)
