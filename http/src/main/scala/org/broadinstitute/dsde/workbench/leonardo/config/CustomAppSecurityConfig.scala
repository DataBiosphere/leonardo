package org.broadinstitute.dsde.workbench.leonardo
package config

final case class CustomAppSecurityConfig(enableCustomAppCheck: Boolean, customApplicationAllowList: Map[String, List[String]])
