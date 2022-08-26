package org.broadinstitute.dsde.workbench.leonardo
package config

final case class CustomAppSecurityConfig(enableCustomAppCheck: Boolean, customApplicationAllowList: CustomApplicationAllowListConfig)

final case class CustomApplicationAllowListConfig(default: List[String], highSecurity:List[String])