package org.broadinstitute.dsde.workbench.leonardo
package config

import org.broadinstitute.dsde.workbench.leonardo.config.ContentSecurityPolicyComponent.{
  ConnectSrc,
  FrameAncestors,
  ObjectSrc,
  ReportUri,
  ScriptSrc,
  StyleSrc
}

// See https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Security-Policy
// for description of the Content-Security-Policy model.
final case class ContentSecurityPolicyConfig(frameAncestors: FrameAncestors,
                                             scriptSrc: ScriptSrc,
                                             styleSrc: StyleSrc,
                                             connectSrc: ConnectSrc,
                                             objectSrc: ObjectSrc,
                                             reportUri: ReportUri
) {
  def asString: String = {
    val components = List(frameAncestors, scriptSrc, styleSrc, connectSrc, objectSrc, reportUri)
    components.map(c => s"${c.name} ${c.values.mkString(" ")}").mkString("; ")
  }
}

sealed trait ContentSecurityPolicyComponent {
  def name: String
  def values: List[String]
}
object ContentSecurityPolicyComponent {
  final case class FrameAncestors(values: List[String]) extends ContentSecurityPolicyComponent {
    override def name: String = "frame-ancestors"
  }
  final case class ScriptSrc(values: List[String]) extends ContentSecurityPolicyComponent {
    override def name: String = "script-src"
  }
  final case class StyleSrc(values: List[String]) extends ContentSecurityPolicyComponent {
    override def name: String = "style-src"
  }
  final case class ConnectSrc(values: List[String]) extends ContentSecurityPolicyComponent {
    override def name: String = "connect-src"
  }
  final case class ObjectSrc(values: List[String]) extends ContentSecurityPolicyComponent {
    override def name: String = "object-src"
  }
  final case class ReportUri(values: List[String]) extends ContentSecurityPolicyComponent {
    override def name: String = "report-uri"
  }
}
