package org.broadinstitute.dsde.workbench.leonardo

case class ContentItem (
                       `type`: String,
                       mimetype: String,
                       writable: Boolean,
                       name: String,
                       format: String,
                       created: String,
                       content: Option[String],
                       last_modified: String,
                       path: String
)
