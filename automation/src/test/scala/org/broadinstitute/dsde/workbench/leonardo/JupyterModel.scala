package org.broadinstitute.dsde.workbench.leonardo

case class ContentItem (
                       `type`: String,
                       mimetype: Option[String],
                       writable: Boolean,
                       name: String,
                       format: String,
                       created: String,
                       content: Option[String],
                       last_modified: String,
                       path: String ,
                       size: Int
)

//response from jupyter API for an .ipynb file
case class NotebookContentItem (
                                 `type`: String,
                                 mimetype: Option[String],
                                 writable: Boolean,
                                 name: String,
                                 format: String,
                                 created: String,
                                 content: Any, //this is a complex JSON object that is identical to the notebook contents on disk.
                                 last_modified: String,
                                 path: String,
                                 size: Int
                               )

