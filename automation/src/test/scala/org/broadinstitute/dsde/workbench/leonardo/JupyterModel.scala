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

//TODO: the below objects are a representation of the notebook
//The attempt to serialize all data fail failed, as the /api/contents endpoint which is used to retrieve a notebook from the jupyter-server image on the cluster changes a lot of the contents
//If the attempt is to succeed, we should directly retrieve the file from the disk of the image on the cluster via gcloud utility functions.
//This was not done originally, as it was deemed a size comparison was sufficient for notebook equality

//response from jupyter API for an .ipynb file
case class NotebookContentItem (
                                 `type`: String,
                                 mimetype: Option[String],
                                 writable: Boolean,
                                 name: String,
                                 format: String,
                                 created: String,
                                 content: RawNotebookContents,
                                 last_modified: String,
                                 path: String,
                                 size: Int
                               )

//what you get if you `cat` a notebook. The majority of the 'Any' typed fields are due to the fact that Jupyter uses inconsistent types for fields, for example sometimes strings and sometimes lists
case class RawNotebookContents(cells: List[Cell], metadata: Any, nbformat: Int, nbformat_minor: Int)

case class Cell(cell_type: String, execution_count: Int, var metadata: Map[String,Any], outputs: List[Output], source: Any)

case class Output(name: Option[String], output_type: Option[String], text: Any, data: Option[Any], execution_count: Option[Int], metadata: Option[Any])

