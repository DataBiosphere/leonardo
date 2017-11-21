package org.broadinstitute.dsde.workbench

import java.io.File

// a standardized way to handle file upload/download involving the resources folder
case class ResourceFile(path: String) extends File("src/test/resources/" + path)

object ResourceFile {
  def downloadsFile(path: String) = ResourceFile(s"downloads/$path")
}