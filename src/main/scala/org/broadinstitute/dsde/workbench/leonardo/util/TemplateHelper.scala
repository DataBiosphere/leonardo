package org.broadinstitute.dsde.workbench.leonardo.util

import java.io.File

import org.broadinstitute.dsde.workbench.leonardo.config.ClusterResourcesConfig
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterResource

import scala.io.Source

trait TemplateHelper {

  /**
    * Process a string using map of replacement values. Each value in the replacement map replaces its key in the string.
    * Note the replaced string is quoted, so $(foo) will be replaced with "a quoted string".
    */
  def template(raw: String, replacementMap: Map[String, String]): String = {
    replacementMap.foldLeft(raw)((a, b) => a.replaceAllLiterally("$(" + b._1 + ")", "\"" + b._2 + "\""))
  }

  def templateFile(file: File, replacementMap: Map[String, String]): String = {
    val raw = Source.fromFile(file).mkString
    template(raw, replacementMap)
  }

  def templateResource(resource: ClusterResource, replacementMap: Map[String, String]): String = {
    val raw = Source.fromResource(s"${ClusterResourcesConfig.basePath}/${resource.value}").mkString
    template(raw, replacementMap)
  }

}
