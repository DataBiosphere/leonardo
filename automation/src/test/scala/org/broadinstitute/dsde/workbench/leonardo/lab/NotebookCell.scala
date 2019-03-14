package org.broadinstitute.dsde.workbench.leonardo.lab

trait NotebookCell {

  lazy val cellSelector: String = ".jp-Notebook-cell"

  lazy val cellOutputSelector: String =  ".jp-OutputArea-output[data-mime-type]"

  // selects the numbered left-side cell prompts
  lazy val prompts: String = ".jp-InputPrompt.jp-InputArea-prompt"

}
