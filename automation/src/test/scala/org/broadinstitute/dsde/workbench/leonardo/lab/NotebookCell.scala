package org.broadinstitute.dsde.workbench.leonardo.lab


trait NotebookCell {


  lazy val cellSelector: String = ".jp-Notebook-cell"

  lazy val selectedCellSelector: String = s"$cellSelector.jp-mod-active.jp-mod-selected"

  lazy val emptyCellSelector: String = s"$cellSelector.jp-mod-noOutputs"

  lazy val cellOutputSelector: String =  s".jp-OutputArea-output[data-mime-type]"

}
