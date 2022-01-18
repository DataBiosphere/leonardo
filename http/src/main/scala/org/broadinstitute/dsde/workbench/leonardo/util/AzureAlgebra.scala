package org.broadinstitute.dsde.workbench.leonardo.util

import org.broadinstitute.dsde.workbench.leonardo.AppContext
import cats.mtl.Ask

trait AzureAlgebra[F[_]] {

  /** Creates an Azure VM but doesn't wait for its completion.
   * This includes creation of all child Azure resources (disk, network, ip), and assumes these are created syncronously
   * */
  def createRuntime(params: CreateAzureRuntimeParams)(implicit ev: Ask[F, AppContext]): F[CreateAzureRuntimeResult]

  /**
   * Polls a creating an Azure VM for its completion
   * Also [TBD]
   */
  def pollRuntime(params: PollRuntimeParams)(implicit ev: Ask[F, AppContext]): F[Unit]

}


final case class CreateAzureRuntimeParams(runtimeId: Long)

final case class CreateAzureRuntimeResult()
final case class PollRuntimeParams()
